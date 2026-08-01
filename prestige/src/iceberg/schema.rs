use crate::{error::Result, traits::ArrowSchema};
use arrow::datatypes::SchemaRef;
use iceberg::{
    arrow::{arrow_schema_to_schema_auto_assign_ids, arrow_type_to_type},
    spec::{
        NestedField, NestedFieldRef, NullOrder, Schema, SortDirection, SortField, SortOrder,
        Transform, UnboundPartitionField, UnboundPartitionSpec,
    },
};
use std::collections::HashMap;

/// Extension trait that bridges prestige's ArrowSchema to iceberg schema concepts.
///
/// Types implementing this trait can define their iceberg schema, partition spec,
/// and sort order, enabling automatic table creation and schema validation.
pub trait IcebergSchema: ArrowSchema {
    fn iceberg_schema() -> Schema;

    fn table_partition_spec() -> Option<UnboundPartitionSpec> {
        None
    }

    fn table_sort_order() -> Option<iceberg::spec::SortOrder> {
        None
    }

    fn default_table_name() -> Option<&'static str> {
        None
    }

    fn default_namespace() -> Option<&'static [&'static str]> {
        None
    }

    fn identifier_field_names() -> &'static [&'static str] {
        &[]
    }
}

/// Convert an Arrow schema to an Iceberg schema.
///
/// Maps Arrow data types to Iceberg primitive/nested types. Field IDs are
/// assigned sequentially starting from 1 since Arrow schemas from prestige
/// don't carry iceberg field ID metadata.
pub fn arrow_to_iceberg_schema(arrow_schema: &SchemaRef) -> Result<Schema> {
    let schema = arrow_schema_to_schema_auto_assign_ids(arrow_schema)?;
    Ok(schema)
}

/// Convert an Arrow schema to an Iceberg schema with identifier fields.
///
/// Like `arrow_to_iceberg_schema`, but additionally marks the named fields as
/// identifier fields in the resulting iceberg schema. Identifier fields declare
/// the logical primary key of the table, enabling equality deletes and
/// semantic deduplication in downstream consumers.
///
/// Field names that don't appear in the schema are silently ignored.
pub fn arrow_to_iceberg_schema_with_identifiers(
    arrow_schema: &SchemaRef,
    identifier_field_names: &[&str],
) -> Result<Schema> {
    let base = arrow_schema_to_schema_auto_assign_ids(arrow_schema)?;

    if identifier_field_names.is_empty() {
        return Ok(base);
    }

    let identifier_field_ids: Vec<i32> = identifier_field_names
        .iter()
        .filter_map(|name| base.field_by_name(name).map(|f| f.id))
        .collect();

    let schema = base
        .into_builder()
        .with_identifier_field_ids(identifier_field_ids)
        .build()?;

    Ok(schema)
}

/// Resolve identifier field names to iceberg field IDs within a schema.
///
/// Returns field IDs for all names that match. Names not found are skipped.
pub fn resolve_identifier_field_ids(schema: &Schema, names: &[&str]) -> Vec<i32> {
    names
        .iter()
        .filter_map(|name| schema.field_by_name(name).map(|f| f.id))
        .collect()
}

/// Compile-time metadata for a sort key field, emitted by the `#[prestige(sort_key)]` macro.
///
/// At table creation time, `build_sort_order` resolves field names to catalog-assigned
/// field IDs and produces an iceberg `SortOrder`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortFieldDef {
    pub name: &'static str,
    pub direction: SortDirection,
    pub null_order: NullOrder,
    pub order: u32,
}

/// Compile-time metadata for a partition field, emitted by `#[prestige(partition)]`.
///
/// At table creation time, `build_partition_spec` resolves field names to source IDs
/// and produces an `UnboundPartitionSpec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionFieldDef {
    pub name: &'static str,
    pub transform: Transform,
}

/// Build an iceberg `SortOrder` from compile-time sort field definitions and a catalog schema.
///
/// Resolves field names to iceberg field IDs, sorts by the `order` priority
/// (lower values first, then by definition order for ties), and validates
/// against the schema via `SortOrderBuilder::build()`.
pub fn build_sort_order(schema: &Schema, defs: &[SortFieldDef]) -> Result<SortOrder> {
    if defs.is_empty() {
        return Ok(SortOrder::unsorted_order());
    }

    let mut sorted_defs: Vec<&SortFieldDef> = defs.iter().collect();
    sorted_defs.sort_by_key(|d| d.order);

    let mut builder = SortOrder::builder();
    builder.with_order_id(1);
    for def in sorted_defs {
        let field_id = schema
            .field_by_name(def.name)
            .ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("sort key field '{}' not found in schema", def.name),
                )
            })?
            .id;

        builder.with_sort_field(
            SortField::builder()
                .source_id(field_id)
                .direction(def.direction)
                .null_order(def.null_order)
                .transform(Transform::Identity)
                .build(),
        );
    }

    let sort_order = builder.build(schema)?;
    Ok(sort_order)
}

/// Build an `UnboundPartitionSpec` from compile-time partition field definitions
/// and a catalog schema.
///
/// Resolves field names to iceberg source IDs. The resulting spec is unbound
/// (no partition field IDs assigned) — the catalog assigns those during table creation.
pub fn build_partition_spec(
    schema: &Schema,
    defs: &[PartitionFieldDef],
) -> Result<UnboundPartitionSpec> {
    // Field IDs start at 1000 per the Iceberg spec (first field after
    // UNPARTITIONED_LAST_ASSIGNED_ID); assigned here to stay deterministic.
    let mut fields = Vec::with_capacity(defs.len());
    for (next_field_id, def) in (1000_i32..).zip(defs) {
        let source_id = schema
            .field_by_name(def.name)
            .ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("partition field '{}' not found in schema", def.name),
                )
            })?
            .id;

        let partition_name = match def.transform {
            Transform::Identity => def.name.to_string(),
            Transform::Bucket(n) => format!("{}_bucket_{}", def.name, n),
            Transform::Truncate(w) => format!("{}_truncate_{}", def.name, w),
            ref t => format!("{}_{}", def.name, t),
        };

        fields.push(
            UnboundPartitionField::builder()
                .source_id(source_id)
                .field_id(next_field_id)
                .name(partition_name)
                .transform(def.transform)
                .build(),
        );
    }

    let spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(fields)?
        .build();
    Ok(spec)
}

/// Result of reconciling a struct-derived Arrow schema against a catalog's iceberg schema.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaReconciliation {
    /// Schemas match — no changes needed.
    UpToDate,
    /// A new schema was produced with columns added and/or dropped.
    Evolved {
        schema: Box<Schema>,
        columns_added: Vec<String>,
        columns_dropped: Vec<String>,
    },
}

/// Compare an Arrow schema (derived from a Rust struct) against the catalog's current
/// iceberg schema and produce a reconciled schema if they differ.
///
/// Matching is by column name. For each field:
/// - Present in both → retained with the catalog's field ID (type changes are not handled)
/// - Present in Arrow only → added as a new optional column with a fresh field ID
/// - Present in catalog only → dropped (omitted from the new schema)
///
/// `identifier_field_names` declares which columns form the logical primary key.
/// These are resolved to field IDs in the reconciled schema and set via
/// `with_identifier_field_ids`. Pass an empty slice if there are no identifiers.
///
/// New columns are always added as optional because existing data files lack the column
/// and iceberg requires nullable defaults for schema evolution compatibility.
pub fn reconcile_schema(
    arrow_schema: &SchemaRef,
    catalog_schema: &Schema,
    identifier_field_names: &[&str],
) -> Result<SchemaReconciliation> {
    let catalog_fields: HashMap<&str, &NestedFieldRef> = catalog_schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    let arrow_field_names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let mut reconciled_fields: Vec<NestedFieldRef> = Vec::new();
    let mut columns_added: Vec<String> = Vec::new();
    let mut columns_dropped: Vec<String> = Vec::new();

    let mut next_field_id = catalog_schema.highest_field_id() + 1;

    // Walk the Arrow schema to build the reconciled field list, preserving Arrow field order.
    for arrow_field in arrow_schema.fields() {
        let name = arrow_field.name().as_str();

        if let Some(existing) = catalog_fields.get(name) {
            // Field exists in catalog — keep with the catalog's field ID and type.
            reconciled_fields.push((*existing).clone());
        } else {
            // New field — assign a fresh field ID and force optional.
            let iceberg_type = arrow_type_to_type(arrow_field.data_type())?;
            let field = NestedField::optional(next_field_id, name, iceberg_type);
            reconciled_fields.push(field.into());
            columns_added.push(name.to_string());
            next_field_id += 1;
        }
    }

    // Detect dropped columns: in catalog but not in Arrow.
    for catalog_field in catalog_schema.as_struct().fields() {
        if !arrow_field_names.contains(&catalog_field.name.as_str()) {
            columns_dropped.push(catalog_field.name.clone());
        }
    }

    // Also check whether identifier field IDs changed (catalog may have different set).
    let new_identifier_ids: Vec<i32> = identifier_field_names
        .iter()
        .filter_map(|name| {
            reconciled_fields
                .iter()
                .find(|f| f.name == *name)
                .map(|f| f.id)
        })
        .collect();

    let mut current_identifier_ids: Vec<i32> = catalog_schema.identifier_field_ids().collect();
    current_identifier_ids.sort();
    let mut sorted_new_ids = new_identifier_ids.clone();
    sorted_new_ids.sort();
    let identifiers_changed = sorted_new_ids != current_identifier_ids;

    if columns_added.is_empty() && columns_dropped.is_empty() && !identifiers_changed {
        return Ok(SchemaReconciliation::UpToDate);
    }

    let mut builder = Schema::builder()
        .with_schema_id(catalog_schema.schema_id())
        .with_fields(reconciled_fields);

    if !new_identifier_ids.is_empty() {
        builder = builder.with_identifier_field_ids(new_identifier_ids);
    }

    let schema = builder.build()?;

    Ok(SchemaReconciliation::Evolved {
        schema: Box::new(schema),
        columns_added,
        columns_dropped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaType};
    use chrono::{DateTime, Utc};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;

    use crate::ArrowSerialize;

    #[test]
    fn test_arrow_to_iceberg_schema_round_trip() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();

        assert_eq!(iceberg_schema.as_struct().fields().len(), 3);

        let id_field = iceberg_schema.field_by_name("id").unwrap();
        assert!(id_field.required);

        let name_field = iceberg_schema.field_by_name("name").unwrap();
        assert!(!name_field.required);

        let value_field = iceberg_schema.field_by_name("value").unwrap();
        assert!(value_field.required);
    }

    #[test]
    fn test_datetime_utc_maps_to_timestamptz() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![Field::new(
            "created_at",
            DateTime::<Utc>::arrow_data_type(),
            false,
        )]));

        let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema).unwrap();
        let field = iceberg_schema.field_by_name("created_at").unwrap();

        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Timestamptz)
        );
    }

    fn catalog_schema_with_fields(fields: Vec<NestedFieldRef>) -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(fields)
            .build()
            .unwrap()
    }

    #[test]
    fn test_reconcile_schemas_match() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &[]).unwrap();
        assert_eq!(result, SchemaReconciliation::UpToDate);
    }

    #[test]
    fn test_reconcile_add_columns() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &[]).unwrap();

        match result {
            SchemaReconciliation::Evolved {
                schema,
                columns_added,
                columns_dropped,
            } => {
                assert_eq!(columns_added, vec!["email", "age"]);
                assert!(columns_dropped.is_empty());
                assert_eq!(schema.as_struct().fields().len(), 4);

                // Existing fields keep their IDs.
                assert_eq!(schema.field_by_name("id").unwrap().id, 1);
                assert_eq!(schema.field_by_name("name").unwrap().id, 2);

                // New fields get IDs starting after highest existing ID (2).
                let email = schema.field_by_name("email").unwrap();
                assert_eq!(email.id, 3);
                assert!(!email.required, "new columns must be optional");

                let age = schema.field_by_name("age").unwrap();
                assert_eq!(age.id, 4);
                assert!(!age.required, "new columns must be optional");
            }
            other => panic!("expected Evolved, got {other:?}"),
        }
    }

    #[test]
    fn test_reconcile_drop_columns() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "email", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &[]).unwrap();

        match result {
            SchemaReconciliation::Evolved {
                schema,
                columns_added,
                columns_dropped,
            } => {
                assert!(columns_added.is_empty());
                assert_eq!(columns_dropped, vec!["name", "email"]);
                assert_eq!(schema.as_struct().fields().len(), 1);
                assert_eq!(schema.field_by_name("id").unwrap().id, 1);
            }
            other => panic!("expected Evolved, got {other:?}"),
        }
    }

    #[test]
    fn test_reconcile_add_and_drop() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
        ]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &[]).unwrap();

        match result {
            SchemaReconciliation::Evolved {
                schema,
                columns_added,
                columns_dropped,
            } => {
                assert_eq!(columns_added, vec!["score"]);
                assert_eq!(columns_dropped, vec!["name"]);
                assert_eq!(schema.as_struct().fields().len(), 2);

                assert_eq!(schema.field_by_name("id").unwrap().id, 1);
                // score gets ID 3 (highest existing was 2).
                assert_eq!(schema.field_by_name("score").unwrap().id, 3);
            }
            other => panic!("expected Evolved, got {other:?}"),
        }
    }

    #[test]
    fn test_reconcile_preserves_arrow_field_order() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
            Field::new("new_col", DataType::Boolean, true),
        ]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &[]).unwrap();

        match result {
            SchemaReconciliation::Evolved { schema, .. } => {
                let field_names: Vec<&str> = schema
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect();
                assert_eq!(field_names, vec!["name", "id", "new_col"]);
            }
            other => panic!("expected Evolved, got {other:?}"),
        }
    }

    #[test]
    fn test_arrow_to_iceberg_schema_with_identifiers() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let schema =
            arrow_to_iceberg_schema_with_identifiers(&arrow_schema, &["id", "tenant_id"]).unwrap();

        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&schema.field_by_name("id").unwrap().id));
        assert!(ids.contains(&schema.field_by_name("tenant_id").unwrap().id));
    }

    #[test]
    fn test_arrow_to_iceberg_schema_with_no_identifiers() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));

        let schema = arrow_to_iceberg_schema_with_identifiers(&arrow_schema, &[]).unwrap();
        let ids: Vec<i32> = schema.identifier_field_ids().collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_reconcile_propagates_identifier_fields() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));

        let catalog_schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let result = reconcile_schema(&arrow_schema, &catalog_schema, &["id"]).unwrap();

        match result {
            SchemaReconciliation::Evolved { schema, .. } => {
                let ids: Vec<i32> = schema.identifier_field_ids().collect();
                assert_eq!(
                    ids,
                    vec![1],
                    "identifier should be the 'id' field with ID 1"
                );
            }
            other => panic!("expected Evolved, got {other:?}"),
        }
    }

    #[test]
    fn test_reconcile_detects_identifier_change() {
        let arrow_schema = Arc::new(ArrowSchemaType::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Catalog has identifier on "id" only.
        let catalog_schema = Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        // Now declare both "id" and "name" as identifiers.
        let result = reconcile_schema(&arrow_schema, &catalog_schema, &["id", "name"]).unwrap();

        match result {
            SchemaReconciliation::Evolved { schema, .. } => {
                let mut ids: Vec<i32> = schema.identifier_field_ids().collect();
                ids.sort();
                assert_eq!(ids, vec![1, 2]);
            }
            other => panic!("expected Evolved due to identifier change, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_identifier_field_ids() {
        let schema = catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "email", Type::Primitive(PrimitiveType::String)).into(),
        ]);

        let ids = resolve_identifier_field_ids(&schema, &["id", "email"]);
        assert_eq!(ids, vec![1, 3]);

        // Unknown names are skipped.
        let ids = resolve_identifier_field_ids(&schema, &["id", "nonexistent"]);
        assert_eq!(ids, vec![1]);
    }

    // --- Sort order tests ---

    fn test_schema() -> Schema {
        catalog_schema_with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "timestamp", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
            NestedField::optional(3, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "region", Type::Primitive(PrimitiveType::String)).into(),
        ])
    }

    #[test]
    fn test_build_sort_order_empty() {
        let schema = test_schema();
        let order = build_sort_order(&schema, &[]).unwrap();
        assert!(order.is_unsorted());
    }

    #[test]
    fn test_build_sort_order_single_asc() {
        let schema = test_schema();
        let defs = [SortFieldDef {
            name: "timestamp",
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
            order: 0,
        }];

        let order = build_sort_order(&schema, &defs).unwrap();
        assert_eq!(order.fields.len(), 1);
        assert_eq!(order.fields[0].source_id, 2);
        assert_eq!(order.fields[0].direction, SortDirection::Ascending);
        assert_eq!(order.fields[0].null_order, NullOrder::First);
        assert_eq!(order.fields[0].transform, Transform::Identity);
        assert_ne!(
            order.order_id, 0,
            "non-empty sort order must not use reserved unsorted order_id 0"
        );
    }

    #[test]
    fn test_build_sort_order_multi_with_explicit_order() {
        let schema = test_schema();
        // Define fields out of struct order but with explicit priorities.
        let defs = [
            SortFieldDef {
                name: "name",
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
                order: 2,
            },
            SortFieldDef {
                name: "timestamp",
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
                order: 0,
            },
        ];

        let order = build_sort_order(&schema, &defs).unwrap();
        assert_eq!(order.fields.len(), 2);
        // Sorted by order priority: timestamp (0) first, name (2) second.
        assert_eq!(order.fields[0].source_id, 2); // timestamp
        assert_eq!(order.fields[0].direction, SortDirection::Descending);
        assert_eq!(order.fields[1].source_id, 3); // name
        assert_eq!(order.fields[1].direction, SortDirection::Ascending);
        assert_ne!(
            order.order_id, 0,
            "non-empty sort order must not use reserved unsorted order_id 0"
        );
    }

    #[test]
    fn test_build_sort_order_missing_field() {
        let schema = test_schema();
        let defs = [SortFieldDef {
            name: "nonexistent",
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
            order: 0,
        }];

        let err = build_sort_order(&schema, &defs).unwrap_err();
        assert!(format!("{err}").contains("nonexistent"));
    }

    // --- Partition spec tests ---

    #[test]
    fn test_build_partition_spec_empty() {
        let schema = test_schema();
        let spec = build_partition_spec(&schema, &[]).unwrap();
        assert!(spec.fields().is_empty());
    }

    #[test]
    fn test_build_partition_spec_identity() {
        let schema = test_schema();
        let defs = [PartitionFieldDef {
            name: "region",
            transform: Transform::Identity,
        }];

        let spec = build_partition_spec(&schema, &defs).unwrap();
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].source_id, 4);
        assert_eq!(spec.fields()[0].name, "region");
        assert_eq!(spec.fields()[0].transform, Transform::Identity);
        assert_eq!(
            spec.fields()[0].field_id,
            Some(1000),
            "partition field IDs must start at 1000"
        );
        assert_eq!(
            spec.spec_id(),
            Some(0),
            "initial partition spec must have spec_id 0"
        );
    }

    #[test]
    fn test_build_partition_spec_day() {
        let schema = test_schema();
        let defs = [PartitionFieldDef {
            name: "timestamp",
            transform: Transform::Day,
        }];

        let spec = build_partition_spec(&schema, &defs).unwrap();
        assert_eq!(spec.fields().len(), 1);
        assert_eq!(spec.fields()[0].source_id, 2);
        assert_eq!(spec.fields()[0].name, "timestamp_day");
        assert_eq!(spec.fields()[0].transform, Transform::Day);
    }

    #[test]
    fn test_build_partition_spec_bucket() {
        let schema = test_schema();
        let defs = [PartitionFieldDef {
            name: "region",
            transform: Transform::Bucket(16),
        }];

        let spec = build_partition_spec(&schema, &defs).unwrap();
        assert_eq!(spec.fields()[0].name, "region_bucket_16");
        assert_eq!(spec.fields()[0].transform, Transform::Bucket(16));
    }

    #[test]
    fn test_build_partition_spec_truncate() {
        let schema = test_schema();
        let defs = [PartitionFieldDef {
            name: "name",
            transform: Transform::Truncate(100),
        }];

        let spec = build_partition_spec(&schema, &defs).unwrap();
        assert_eq!(spec.fields()[0].name, "name_truncate_100");
        assert_eq!(spec.fields()[0].transform, Transform::Truncate(100));
    }

    #[test]
    fn test_build_partition_spec_composite() {
        let schema = test_schema();
        let defs = [
            PartitionFieldDef {
                name: "timestamp",
                transform: Transform::Day,
            },
            PartitionFieldDef {
                name: "region",
                transform: Transform::Bucket(8),
            },
        ];

        let spec = build_partition_spec(&schema, &defs).unwrap();
        assert_eq!(spec.fields().len(), 2);
        assert_eq!(spec.fields()[0].name, "timestamp_day");
        assert_eq!(spec.fields()[1].name, "region_bucket_8");
        assert_eq!(
            spec.fields()[0].field_id,
            Some(1000),
            "first partition field ID must be 1000"
        );
        assert_eq!(
            spec.fields()[1].field_id,
            Some(1001),
            "second partition field ID must be 1001"
        );
    }

    #[test]
    fn test_build_partition_spec_missing_field() {
        let schema = test_schema();
        let defs = [PartitionFieldDef {
            name: "nonexistent",
            transform: Transform::Identity,
        }];

        let err = build_partition_spec(&schema, &defs).unwrap_err();
        assert!(format!("{err}").contains("nonexistent"));
    }
}
