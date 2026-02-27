use crate::error::Result;
use crate::traits::ArrowSchema;
use arrow::datatypes::SchemaRef;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{Schema, UnboundPartitionSpec};

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchemaType};
    use std::sync::Arc;

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
}
