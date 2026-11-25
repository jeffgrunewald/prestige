pub trait ParquetSerialize {
    fn parquet_schema_element() -> parquet::schema::types::Type;
}

pub trait ArrowSerialize {
    fn arrow_data_type() -> arrow::datatypes::DataType;
}

/// Trait for types that can provide a complete Arrow schema
///
/// This is typically derived or implemented for structs that will be written to parquet.
pub trait ArrowSchema {
    fn arrow_schema() -> arrow::datatypes::SchemaRef;
}

macro_rules! impl_parquet_serialize {
    ($rust_type:ty, $physical_type:expr, $logical_type:expr) => {
        impl ParquetSerialize for $rust_type {
            fn parquet_schema_element() -> parquet::schema::types::Type {
                use parquet::schema::types::Type;
                Type::primitive_type_builder("field", $physical_type)
                    .with_logical_type($logical_type)
                    .with_repetition(parquet::basic::Repetition::REQUIRED)
                    .build()
                    .expect("Failed to build parquet schema element")
            }
        }
    };
}

macro_rules! impl_arrow_serialize {
    ($rust_type:ty, $arrow_type:expr) => {
        impl ArrowSerialize for $rust_type {
            fn arrow_data_type() -> arrow::datatypes::DataType {
                $arrow_type
            }
        }
    };
}

impl_parquet_serialize!(
    i8,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 8,
        is_signed: true
    })
);
impl_arrow_serialize!(i8, arrow::datatypes::DataType::Int8);

impl_parquet_serialize!(
    i16,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 16,
        is_signed: true
    })
);
impl_arrow_serialize!(i16, arrow::datatypes::DataType::Int16);

impl_parquet_serialize!(i32, parquet::basic::Type::INT32, None);
impl_arrow_serialize!(i32, arrow::datatypes::DataType::Int32);

impl_parquet_serialize!(i64, parquet::basic::Type::INT64, None);
impl_arrow_serialize!(i64, arrow::datatypes::DataType::Int64);

impl_parquet_serialize!(
    u8,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 8,
        is_signed: false
    })
);
impl_arrow_serialize!(u8, arrow::datatypes::DataType::UInt8);

impl_parquet_serialize!(
    u16,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 16,
        is_signed: false
    })
);
impl_arrow_serialize!(u16, arrow::datatypes::DataType::UInt16);

impl_parquet_serialize!(
    u32,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 32,
        is_signed: false
    })
);
impl_arrow_serialize!(u32, arrow::datatypes::DataType::UInt32);

impl_parquet_serialize!(
    u64,
    parquet::basic::Type::INT64,
    Some(parquet::basic::LogicalType::Integer {
        bit_width: 64,
        is_signed: false
    })
);
impl_arrow_serialize!(u64, arrow::datatypes::DataType::UInt64);

impl_parquet_serialize!(f32, parquet::basic::Type::FLOAT, None);
impl_arrow_serialize!(f32, arrow::datatypes::DataType::Float32);

impl_parquet_serialize!(f64, parquet::basic::Type::DOUBLE, None);
impl_arrow_serialize!(f64, arrow::datatypes::DataType::Float64);

impl_parquet_serialize!(bool, parquet::basic::Type::BOOLEAN, None);
impl_arrow_serialize!(bool, arrow::datatypes::DataType::Boolean);

impl_parquet_serialize!(
    String,
    parquet::basic::Type::BYTE_ARRAY,
    Some(parquet::basic::LogicalType::String)
);
impl_arrow_serialize!(String, arrow::datatypes::DataType::Utf8);

// Fixed-size byte array support.
//
// By default, [u8; N] maps to FixedSizeList(N, UInt8) — the structural
// representation as a list of integers. Use `#[prestige(as_binary)]` on struct
// fields to opt into FixedSizeBinary(N) encoding with automatic serde_bytes.
impl<const N: usize> ParquetSerialize for [u8; N] {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::basic::{LogicalType, Repetition};
        use parquet::schema::types::Type;
        use std::sync::Arc;

        let element = Arc::new(
            Type::primitive_type_builder("element", parquet::basic::Type::INT32)
                .with_logical_type(Some(LogicalType::Integer {
                    bit_width: 8,
                    is_signed: false,
                }))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("Failed to build element type"),
        );

        let list_group = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![element])
            .build()
            .expect("Failed to build list wrapper group");

        Type::group_type_builder("field")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .expect("Failed to build parquet LIST schema element")
    }
}

impl<const N: usize> ArrowSerialize for [u8; N] {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        // Use variable-length List(UInt8) rather than FixedSizeList because serde_arrow
        // doesn't support deserialize_tuple for FixedSizeList. serde serializes [T; N] as
        // a tuple, and serde_arrow can map that to/from a List column.
        arrow::datatypes::DataType::List(arrow::datatypes::FieldRef::new(
            arrow::datatypes::Field::new("item", arrow::datatypes::DataType::UInt8, true),
        ))
    }
}

// Temporal layouts match prestige 0.4 (chrono), so 0.5 reads files that 0.4 wrote.
// UTC timestamps are microseconds (Iceberg `timestamptz`), civil datetimes milliseconds.
impl_parquet_serialize!(
    jiff::Timestamp,
    parquet::basic::Type::INT64,
    Some(parquet::basic::LogicalType::Timestamp {
        is_adjusted_to_u_t_c: true,
        unit: parquet::basic::TimeUnit::MICROS
    })
);
impl_arrow_serialize!(
    jiff::Timestamp,
    arrow::datatypes::DataType::Timestamp(
        arrow::datatypes::TimeUnit::Microsecond,
        Some("UTC".into())
    )
);

impl_parquet_serialize!(
    jiff::civil::DateTime,
    parquet::basic::Type::INT64,
    Some(parquet::basic::LogicalType::Timestamp {
        is_adjusted_to_u_t_c: false,
        unit: parquet::basic::TimeUnit::MILLIS
    })
);
impl_arrow_serialize!(
    jiff::civil::DateTime,
    arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
);

impl_parquet_serialize!(
    jiff::civil::Date,
    parquet::basic::Type::INT32,
    Some(parquet::basic::LogicalType::Date)
);
impl_arrow_serialize!(jiff::civil::Date, arrow::datatypes::DataType::Date32);

impl_parquet_serialize!(
    jiff::civil::Time,
    parquet::basic::Type::INT64,
    Some(parquet::basic::LogicalType::Time {
        is_adjusted_to_u_t_c: false,
        unit: parquet::basic::TimeUnit::NANOS
    })
);
impl_arrow_serialize!(
    jiff::civil::Time,
    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
);

#[cfg(feature = "decimal")]
mod decimal_impls {
    use super::*;
    use rust_decimal::Decimal;

    impl ParquetSerialize for Decimal {
        fn parquet_schema_element() -> parquet::schema::types::Type {
            use parquet::basic::{LogicalType, Type as PhysicalType};
            use parquet::schema::types::Type;

            // rust_decimal::Decimal is a 128-bit decimal with precision up to 28
            // We use 16 bytes (128 bits) to store the value
            // Precision: 28 (max digits), Scale: 10 (decimal places)
            Type::primitive_type_builder("field", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::Decimal {
                    scale: 10,
                    precision: 28,
                }))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .with_length(16) // 128 bits = 16 bytes
                .build()
                .expect("Failed to build parquet schema element")
        }
    }

    impl ArrowSerialize for Decimal {
        fn arrow_data_type() -> arrow::datatypes::DataType {
            // Decimal128 with precision 28 and scale 10
            // This matches the Parquet representation
            arrow::datatypes::DataType::Decimal128(28, 10)
        }
    }
}

impl<T: ArrowSerialize> ArrowSerialize for Vec<T> {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::List(arrow::datatypes::FieldRef::new(
            arrow::datatypes::Field::new("item", T::arrow_data_type(), true),
        ))
    }
}

impl<T: ParquetSerialize> ParquetSerialize for Vec<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::basic::{LogicalType, Repetition};
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Get the inner element type and rebuild it with name "element" and OPTIONAL repetition
        let inner_base = T::parquet_schema_element();
        let element = Arc::new(crate::rebuild_type_with_optional(inner_base, "element"));

        // Build the repeated wrapper group named "list"
        let list_group = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![element])
            .build()
            .expect("Failed to build list wrapper group");

        // Build the outer group with LIST logical type annotation
        Type::group_type_builder("field")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .expect("Failed to build parquet LIST schema element")
    }
}

// HashSet support - uses Parquet LIST logical type (same as Vec)
impl<T: ParquetSerialize> ParquetSerialize for std::collections::HashSet<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::basic::{LogicalType, Repetition};
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Get the inner element type and rebuild it with name "element" and OPTIONAL repetition
        let inner_base = T::parquet_schema_element();
        let element = Arc::new(crate::rebuild_type_with_optional(inner_base, "element"));

        // Build the repeated wrapper group named "list"
        let list_group = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![element])
            .build()
            .expect("Failed to build list wrapper group");

        // Build the outer group with LIST logical type annotation
        Type::group_type_builder("field")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .expect("Failed to build parquet LIST schema element")
    }
}

impl<T: ArrowSerialize> ArrowSerialize for std::collections::HashSet<T> {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::List(arrow::datatypes::FieldRef::new(
            arrow::datatypes::Field::new("item", T::arrow_data_type(), true),
        ))
    }
}

// HashMap support - uses Parquet MAP logical type
impl<K: ParquetSerialize, V: ParquetSerialize> ParquetSerialize
    for std::collections::HashMap<K, V>
{
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::basic::{LogicalType, Repetition};
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Build the key field (required/non-nullable)
        let key_base = K::parquet_schema_element();
        let key_field = Arc::new(crate::rebuild_type_as_required(key_base, "key"));

        // Build the value field (optional/nullable)
        let value_base = V::parquet_schema_element();
        let value_field = Arc::new(crate::rebuild_type_with_optional(value_base, "value"));

        // Build the key_value group (repeated)
        let key_value_group = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![key_field, value_field])
            .build()
            .expect("Failed to build key_value group");

        // Build the outer group with MAP logical type
        Type::group_type_builder("field")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value_group)])
            .build()
            .expect("Failed to build MAP schema element")
    }
}

impl<K: ArrowSerialize, V: ArrowSerialize> ArrowSerialize for std::collections::HashMap<K, V> {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, Field, Fields};

        // Map = List<Struct<key: K, value: V>>
        let key_field = Field::new("key", K::arrow_data_type(), false); // Non-nullable
        let value_field = Field::new("value", V::arrow_data_type(), true); // Nullable

        let entries_struct = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let entries_field = Field::new("entries", entries_struct, false);

        DataType::Map(
            std::sync::Arc::new(entries_field),
            false, // keys_sorted = false for HashMap
        )
    }
}

// BTreeMap support - similar to HashMap but with sorted keys hint
impl<K: ParquetSerialize, V: ParquetSerialize> ParquetSerialize
    for std::collections::BTreeMap<K, V>
{
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::basic::{LogicalType, Repetition};
        use parquet::schema::types::Type;
        use std::sync::Arc;

        // Build the key field (required/non-nullable)
        let key_base = K::parquet_schema_element();
        let key_field = Arc::new(crate::rebuild_type_as_required(key_base, "key"));

        // Build the value field (optional/nullable)
        let value_base = V::parquet_schema_element();
        let value_field = Arc::new(crate::rebuild_type_with_optional(value_base, "value"));

        // Build the key_value group (repeated)
        let key_value_group = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![key_field, value_field])
            .build()
            .expect("Failed to build key_value group");

        // Build the outer group with MAP logical type
        Type::group_type_builder("field")
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value_group)])
            .build()
            .expect("Failed to build MAP schema element")
    }
}

impl<K: ArrowSerialize, V: ArrowSerialize> ArrowSerialize for std::collections::BTreeMap<K, V> {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, Field, Fields};

        // Map = List<Struct<key: K, value: V>>
        let key_field = Field::new("key", K::arrow_data_type(), false); // Non-nullable
        let value_field = Field::new("value", V::arrow_data_type(), true); // Nullable

        let entries_struct = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let entries_field = Field::new("entries", entries_struct, false);

        DataType::Map(
            std::sync::Arc::new(entries_field),
            true, // keys_sorted = true for BTreeMap
        )
    }
}

impl<T: ParquetSerialize> ParquetSerialize for Option<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        // For Option<T>, we get the inner element's schema and rebuild it with OPTIONAL repetition
        let inner_base = T::parquet_schema_element();
        crate::rebuild_type_with_optional(inner_base, "field")
    }
}

impl<T: ArrowSerialize> ArrowSerialize for Option<T> {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        // Option<T> in Arrow is just the inner type - nullability is handled at the field level
        T::arrow_data_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::{Timestamp, civil};
    use std::collections::HashSet;

    #[test]
    fn test_vec_and_hashset_produce_identical_parquet_schemas() {
        // Test with i32 as a representative scalar type
        let vec_schema = Vec::<i32>::parquet_schema_element();
        let hashset_schema = HashSet::<i32>::parquet_schema_element();

        // Both should produce identical schemas since HashSet uses LIST, same as Vec
        assert_eq!(
            format!("{:?}", vec_schema),
            format!("{:?}", hashset_schema),
            "Vec<i32> and HashSet<i32> should produce identical Parquet schemas"
        );

        // Test with String as well
        let vec_schema = Vec::<String>::parquet_schema_element();
        let hashset_schema = HashSet::<String>::parquet_schema_element();

        assert_eq!(
            format!("{:?}", vec_schema),
            format!("{:?}", hashset_schema),
            "Vec<String> and HashSet<String> should produce identical Parquet schemas"
        );
    }

    #[test]
    fn test_vec_and_hashset_produce_identical_arrow_data_types() {
        // Test with i32
        let vec_type = Vec::<i32>::arrow_data_type();
        let hashset_type = HashSet::<i32>::arrow_data_type();

        assert_eq!(
            vec_type, hashset_type,
            "Vec<i32> and HashSet<i32> should produce identical Arrow data types"
        );

        // Test with String
        let vec_type = Vec::<String>::arrow_data_type();
        let hashset_type = HashSet::<String>::arrow_data_type();

        assert_eq!(
            vec_type, hashset_type,
            "Vec<String> and HashSet<String> should produce identical Arrow data types"
        );

        // Test with u64
        let vec_type = Vec::<u64>::arrow_data_type();
        let hashset_type = HashSet::<u64>::arrow_data_type();

        assert_eq!(
            vec_type, hashset_type,
            "Vec<u64> and HashSet<u64> should produce identical Arrow data types"
        );
    }

    #[test]
    fn test_hashset_schema_structure() {
        // Verify the schema has the expected LIST structure
        let schema = HashSet::<i32>::parquet_schema_element();

        // Should be a group type with LIST logical type
        assert!(schema.is_group());
        assert_eq!(
            schema.get_basic_info().logical_type_ref(),
            Some(&parquet::basic::LogicalType::List)
        );
        assert_eq!(schema.name(), "field");

        // Should have one child (the "list" group)
        if let parquet::schema::types::Type::GroupType { fields, .. } = &schema {
            assert_eq!(fields.len(), 1, "Should have one child field");
            let list_group = &fields[0];
            assert_eq!(list_group.name(), "list");
            assert_eq!(
                list_group.get_basic_info().repetition(),
                parquet::basic::Repetition::REPEATED
            );
        } else {
            panic!("Expected GroupType");
        }
    }

    #[test]
    fn test_hashset_arrow_type_is_list() {
        let data_type = HashSet::<i32>::arrow_data_type();

        // Should be a List type
        match data_type {
            arrow::datatypes::DataType::List(field) => {
                assert_eq!(field.name(), "item");
                assert_eq!(field.data_type(), &arrow::datatypes::DataType::Int32);
                assert!(field.is_nullable(), "List items should be nullable");
            }
            _ => panic!("Expected List data type, got {:?}", data_type),
        }
    }

    #[test]
    fn test_timestamp_uses_microsecond_precision() {
        assert_eq!(
            Timestamp::arrow_data_type(),
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            )
        );

        let schema = Timestamp::parquet_schema_element();
        assert_eq!(
            schema.get_basic_info().logical_type_ref(),
            Some(&parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS,
            })
        );
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TemporalRow {
        timestamp: Timestamp,
        datetime: civil::DateTime,
        date: civil::Date,
        time: civil::Time,
        maybe_timestamp: Option<Timestamp>,
    }

    fn temporal_fields() -> Vec<arrow::datatypes::FieldRef> {
        vec![
            arrow::datatypes::Field::new("timestamp", Timestamp::arrow_data_type(), false),
            arrow::datatypes::Field::new("datetime", civil::DateTime::arrow_data_type(), false),
            arrow::datatypes::Field::new("date", civil::Date::arrow_data_type(), false),
            arrow::datatypes::Field::new("time", civil::Time::arrow_data_type(), false),
            arrow::datatypes::Field::new(
                "maybe_timestamp",
                Option::<Timestamp>::arrow_data_type(),
                true,
            ),
        ]
        .into_iter()
        .map(std::sync::Arc::new)
        .collect()
    }

    #[test]
    fn test_jiff_types_round_trip_through_arrow() {
        let rows = vec![
            TemporalRow {
                timestamp: Timestamp::from_microsecond(1_700_000_000_123_456).unwrap(),
                datetime: civil::date(2024, 2, 29).at(13, 45, 7, 123_000_000),
                date: civil::date(1969, 7, 20),
                time: civil::time(23, 59, 59, 999_999_999),
                maybe_timestamp: None,
            },
            TemporalRow {
                timestamp: Timestamp::from_microsecond(-1_500_000).unwrap(),
                datetime: civil::date(1970, 1, 1).at(0, 0, 0, 0),
                date: civil::date(2038, 1, 19),
                time: civil::time(0, 0, 0, 0),
                maybe_timestamp: Some(Timestamp::UNIX_EPOCH),
            },
        ];

        let batch = serde_arrow::to_record_batch(&temporal_fields(), &rows).unwrap();
        let back: Vec<TemporalRow> = serde_arrow::from_record_batch(&batch).unwrap();

        assert_eq!(back, rows);
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1_700_000_000_123_456);
        assert_eq!(timestamps.value(1), -1_500_000);
    }

    #[test]
    fn test_temporal_parquet_layouts_match_prestige_0_4() {
        fn layout<T: ParquetSerialize>()
        -> (parquet::basic::Type, Option<parquet::basic::LogicalType>) {
            let schema = T::parquet_schema_element();
            (
                schema.get_physical_type(),
                schema.get_basic_info().logical_type_ref().cloned(),
            )
        }
        let timestamp = |utc, unit| {
            Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: utc,
                unit,
            })
        };

        assert_eq!(
            layout::<Timestamp>(),
            (
                parquet::basic::Type::INT64,
                timestamp(true, parquet::basic::TimeUnit::MICROS)
            )
        );
        assert_eq!(
            layout::<civil::DateTime>(),
            (
                parquet::basic::Type::INT64,
                timestamp(false, parquet::basic::TimeUnit::MILLIS)
            )
        );
        assert_eq!(
            layout::<civil::Date>(),
            (
                parquet::basic::Type::INT32,
                Some(parquet::basic::LogicalType::Date)
            )
        );
        assert_eq!(
            layout::<civil::Time>(),
            (
                parquet::basic::Type::INT64,
                Some(parquet::basic::LogicalType::Time {
                    is_adjusted_to_u_t_c: false,
                    unit: parquet::basic::TimeUnit::NANOS,
                })
            )
        );
    }
}
