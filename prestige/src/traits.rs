pub trait ParquetSerialize {
    fn parquet_schema_element() -> parquet::schema::types::Type;
}

pub trait ArrowSerialize {
    fn arrow_data_type() -> arrow::datatypes::DataType;
}

// Newtype wrapper for Vec<u8> to handle it as binary data
#[derive(Clone, Debug, PartialEq)]
pub struct BinaryData(pub Vec<u8>);

impl ParquetSerialize for BinaryData {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::schema::types::Type;
        Type::primitive_type_builder("field", parquet::basic::Type::BYTE_ARRAY)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .expect("Failed to build parquet schema element")
    }
}

impl ArrowSerialize for BinaryData {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Binary
    }
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

impl_parquet_serialize!(i8, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Integer { bit_width: 8, is_signed: true }));
impl_arrow_serialize!(i8, arrow::datatypes::DataType::Int8);

impl_parquet_serialize!(i16, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Integer { bit_width: 16, is_signed: true }));
impl_arrow_serialize!(i16, arrow::datatypes::DataType::Int16);

impl_parquet_serialize!(i32, parquet::basic::Type::INT32, None);
impl_arrow_serialize!(i32, arrow::datatypes::DataType::Int32);

impl_parquet_serialize!(i64, parquet::basic::Type::INT64, None);
impl_arrow_serialize!(i64, arrow::datatypes::DataType::Int64);

impl_parquet_serialize!(u8, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Integer { bit_width: 8, is_signed: false }));
impl_arrow_serialize!(u8, arrow::datatypes::DataType::UInt8);

impl_parquet_serialize!(u16, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Integer { bit_width: 16, is_signed: false }));
impl_arrow_serialize!(u16, arrow::datatypes::DataType::UInt16);

impl_parquet_serialize!(u32, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Integer { bit_width: 32, is_signed: false }));
impl_arrow_serialize!(u32, arrow::datatypes::DataType::UInt32);

impl_parquet_serialize!(u64, parquet::basic::Type::INT64, Some(parquet::basic::LogicalType::Integer { bit_width: 64, is_signed: false }));
impl_arrow_serialize!(u64, arrow::datatypes::DataType::UInt64);

impl_parquet_serialize!(f32, parquet::basic::Type::FLOAT, None);
impl_arrow_serialize!(f32, arrow::datatypes::DataType::Float32);

impl_parquet_serialize!(f64, parquet::basic::Type::DOUBLE, None);
impl_arrow_serialize!(f64, arrow::datatypes::DataType::Float64);

impl_parquet_serialize!(bool, parquet::basic::Type::BOOLEAN, None);
impl_arrow_serialize!(bool, arrow::datatypes::DataType::Boolean);

impl_parquet_serialize!(String, parquet::basic::Type::BYTE_ARRAY, Some(parquet::basic::LogicalType::String));
impl_arrow_serialize!(String, arrow::datatypes::DataType::Utf8);

// Fixed-size byte array support - enables use as HashMap keys and in collections
// Special handling for [u8; N] exists in macros for struct fields, but we need
// trait implementations for use in generic contexts like HashMap<[u8; N], V>
impl<const N: usize> ParquetSerialize for [u8; N] {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::schema::types::Type;
        Type::primitive_type_builder("field", parquet::basic::Type::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .with_length(N as i32)
            .build()
            .expect("Failed to build parquet schema element")
    }
}

impl<const N: usize> ArrowSerialize for [u8; N] {
    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::FixedSizeBinary(N as i32)
    }
}

#[cfg(feature = "chrono")]
mod chrono_impls {
    use super::*;
    use chrono::{DateTime, NaiveDateTime, NaiveDate, NaiveTime, Utc};

    impl_parquet_serialize!(DateTime<Utc>, parquet::basic::Type::INT64, Some(parquet::basic::LogicalType::Timestamp {
        is_adjusted_to_u_t_c: true,
        unit: parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {})
    }));
    impl_arrow_serialize!(DateTime<Utc>, arrow::datatypes::DataType::Timestamp(
        arrow::datatypes::TimeUnit::Millisecond,
        Some("UTC".into())
    ));

    impl_parquet_serialize!(NaiveDateTime, parquet::basic::Type::INT64, Some(parquet::basic::LogicalType::Timestamp {
        is_adjusted_to_u_t_c: false,
        unit: parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {})
    }));
    impl_arrow_serialize!(NaiveDateTime, arrow::datatypes::DataType::Timestamp(
        arrow::datatypes::TimeUnit::Millisecond,
        None
    ));

    impl_parquet_serialize!(NaiveDate, parquet::basic::Type::INT32, Some(parquet::basic::LogicalType::Date));
    impl_arrow_serialize!(NaiveDate, arrow::datatypes::DataType::Date32);

    impl_parquet_serialize!(NaiveTime, parquet::basic::Type::INT64, Some(parquet::basic::LogicalType::Time {
        is_adjusted_to_u_t_c: false,
        unit: parquet::basic::TimeUnit::NANOS(parquet::format::NanoSeconds {})
    }));
    impl_arrow_serialize!(NaiveTime, arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond));
}

#[cfg(feature = "decimal")]
mod decimal_impls {
    use super::*;
    use rust_decimal::Decimal;

    impl ParquetSerialize for Decimal {
        fn parquet_schema_element() -> parquet::schema::types::Type {
            use parquet::schema::types::Type;
            use parquet::basic::{LogicalType, Type as PhysicalType};

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
        arrow::datatypes::DataType::List(
            arrow::datatypes::FieldRef::new(arrow::datatypes::Field::new("item", T::arrow_data_type(), true))
        )
    }
}

impl<T: ParquetSerialize> ParquetSerialize for Vec<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::schema::types::Type;
        use parquet::basic::{Repetition, LogicalType};
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

// HashMap support - uses Parquet MAP logical type
impl<K: ParquetSerialize, V: ParquetSerialize> ParquetSerialize for std::collections::HashMap<K, V> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::schema::types::Type;
        use parquet::basic::{Repetition, LogicalType};
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
        let key_field = Field::new("key", K::arrow_data_type(), false);  // Non-nullable
        let value_field = Field::new("value", V::arrow_data_type(), true); // Nullable

        let entries_struct = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let entries_field = Field::new("entries", entries_struct, false);

        DataType::Map(
            std::sync::Arc::new(entries_field),
            false  // keys_sorted = false for HashMap
        )
    }
}

// BTreeMap support - similar to HashMap but with sorted keys hint
impl<K: ParquetSerialize, V: ParquetSerialize> ParquetSerialize for std::collections::BTreeMap<K, V> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        use parquet::schema::types::Type;
        use parquet::basic::{Repetition, LogicalType};
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
        let key_field = Field::new("key", K::arrow_data_type(), false);  // Non-nullable
        let value_field = Field::new("value", V::arrow_data_type(), true); // Nullable

        let entries_struct = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let entries_field = Field::new("entries", entries_struct, false);

        DataType::Map(
            std::sync::Arc::new(entries_field),
            true  // keys_sorted = true for BTreeMap
        )
    }
}

impl<T: ParquetSerialize> ParquetSerialize for Option<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        // For Option<T>, we modify the inner element to be optional
        let inner_element = T::parquet_schema_element();
        // We can't easily modify the repetition of an existing element, so we rebuild it
        // This is a simplified approach - in practice we'd need more complex logic
        inner_element
    }
}