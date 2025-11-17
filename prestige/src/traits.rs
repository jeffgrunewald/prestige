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

// We'll handle Vec<u8> specially in the generic Vec<T> implementation

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

impl<T: ParquetSerialize> ParquetSerialize for Option<T> {
    fn parquet_schema_element() -> parquet::schema::types::Type {
        // For Option<T>, we modify the inner element to be optional
        let inner_element = T::parquet_schema_element();
        // We can't easily modify the repetition of an existing element, so we rebuild it
        // This is a simplified approach - in practice we'd need more complex logic
        inner_element
    }
}