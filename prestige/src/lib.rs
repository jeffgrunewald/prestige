mod error;
// pub mod file_meta;
// pub mod file_sink;
// pub mod file_source;
// pub mod file_store;
// pub mod file_upload;
mod settings;
pub mod traits;

pub use error::{Error, Result};
// pub use file_meta::FileMeta;
// pub use file_sink::{FileSink, FileSinkBuilder};
// pub use file_store::FileStore;
pub use settings::Settings;
pub use traits::{ParquetSerialize, ArrowSerialize};

/// Helper function to rebuild a parquet Type with OPTIONAL repetition and a new field name
/// This is used by the derive macros to properly handle Option<T> fields
pub fn rebuild_type_with_optional(base_type: parquet::schema::types::Type, field_name: &str) -> parquet::schema::types::Type {
    use parquet::schema::types::{Type, TypePtr};
    use parquet::basic::Repetition;
    use std::sync::Arc;

    match base_type {
        Type::PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {
            let mut builder = Type::primitive_type_builder(field_name, physical_type)
                .with_repetition(Repetition::OPTIONAL);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            if type_length >= 0 {
                builder = builder.with_length(type_length);
            }

            if scale >= 0 {
                builder = builder.with_scale(scale);
            }

            if precision >= 0 {
                builder = builder.with_precision(precision);
            }

            builder.build().expect("Failed to rebuild primitive type")
        },
        Type::GroupType { basic_info, fields } => {
            let mut builder = Type::group_type_builder(field_name)
                .with_repetition(Repetition::OPTIONAL);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            let fields_vec: Vec<TypePtr> = fields.iter().map(|f| Arc::clone(f)).collect();
            builder = builder.with_fields(fields_vec);

            builder.build().expect("Failed to rebuild group type")
        },
    }
}

/// Helper function to rebuild a parquet Type with REQUIRED repetition and a new field name
/// This is used for map keys which must be non-nullable
pub fn rebuild_type_as_required(base_type: parquet::schema::types::Type, field_name: &str) -> parquet::schema::types::Type {
    use parquet::schema::types::{Type, TypePtr};
    use parquet::basic::Repetition;
    use std::sync::Arc;

    match base_type {
        Type::PrimitiveType { basic_info, physical_type, type_length, scale, precision } => {
            let mut builder = Type::primitive_type_builder(field_name, physical_type)
                .with_repetition(Repetition::REQUIRED);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            if type_length >= 0 {
                builder = builder.with_length(type_length);
            }

            if scale >= 0 {
                builder = builder.with_scale(scale);
            }

            if precision >= 0 {
                builder = builder.with_precision(precision);
            }

            builder.build().expect("Failed to rebuild primitive type")
        },
        Type::GroupType { basic_info, fields } => {
            let mut builder = Type::group_type_builder(field_name)
                .with_repetition(Repetition::REQUIRED);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            let fields_vec: Vec<TypePtr> = fields.iter().map(|f| Arc::clone(f)).collect();
            builder = builder.with_fields(fields_vec);

            builder.build().expect("Failed to rebuild group type")
        },
    }
}
