use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, DeriveInput, Data, Fields, Error,
    Type, TypePath, TypeArray, Expr, ExprLit, Lit, GenericArgument, PathArguments, spanned::Spanned
};

/// Helper function to extract the size from [u8; N] array types
fn extract_fixed_byte_array_size(ty: &Type) -> Option<i32> {
    if let Type::Array(TypeArray { elem, len, .. }) = ty {
        // Check if element type is u8
        if let Type::Path(TypePath { path, .. }) = elem.as_ref() {
            if let Some(last_segment) = path.segments.last() {
                if last_segment.ident == "u8" {
                    // Extract the array length
                    if let Expr::Lit(ExprLit { lit: Lit::Int(int_lit), .. }) = len {
                        if let Ok(length) = int_lit.base10_parse::<i32>() {
                            return Some(length);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Helper function to extract the inner type from Option<T>
/// Returns Some(T) if the type is Option<T>, None otherwise
fn extract_option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        // Check if this is a single-segment path (no ::) or ends with Option
        if let Some(last_segment) = path.segments.last() {
            if last_segment.ident == "Option" {
                // Extract the generic argument
                if let PathArguments::AngleBracketed(ref args) = last_segment.arguments {
                    if args.args.len() == 1 {
                        if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                            return Some(inner_ty);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Helper function to extract named fields from a DeriveInput
fn extract_named_fields(input: &DeriveInput) -> Result<&syn::punctuated::Punctuated<syn::Field, syn::token::Comma>, Error> {
    match input.data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => Ok(&fields.named),
                _ => Err(Error::new(input.span(), "Can only be derived for structs with named fields")),
            }
        },
        _ => Err(Error::new(input.span(), "Can only be derived for structs")),
    }
}

/// Generate Arrow field schemas for all fields in a struct
fn generate_arrow_field_schemas(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<proc_macro2::TokenStream> {
    fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let field_type = &field.ty;

        if let Some(inner_type) = extract_option_inner_type(field_type) {
            if let Some(array_size) = extract_fixed_byte_array_size(inner_type) {
                quote! {
                    arrow::datatypes::Field::new(#field_name, arrow::datatypes::DataType::FixedSizeBinary(#array_size), true)
                }
            } else {
                quote! {
                    arrow::datatypes::Field::new(#field_name, <#inner_type as ::prestige::ArrowSerialize>::arrow_data_type(), true)
                }
            }
        } else if let Some(array_size) = extract_fixed_byte_array_size(field_type) {
            quote! {
                arrow::datatypes::Field::new(#field_name, arrow::datatypes::DataType::FixedSizeBinary(#array_size), false)
            }
        } else {
            quote! {
                arrow::datatypes::Field::new(#field_name, <#field_type as ::prestige::ArrowSerialize>::arrow_data_type(), false)
            }
        }
    }).collect()
}

/// Generate trait bounds for Arrow serialization
fn generate_arrow_field_bounds(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<proc_macro2::TokenStream> {
    fields.iter().map(|field| {
        let field_type = &field.ty;
        if let Some(inner_type) = extract_option_inner_type(field_type) {
            quote! { #inner_type: ::prestige::ArrowSerialize }
        } else {
            quote! { #field_type: ::prestige::ArrowSerialize }
        }
    }).collect()
}

/// Generate the ArrowGroup implementation
fn generate_arrow_group_impl(name: &syn::Ident, fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>) -> proc_macro2::TokenStream {
    let arrow_field_schemas = generate_arrow_field_schemas(fields);
    let arrow_field_bounds = generate_arrow_field_bounds(fields);

    quote! {
        impl #name
        where
            #(#arrow_field_bounds),*
        {
            pub fn arrow_schema() -> arrow::datatypes::Schema {
                arrow::datatypes::Schema::new(vec![
                    #(#arrow_field_schemas),*
                ])
            }
        }
    }
}

/// Generate the ArrowReader implementation
fn generate_arrow_reader_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name
        where
            Self: for<'de> serde::Deserialize<'de>,
        {
            pub fn from_arrow_records(
                arrays: &[std::sync::Arc<dyn arrow::array::Array>],
                schema: &arrow::datatypes::Schema,
            ) -> Result<Vec<Self>, serde_arrow::Error> {
                serde_arrow::from_arrow(schema.fields(), arrays)
            }

            pub fn from_arrow_reader<R: std::io::Read + std::io::Seek>(
                reader: R,
            ) -> Result<Vec<Self>, Box<dyn std::error::Error>> {
                use arrow::ipc::reader::FileReader;

                let arrow_reader = FileReader::try_new(reader, None)?;
                let schema = arrow_reader.schema();
                let mut records = Vec::new();

                for batch_result in arrow_reader {
                    let batch = batch_result?;
                    let arrays: Vec<std::sync::Arc<dyn arrow::array::Array>> =
                        (0..batch.num_columns())
                            .map(|i| batch.column(i).clone())
                            .collect();

                    let batch_records = Self::from_arrow_records(&arrays, &schema)?;
                    records.extend(batch_records);
                }

                Ok(records)
            }
        }
    }
}

/// Generate the ArrowWriter implementation
fn generate_arrow_writer_impl(name: &syn::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl #name
        where
            Self: serde::Serialize,
        {
            pub fn to_arrow_arrays(
                records: &[Self],
            ) -> Result<(Vec<std::sync::Arc<dyn arrow::array::Array>>, arrow::datatypes::Schema), serde_arrow::Error> {
                if records.is_empty() {
                    return Ok((Vec::new(), Self::arrow_schema()));
                }

                let arrow_schema = Self::arrow_schema();
                let arrays = serde_arrow::to_arrow(arrow_schema.fields(), records)?;
                Ok((arrays, arrow_schema))
            }

            pub fn write_arrow_file<W: std::io::Write + std::io::Seek>(
                records: &[Self],
                writer: W,
            ) -> Result<(), Box<dyn std::error::Error>> {
                use arrow::ipc::writer::FileWriter;

                let (arrays, schema) = Self::to_arrow_arrays(records)?;
                let batch = arrow::record_batch::RecordBatch::try_new(
                    std::sync::Arc::new(schema.clone()),
                    arrays,
                )?;

                let mut arrow_writer = FileWriter::try_new(writer, &schema)?;
                arrow_writer.write(&batch)?;
                arrow_writer.finish()?;

                Ok(())
            }

            pub fn write_arrow_stream<W: std::io::Write>(
                records: &[Self],
                writer: W,
            ) -> Result<(), Box<dyn std::error::Error>> {
                use arrow::ipc::writer::StreamWriter;

                let (arrays, schema) = Self::to_arrow_arrays(records)?;
                let batch = arrow::record_batch::RecordBatch::try_new(
                    std::sync::Arc::new(schema.clone()),
                    arrays,
                )?;

                let mut arrow_writer = StreamWriter::try_new(writer, &schema)?;
                arrow_writer.write(&batch)?;
                arrow_writer.finish()?;

                Ok(())
            }
        }
    }
}

#[proc_macro_derive(ArrowGroup)]
pub fn derive_arrow_group(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match extract_named_fields(&input) {
        Ok(fields) => fields,
        Err(err) => return err.to_compile_error().into(),
    };

    let expanded = generate_arrow_group_impl(name, fields);
    TokenStream::from(expanded)
}

#[proc_macro_derive(ArrowReader)]
pub fn derive_arrow_reader(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = generate_arrow_reader_impl(name);
    TokenStream::from(expanded)
}

#[proc_macro_derive(ArrowWriter)]
pub fn derive_arrow_writer(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = generate_arrow_writer_impl(name);
    TokenStream::from(expanded)
}

/// Comprehensive derive macro that implements all Prestige traits and schemas
///
/// This macro is a convenience wrapper that applies all the necessary derive macros
/// and trait implementations for working with Prestige parquet files.
///
/// It automatically derives:
/// - `ArrowGroup` - for Arrow schema generation
/// - `ArrowReader` - for reading from Arrow/Parquet
/// - `ArrowWriter` - for writing to Arrow/Parquet
/// - Implements `ArrowSchema` trait (wrapping the generated arrow_schema method)
///
/// # Requirements
///
/// The type **must** implement `serde::Serialize` and `serde::Deserialize` because
/// the generated code uses `serde_arrow` for converting between Rust types and Arrow arrays.
/// These traits should be derived before `PrestigeSchema`:
///
/// ```rust,ignore
/// #[derive(Serialize, Deserialize, PrestigeSchema)]  // Correct order
/// struct MyData { ... }
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use prestige::PrestigeSchema;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize, PrestigeSchema)]
/// struct SensorData {
///     timestamp: u64,
///     sensor_id: String,
///     temperature: f32,
///     device_mac: [u8; 6],
/// }
/// ```
#[proc_macro_derive(PrestigeSchema)]
pub fn derive_prestige_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match extract_named_fields(&input) {
        Ok(fields) => fields,
        Err(err) => return err.to_compile_error().into(),
    };

    // Generate all implementations using helper functions
    let arrow_group = generate_arrow_group_impl(name, fields);
    let arrow_reader = generate_arrow_reader_impl(name);
    let arrow_writer = generate_arrow_writer_impl(name);

    let expanded = quote! {
        // ArrowGroup implementation - generates arrow_schema() method
        #arrow_group

        // ArrowSchema trait implementation
        impl ::prestige::ArrowSchema for #name {
            fn arrow_schema() -> arrow::datatypes::SchemaRef {
                std::sync::Arc::new(Self::arrow_schema())
            }
        }

        // ArrowReader implementation
        #arrow_reader

        // ArrowWriter implementation
        #arrow_writer
    };

    TokenStream::from(expanded)
}
