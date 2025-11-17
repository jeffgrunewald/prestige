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


#[proc_macro_derive(ParquetSchema)]
pub fn derive_parquet_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match input.data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => &fields.named,
                _ => return syn::Error::new(input.span(), "ParquetSchema can only be derived for structs with named fields")
                    .to_compile_error().into(),
            }
        },
        _ => return syn::Error::new(input.span(), "ParquetSchema can only be derived for structs")
            .to_compile_error().into(),
    };

    let schema = fields.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        // Check if this is an Option<T> type
        if let Some(inner_type) = extract_option_inner_type(field_type) {
            // For Option<T>, we need to build a schema with OPTIONAL repetition
            // Special handling for Option<[u8; N]>
            if let Some(array_size) = extract_fixed_byte_array_size(inner_type) {
                quote! {
                    parquet::schema::types::Type::primitive_type_builder(stringify!(#field_name), parquet::basic::Type::FIXED_LEN_BYTE_ARRAY)
                        .with_repetition(parquet::basic::Repetition::OPTIONAL)
                        .with_type_length(#array_size)
                        .build()
                        .expect("Failed to build parquet schema")
                }
            } else {
                // Get the base schema from the inner type and rebuild it with OPTIONAL repetition
                // We use a helper that rebuilds the Type with the correct field name and repetition
                quote! {
                    {
                        let base = <#inner_type as ::prestige::ParquetSerialize>::parquet_schema_element();
                        ::prestige::rebuild_type_with_optional(base, stringify!(#field_name))
                    }
                }
            }
        }
        // Check if this is a fixed-size byte array that needs special handling
        else if let Some(array_size) = extract_fixed_byte_array_size(field_type) {
            quote! {
                parquet::schema::types::Type::primitive_type_builder(stringify!(#field_name), parquet::basic::Type::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(parquet::basic::Repetition::REQUIRED)
                    .with_type_length(#array_size)
                    .build()
                    .expect("Failed to build parquet schema")
            }
        } else {
            quote! {
                <#field_type as ::prestige::ParquetSerialize>::parquet_schema_element()
            }
        }
    });

    // Add trait bounds for all field types
    // For Option<T>, we need bounds on T, not Option<T>
    let field_bounds = fields.iter().map(|field| {
        let field_type = &field.ty;
        if let Some(inner_type) = extract_option_inner_type(field_type) {
            quote! { #inner_type: ::prestige::ParquetSerialize }
        } else {
            quote! { #field_type: ::prestige::ParquetSerialize }
        }
    });

    let expanded = quote! {
        impl #name 
        where
            #(#field_bounds),*
        {
            pub fn parquet_schema() -> parquet::schema::types::Type {
                let mut fields = vec![#(#schema),*];

                parquet::schema::types::Type::group_type_builder(stringify!(#name))
                    .with_fields(&mut fields)
                    .with_repetition(parquet::basic::Repetition::REQUIRED)
                    .build()
                    .expect("Failed to build parquet schema")
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ArrowGroup)]
pub fn derive_arrow_group(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match input.data {
        Data::Struct(ref data) => {
            match data.fields {
                Fields::Named(ref fields) => &fields.named,
                _ => return Error::new(input.span(), "ArrowGroup can only be derived for structs with named fields")
                    .to_compile_error().into(),
            }
        },
        _ => return Error::new(input.span(), "ArrowGroup can only be derived for structs")
            .to_compile_error().into(),
    };

    let field_schemas = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let field_type = &field.ty;

        // Check if this is an Option<T> type
        if let Some(inner_type) = extract_option_inner_type(field_type) {
            // For Option<T>, set nullable=true and use the inner type
            if let Some(array_size) = extract_fixed_byte_array_size(inner_type) {
                quote! {
                    arrow::datatypes::Field::new(#field_name, arrow::datatypes::DataType::FixedSizeBinary(#array_size), true)
                }
            } else {
                quote! {
                    arrow::datatypes::Field::new(#field_name, <#inner_type as ::prestige::ArrowSerialize>::arrow_data_type(), true)
                }
            }
        }
        // Check if this is a fixed-size byte array that needs special handling
        else if let Some(array_size) = extract_fixed_byte_array_size(field_type) {
            quote! {
                arrow::datatypes::Field::new(#field_name, arrow::datatypes::DataType::FixedSizeBinary(#array_size), false)
            }
        } else {
            quote! {
                arrow::datatypes::Field::new(#field_name, <#field_type as ::prestige::ArrowSerialize>::arrow_data_type(), false)
            }
        }
    });

    // Add trait bounds for all field types
    // For Option<T>, we need bounds on T, not Option<T>
    let field_bounds = fields.iter().map(|field| {
        let field_type = &field.ty;
        if let Some(inner_type) = extract_option_inner_type(field_type) {
            quote! { #inner_type: ::prestige::ArrowSerialize }
        } else {
            quote! { #field_type: ::prestige::ArrowSerialize }
        }
    });

    let expanded = quote! {
        impl #name 
        where
            #(#field_bounds),*
        {
            pub fn arrow_schema() -> arrow::datatypes::Schema {
                arrow::datatypes::Schema::new(vec![
                    #(#field_schemas),*
                ])
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ArrowReader)]
pub fn derive_arrow_reader(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl #name {
            pub fn from_arrow_records(
                arrays: &[std::sync::Arc<dyn arrow::array::Array>],
                schema: &arrow::datatypes::Schema,
            ) -> Result<Vec<Self>, serde_arrow::Error> {
                let mut deserializer = serde_arrow::Deserializer::new(arrays, schema)?;
                let mut records = Vec::new();
                
                while let Some(record) = deserializer.next::<Self>()? {
                    records.push(record);
                }
                
                Ok(records)
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
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ArrowWriter)]
pub fn derive_arrow_writer(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let expanded = quote! {
        impl #name {
            pub fn to_arrow_arrays(
                records: &[Self],
            ) -> Result<(Vec<std::sync::Arc<dyn arrow::array::Array>>, arrow::datatypes::Schema), serde_arrow::Error> {
                if records.is_empty() {
                    return Ok((Vec::new(), Self::arrow_schema()));
                }

                let arrow_schema = Self::arrow_schema();
                let serde_schema = serde_arrow::schema::Schema::from(&arrow_schema);
                
                let mut serializer = serde_arrow::Serializer::new();
                for record in records {
                    serializer.push(record)?;
                }
                
                let arrays = serializer.to_arrays(&serde_schema)?;
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
    };

    TokenStream::from(expanded)
}
