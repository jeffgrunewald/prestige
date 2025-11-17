use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, DeriveInput, Data, Fields, Error,
    Type, TypePath, TypeArray, Expr, ExprLit, Lit, spanned::Spanned
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

        // Check if this is a fixed-size byte array that needs special handling
        if let Some(array_size) = extract_fixed_byte_array_size(field_type) {
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
    let field_bounds = fields.iter().map(|field| {
        let field_type = &field.ty;
        quote! { #field_type: ::prestige::ParquetSerialize }
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

        // Check if this is a fixed-size byte array that needs special handling
        if let Some(array_size) = extract_fixed_byte_array_size(field_type) {
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
    let field_bounds = fields.iter().map(|field| {
        let field_type = &field.ty;
        quote! { #field_type: ::prestige::ArrowSerialize }
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
