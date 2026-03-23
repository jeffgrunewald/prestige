use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Error, Expr, ExprLit, Fields, GenericArgument, ItemStruct, Lit,
    PathArguments, Type, TypeArray, TypePath, parse_macro_input, spanned::Spanned,
};

/// Helper function to extract the size from [u8; N] array types
fn extract_fixed_byte_array_size(ty: &Type) -> Option<i32> {
    if let Type::Array(TypeArray { elem, len, .. }) = ty {
        // Check if element type is u8
        if let Type::Path(TypePath { path, .. }) = elem.as_ref()
            && let Some(last_segment) = path.segments.last()
            && last_segment.ident == "u8"
        {
            // Extract the array length
            if let Expr::Lit(ExprLit {
                lit: Lit::Int(int_lit),
                ..
            }) = len
                && let Ok(length) = int_lit.base10_parse::<i32>()
            {
                return Some(length);
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
        if let Some(last_segment) = path.segments.last()
            && last_segment.ident == "Option"
        {
            // Extract the generic argument
            if let PathArguments::AngleBracketed(ref args) = last_segment.arguments
                && args.args.len() == 1
                && let Some(GenericArgument::Type(inner_ty)) = args.args.first()
            {
                return Some(inner_ty);
            }
        }
    }
    None
}

/// Check if a type is `Vec<u8>`.
fn is_vec_u8(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty
        && let Some(last_segment) = path.segments.last()
        && last_segment.ident == "Vec"
        && let PathArguments::AngleBracketed(ref args) = last_segment.arguments
        && args.args.len() == 1
        && let Some(GenericArgument::Type(Type::Path(TypePath {
            path: inner_path, ..
        }))) = args.args.first()
        && let Some(seg) = inner_path.segments.last()
    {
        return seg.ident == "u8";
    }
    false
}

/// Check if a field has `#[prestige(as_binary)]` attribute.
fn has_as_binary_attr(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("prestige") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("as_binary") {
                found = true;
            }
            Ok(())
        });
        found
    })
}

/// Check if a field already has `#[serde(with = "serde_bytes")]` or similar.
fn has_serde_bytes_attr(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("serde") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("with")
                && let Ok(value) = meta.value()
                && let Ok(lit) = value.parse::<syn::LitStr>()
            {
                let val = lit.value();
                if val == "serde_bytes" || val.ends_with("::serde_bytes") {
                    found = true;
                }
            }
            Ok(())
        });
        found
    })
}

/// Helper function to extract named fields from a DeriveInput
fn extract_named_fields(
    input: &DeriveInput,
) -> Result<&syn::punctuated::Punctuated<syn::Field, syn::token::Comma>, Error> {
    match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => Ok(&fields.named),
            _ => Err(Error::new(
                input.span(),
                "Can only be derived for structs with named fields",
            )),
        },
        _ => Err(Error::new(input.span(), "Can only be derived for structs")),
    }
}

/// Check if a field has `#[prestige(identifier)]` attribute.
fn has_identifier_attr(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident("prestige") {
            return false;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("identifier") {
                found = true;
            }
            Ok(())
        });
        found
    })
}

/// Collect the string names of fields marked with `#[prestige(identifier)]`.
fn collect_identifier_field_names(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<String> {
    fields
        .iter()
        .filter(|f| has_identifier_attr(f))
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect()
}

/// Validate identifier field constraints at compile time.
fn validate_identifier_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Result<(), Error> {
    for field in fields {
        if !has_identifier_attr(field) {
            continue;
        }
        if extract_option_inner_type(&field.ty).is_some() {
            return Err(Error::new(
                field.span(),
                "identifier fields must be required (not Option<T>)",
            ));
        }
    }
    Ok(())
}

/// Parsed sort key metadata from `#[prestige(sort_key)]` or `#[prestige(sort_key(desc, order = 2))]`.
#[derive(Debug)]
struct SortKeyAttr {
    descending: bool,
    explicit_order: Option<u32>,
}

/// Parsed partition metadata from `#[prestige(partition)]` or `#[prestige(partition(day))]`.
#[derive(Debug)]
enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

/// Extract sort_key attribute from a field's `#[prestige(...)]` attributes.
fn extract_sort_key_attr(field: &syn::Field) -> Option<SortKeyAttr> {
    for attr in &field.attrs {
        if !attr.path().is_ident("prestige") {
            continue;
        }

        let mut found_sort_key = false;
        let mut descending = false;
        let mut explicit_order: Option<u32> = None;

        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("sort_key") {
                found_sort_key = true;
                // Check for parenthesized arguments: sort_key(desc, order = 2)
                if meta.input.peek(syn::token::Paren) {
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("desc") {
                            descending = true;
                        } else if inner.path.is_ident("asc") {
                            descending = false;
                        } else if inner.path.is_ident("order") {
                            let value = inner.value()?;
                            let lit: syn::LitInt = value.parse()?;
                            explicit_order = Some(lit.base10_parse()?);
                        }
                        Ok(())
                    })?;
                }
            }
            Ok(())
        });

        if found_sort_key {
            return Some(SortKeyAttr {
                descending,
                explicit_order,
            });
        }
    }
    None
}

/// Extract partition attribute from a field's `#[prestige(...)]` attributes.
fn extract_partition_attr(field: &syn::Field) -> Result<Option<PartitionTransform>, Error> {
    for attr in &field.attrs {
        if !attr.path().is_ident("prestige") {
            continue;
        }

        let mut found_partition = false;
        let mut transform = PartitionTransform::Identity;
        let mut parse_error: Option<Error> = None;

        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("partition") {
                found_partition = true;
                // Check for parenthesized arguments: partition(day), partition(bucket(16))
                if meta.input.peek(syn::token::Paren) {
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("year") {
                            transform = PartitionTransform::Year;
                        } else if inner.path.is_ident("month") {
                            transform = PartitionTransform::Month;
                        } else if inner.path.is_ident("day") {
                            transform = PartitionTransform::Day;
                        } else if inner.path.is_ident("hour") {
                            transform = PartitionTransform::Hour;
                        } else if inner.path.is_ident("bucket") {
                            let content;
                            syn::parenthesized!(content in inner.input);
                            let lit: syn::LitInt = content.parse()?;
                            let n: u32 = lit.base10_parse()?;
                            transform = PartitionTransform::Bucket(n);
                        } else if inner.path.is_ident("truncate") {
                            let content;
                            syn::parenthesized!(content in inner.input);
                            let lit: syn::LitInt = content.parse()?;
                            let w: u32 = lit.base10_parse()?;
                            transform = PartitionTransform::Truncate(w);
                        } else {
                            parse_error = Some(Error::new(
                                inner.path.span(),
                                "unknown partition transform; expected: year, month, day, hour, bucket(N), truncate(W)",
                            ));
                        }
                        Ok(())
                    })?;
                }
            }
            Ok(())
        });

        if let Some(err) = parse_error {
            return Err(err);
        }

        if found_partition {
            return Ok(Some(transform));
        }
    }
    Ok(None)
}

/// Collect sort field definitions from struct fields.
fn collect_sort_field_defs(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<(String, SortKeyAttr, usize)> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(pos, f)| {
            extract_sort_key_attr(f).map(|attr| (f.ident.as_ref().unwrap().to_string(), attr, pos))
        })
        .collect()
}

/// Collect partition field definitions from struct fields.
fn collect_partition_field_defs(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Result<Vec<(String, PartitionTransform)>, Error> {
    let mut result = Vec::new();
    for field in fields {
        if let Some(transform) = extract_partition_attr(field)? {
            result.push((field.ident.as_ref().unwrap().to_string(), transform));
        }
    }
    Ok(result)
}

/// Parsed struct-level `#[prestige(...)]` attributes.
#[derive(Debug, Default)]
struct StructPrestigeAttrs {
    table_name: Option<String>,
    namespace: Option<Vec<String>>,
}

/// Generate the `impl IcebergSchema for T` block (gated by `#[cfg(feature = "iceberg")]`).
fn generate_iceberg_schema_impl(
    name: &syn::Ident,
    struct_attrs: &StructPrestigeAttrs,
    identifier_names: &[String],
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Result<proc_macro2::TokenStream, Error> {
    let sort_defs = collect_sort_field_defs(fields);
    let partition_defs = collect_partition_field_defs(fields)?;

    // Build sort order method body
    let sort_order_impl = if sort_defs.is_empty() {
        quote! { None }
    } else {
        let sort_entries: Vec<proc_macro2::TokenStream> = sort_defs
            .iter()
            .map(|(field_name, attr, pos)| {
                let order = attr.explicit_order.unwrap_or(*pos as u32);
                let (dir, null) = if attr.descending {
                    (
                        quote! { ::prestige::iceberg::SortDirection::Descending },
                        quote! { ::prestige::iceberg::NullOrder::Last },
                    )
                } else {
                    (
                        quote! { ::prestige::iceberg::SortDirection::Ascending },
                        quote! { ::prestige::iceberg::NullOrder::First },
                    )
                };
                quote! {
                    ::prestige::iceberg::SortFieldDef {
                        name: #field_name,
                        direction: #dir,
                        null_order: #null,
                        order: #order,
                    }
                }
            })
            .collect();

        quote! {
            {
                let schema = Self::iceberg_schema();
                let defs = &[#(#sort_entries),*];
                // Field names are macro-generated from the struct — guaranteed to exist.
                Some(::prestige::iceberg::build_sort_order(&schema, defs).expect("sort order fields must exist in schema"))
            }
        }
    };

    // Build partition spec method body
    let partition_spec_impl = if partition_defs.is_empty() {
        quote! { None }
    } else {
        let partition_entries: Vec<proc_macro2::TokenStream> = partition_defs
            .iter()
            .map(|(field_name, transform)| {
                let transform_expr = match transform {
                    PartitionTransform::Identity => {
                        quote! { ::prestige::iceberg::Transform::Identity }
                    }
                    PartitionTransform::Year => quote! { ::prestige::iceberg::Transform::Year },
                    PartitionTransform::Month => quote! { ::prestige::iceberg::Transform::Month },
                    PartitionTransform::Day => quote! { ::prestige::iceberg::Transform::Day },
                    PartitionTransform::Hour => quote! { ::prestige::iceberg::Transform::Hour },
                    PartitionTransform::Bucket(n) => {
                        quote! { ::prestige::iceberg::Transform::Bucket(#n) }
                    }
                    PartitionTransform::Truncate(w) => {
                        quote! { ::prestige::iceberg::Transform::Truncate(#w) }
                    }
                };
                quote! {
                    ::prestige::iceberg::PartitionFieldDef {
                        name: #field_name,
                        transform: #transform_expr,
                    }
                }
            })
            .collect();

        quote! {
            {
                let schema = Self::iceberg_schema();
                let defs = &[#(#partition_entries),*];
                // Field names are macro-generated from the struct — guaranteed to exist.
                Some(::prestige::iceberg::build_partition_spec(&schema, defs).expect("partition fields must exist in schema"))
            }
        }
    };

    // default_table_name
    let table_name_impl = match &struct_attrs.table_name {
        Some(name) => quote! { Some(#name) },
        None => quote! { None },
    };

    // default_namespace
    let namespace_impl = match &struct_attrs.namespace {
        Some(parts) => {
            quote! { Some(&[#(#parts),*]) }
        }
        None => quote! { None },
    };

    // identifier_field_names
    let identifier_impl = quote! { &[#(#identifier_names),*] };

    Ok(quote! {
        #[allow(unexpected_cfgs)]
        #[cfg(feature = "iceberg")]
        impl ::prestige::iceberg::IcebergSchema for #name {
            fn iceberg_schema() -> ::prestige::iceberg::Schema {
                ::prestige::iceberg::arrow_to_iceberg_schema_with_identifiers(
                    &<Self as ::prestige::ArrowSchema>::arrow_schema(),
                    Self::identifier_field_names(),
                ).expect("arrow schema must convert to iceberg schema")
            }

            fn table_partition_spec() -> Option<::prestige::iceberg::UnboundPartitionSpec> {
                #partition_spec_impl
            }

            fn table_sort_order() -> Option<::prestige::iceberg::SortOrder> {
                #sort_order_impl
            }

            fn default_table_name() -> Option<&'static str> {
                #table_name_impl
            }

            fn default_namespace() -> Option<&'static [&'static str]> {
                #namespace_impl
            }

            fn identifier_field_names() -> &'static [&'static str] {
                #identifier_impl
            }
        }
    })
}

/// Generate the sort_field_definitions() and partition_field_defs() methods.
fn generate_sort_and_partition_impl(
    name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Result<proc_macro2::TokenStream, Error> {
    let sort_defs = collect_sort_field_defs(fields);
    let partition_defs = collect_partition_field_defs(fields)?;

    let sort_entries: Vec<proc_macro2::TokenStream> = sort_defs
        .iter()
        .map(|(field_name, attr, pos)| {
            let order = attr.explicit_order.unwrap_or(*pos as u32);
            let (dir, null) = if attr.descending {
                (
                    quote! { ::prestige::iceberg::SortDirection::Descending },
                    quote! { ::prestige::iceberg::NullOrder::Last },
                )
            } else {
                (
                    quote! { ::prestige::iceberg::SortDirection::Ascending },
                    quote! { ::prestige::iceberg::NullOrder::First },
                )
            };
            quote! {
                ::prestige::iceberg::SortFieldDef {
                    name: #field_name,
                    direction: #dir,
                    null_order: #null,
                    order: #order,
                }
            }
        })
        .collect();

    let partition_entries: Vec<proc_macro2::TokenStream> = partition_defs
        .iter()
        .map(|(field_name, transform)| {
            let transform_expr = match transform {
                PartitionTransform::Identity => quote! { ::prestige::iceberg::Transform::Identity },
                PartitionTransform::Year => quote! { ::prestige::iceberg::Transform::Year },
                PartitionTransform::Month => quote! { ::prestige::iceberg::Transform::Month },
                PartitionTransform::Day => quote! { ::prestige::iceberg::Transform::Day },
                PartitionTransform::Hour => quote! { ::prestige::iceberg::Transform::Hour },
                PartitionTransform::Bucket(n) => {
                    quote! { ::prestige::iceberg::Transform::Bucket(#n) }
                }
                PartitionTransform::Truncate(w) => {
                    quote! { ::prestige::iceberg::Transform::Truncate(#w) }
                }
            };
            quote! {
                ::prestige::iceberg::PartitionFieldDef {
                    name: #field_name,
                    transform: #transform_expr,
                }
            }
        })
        .collect();

    Ok(quote! {
        #[allow(unexpected_cfgs)]
        #[cfg(feature = "iceberg")]
        impl #name {
            pub fn sort_field_definitions() -> &'static [::prestige::iceberg::SortFieldDef] {
                &[#(#sort_entries),*]
            }

            pub fn partition_field_definitions() -> &'static [::prestige::iceberg::PartitionFieldDef] {
                &[#(#partition_entries),*]
            }
        }
    })
}

/// Generate Arrow field schemas for all fields in a struct.
///
/// Fields annotated with `#[prestige(as_binary)]` emit `FixedSizeBinary(N)` for
/// `[u8; N]` and `Binary` for `Vec<u8>`. All other fields delegate to the
/// `ArrowSerialize` trait, which maps `[u8; N]` to `FixedSizeList(N, UInt8)` and
/// `Vec<u8>` to `List(UInt8)` by default.
fn generate_arrow_field_schemas(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap().to_string();
            let field_type = &field.ty;
            let is_binary = has_as_binary_attr(field);

            // Unwrap Option<T> to determine inner type and nullability.
            let (inner_type, nullable) = match extract_option_inner_type(field_type) {
                Some(inner) => (inner, true),
                None => (field_type, false),
            };

            let data_type_expr = if is_binary {
                if let Some(size) = extract_fixed_byte_array_size(inner_type) {
                    quote! { arrow::datatypes::DataType::FixedSizeBinary(#size) }
                } else if is_vec_u8(inner_type) {
                    quote! { arrow::datatypes::DataType::Binary }
                } else {
                    // as_binary on a non-byte type: fall through to trait (will likely fail at compile time)
                    quote! { <#inner_type as ::prestige::ArrowSerialize>::arrow_data_type() }
                }
            } else {
                // Default path: delegate to ArrowSerialize trait for all types.
                quote! { <#inner_type as ::prestige::ArrowSerialize>::arrow_data_type() }
            };

            quote! {
                arrow::datatypes::Field::new(#field_name, #data_type_expr, #nullable)
            }
        })
        .collect()
}

/// Generate trait bounds for Arrow serialization
fn generate_arrow_field_bounds(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Vec<proc_macro2::TokenStream> {
    fields
        .iter()
        .map(|field| {
            let field_type = &field.ty;
            if let Some(inner_type) = extract_option_inner_type(field_type) {
                quote! { #inner_type: ::prestige::ArrowSerialize }
            } else {
                quote! { #field_type: ::prestige::ArrowSerialize }
            }
        })
        .collect()
}

/// Generate the ArrowGroup implementation
fn generate_arrow_group_impl(
    name: &syn::Ident,
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> proc_macro2::TokenStream {
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

/// Attribute macro that replaces `#[derive(PrestigeSchema)]` with full control over
/// the struct definition, enabling automatic serde annotation injection.
///
/// This macro generates all Prestige trait implementations (`ArrowSchema`,
/// `ArrowGroup`, `ArrowReader`, `ArrowWriter`, `IcebergSchema`) and auto-injects
/// `#[derive(serde::Serialize, serde::Deserialize)]` if not already present.
///
/// Supports struct-level arguments (`table`, `namespace`) and field-level attributes
/// (`identifier`, `sort_key`, `partition`, `as_binary`).
///
/// # Example
///
/// ```rust,ignore
/// #[prestige::prestige_schema(table = "events", namespace = "analytics")]
/// #[derive(Clone, Debug, PartialEq)]
/// struct Event {
///     #[prestige(identifier)]
///     id: String,
///     #[prestige(as_binary)]
///     payload: Vec<u8>,
///     #[prestige(as_binary)]
///     hash: [u8; 32],
///     #[prestige(sort_key)]
///     timestamp: i64,
/// }
/// ```
///
/// `Serialize` and `Deserialize` are auto-injected — no need to derive them manually.
///
/// Fields marked `as_binary` get `FixedSizeBinary(N)` / `Binary` Arrow types
/// and `#[serde(with = "::prestige::serde_bytes")]` is automatically injected
/// so serde_arrow uses efficient byte serialization.
#[proc_macro_attribute]
pub fn prestige_schema(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item = parse_macro_input!(input as ItemStruct);

    // Parse struct-level args from the attribute: #[prestige_schema(table = "...", namespace = "...")]
    let struct_attrs = match syn::parse::Parser::parse(
        |input: syn::parse::ParseStream| {
            let mut result = StructPrestigeAttrs::default();
            while !input.is_empty() {
                let ident: syn::Ident = input.parse()?;
                let _: syn::Token![=] = input.parse()?;
                let lit: syn::LitStr = input.parse()?;

                if ident == "table" {
                    result.table_name = Some(lit.value());
                } else if ident == "namespace" {
                    result.namespace = Some(lit.value().split('.').map(String::from).collect());
                } else {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown prestige_schema argument: {ident}"),
                    ));
                }

                // Consume optional trailing comma
                let _ = input.parse::<syn::Token![,]>();
            }
            Ok(result)
        },
        args,
    ) {
        Ok(attrs) => attrs,
        Err(err) => return err.to_compile_error().into(),
    };

    // Extract named fields (must be a struct with named fields).
    let fields = match &mut item.fields {
        Fields::Named(named) => named,
        _ => {
            return syn::Error::new(item.ident.span(), "prestige_schema requires named fields")
                .to_compile_error()
                .into();
        }
    };

    // Auto-inject #[derive(serde::Serialize, serde::Deserialize)] if not already present.
    // This ensures correct attribute ordering (prestige_schema runs first, then derives).
    {
        let has_serialize = item.attrs.iter().any(|attr| {
            if !attr.path().is_ident("derive") {
                return false;
            }
            let mut found = false;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("Serialize")
                    || meta.path.segments.len() == 2
                        && meta.path.segments[0].ident == "serde"
                        && meta.path.segments[1].ident == "Serialize"
                {
                    found = true;
                }
                Ok(())
            });
            found
        });

        let has_deserialize = item.attrs.iter().any(|attr| {
            if !attr.path().is_ident("derive") {
                return false;
            }
            let mut found = false;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("Deserialize")
                    || meta.path.segments.len() == 2
                        && meta.path.segments[0].ident == "serde"
                        && meta.path.segments[1].ident == "Deserialize"
                {
                    found = true;
                }
                Ok(())
            });
            found
        });

        if !has_serialize || !has_deserialize {
            let derive_attr: syn::Attribute = if !has_serialize && !has_deserialize {
                syn::parse_quote! { #[derive(serde::Serialize, serde::Deserialize)] }
            } else if !has_serialize {
                syn::parse_quote! { #[derive(serde::Serialize)] }
            } else {
                syn::parse_quote! { #[derive(serde::Deserialize)] }
            };
            item.attrs.push(derive_attr);
        }
    }

    // Walk fields: inject serde helpers on byte-typed fields.
    //
    // - `[u8; N]` with as_binary → serde_bytes (binary encoding)
    // - `[u8; N]` without as_binary → serde_u8_array (seq protocol for list encoding)
    // - `Vec<u8>` with as_binary → serde_bytes (binary encoding)
    // - `Vec<u8>` without as_binary → no injection (serde default seq protocol works)
    // - Option variants use appropriate Option-aware helpers.
    for field in fields.named.iter_mut() {
        if has_serde_bytes_attr(field) {
            continue; // User already provided explicit serde annotation.
        }

        let is_binary = has_as_binary_attr(field);
        let ty = &field.ty;

        // Check for Option<inner> wrapping.
        let (inner_ty, is_option) = match extract_option_inner_type(ty) {
            Some(inner) => (inner.clone(), true),
            None => (ty.clone(), false),
        };

        let is_fixed_u8 = extract_fixed_byte_array_size(&inner_ty).is_some();
        let is_vec_u8_field = is_vec_u8(&inner_ty);

        if is_binary && (is_fixed_u8 || is_vec_u8_field) {
            // as_binary: inject serde_bytes (handles both direct and Option types).
            field.attrs.push(syn::parse_quote! {
                #[serde(with = "::prestige::serde_bytes")]
            });
        } else if !is_binary && is_fixed_u8 {
            // Default [u8; N]: inject seq-protocol helper.
            if is_option {
                field.attrs.push(syn::parse_quote! {
                    #[serde(with = "::prestige::serde_u8_array::option")]
                });
            } else {
                field.attrs.push(syn::parse_quote! {
                    #[serde(with = "::prestige::serde_u8_array")]
                });
            }
        }
        // Vec<u8> without as_binary: no injection needed.
    }

    // Validate identifier fields.
    if let Err(err) = validate_identifier_fields(&fields.named) {
        return err.to_compile_error().into();
    }

    let identifier_names = collect_identifier_field_names(&fields.named);
    let name = &item.ident;

    let sort_and_partition = match generate_sort_and_partition_impl(name, &fields.named) {
        Ok(tokens) => tokens,
        Err(err) => return err.to_compile_error().into(),
    };

    let iceberg_schema_impl =
        match generate_iceberg_schema_impl(name, &struct_attrs, &identifier_names, &fields.named) {
            Ok(tokens) => tokens,
            Err(err) => return err.to_compile_error().into(),
        };

    let arrow_group = generate_arrow_group_impl(name, &fields.named);
    let arrow_reader = generate_arrow_reader_impl(name);
    let arrow_writer = generate_arrow_writer_impl(name);

    // Strip #[prestige(...)] attributes from fields so downstream derives don't choke.
    for field in fields.named.iter_mut() {
        field.attrs.retain(|attr| !attr.path().is_ident("prestige"));
    }

    let expanded = quote! {
        // Emit the (modified) struct — serde_bytes injected, prestige attrs stripped.
        #item

        // ArrowGroup implementation
        #arrow_group

        // ArrowSchema trait implementation
        impl ::prestige::ArrowSchema for #name {
            fn arrow_schema() -> arrow::datatypes::SchemaRef {
                std::sync::Arc::new(Self::arrow_schema())
            }
        }

        // Identifier field names
        impl #name {
            pub fn identifier_field_names() -> &'static [&'static str] {
                &[#(#identifier_names),*]
            }
        }

        // Sort field and partition field definitions
        #sort_and_partition

        // IcebergSchema trait implementation
        #iceberg_schema_impl

        // ArrowReader implementation
        #arrow_reader

        // ArrowWriter implementation
        #arrow_writer
    };

    TokenStream::from(expanded)
}
