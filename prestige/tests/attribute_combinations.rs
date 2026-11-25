//! Several items in one `#[prestige(..)]` attribute must all apply, in any order.
#![cfg(feature = "iceberg")]

use prestige::iceberg::{PartitionFieldDef, SortDirection, SortFieldDef, Transform};

#[prestige::prestige_schema]
#[derive(Debug, Clone, PartialEq)]
struct CombinedAttributes {
    #[prestige(sort_key(desc, order = 2), identifier)]
    id: String,
    #[prestige(partition(day), sort_key(order = 1))]
    partition_first: i64,
    #[prestige(sort_key(order = 3), partition(bucket(16)))]
    sort_first: i64,
    #[prestige(as_fixed_binary(4), partition(truncate(2)))]
    fixed: [u8; 4],
}

fn sort_def(
    name: &'static str,
    direction: SortDirection,
    order: u32,
) -> (&'static str, SortDirection, u32) {
    (name, direction, order)
}

#[test]
fn every_sort_key_applies() {
    let defs: Vec<_> = CombinedAttributes::sort_field_definitions()
        .iter()
        .map(
            |SortFieldDef {
                 name,
                 direction,
                 order,
                 ..
             }| (*name, *direction, *order),
        )
        .collect();

    assert_eq!(
        defs,
        [
            sort_def("id", SortDirection::Descending, 2),
            sort_def("partition_first", SortDirection::Ascending, 1),
            sort_def("sort_first", SortDirection::Ascending, 3),
        ]
    );
}

#[test]
fn every_partition_applies() {
    let defs = CombinedAttributes::partition_field_definitions();

    assert_eq!(
        defs,
        [
            PartitionFieldDef {
                name: "partition_first",
                transform: Transform::Day
            },
            PartitionFieldDef {
                name: "sort_first",
                transform: Transform::Bucket(16)
            },
            PartitionFieldDef {
                name: "fixed",
                transform: Transform::Truncate(2)
            },
        ]
    );
}

#[test]
fn identifier_after_another_item_applies() {
    assert_eq!(CombinedAttributes::identifier_field_names(), ["id"]);
}

#[test]
fn fixed_binary_before_another_item_applies() {
    let schema = CombinedAttributes::arrow_schema();

    assert_eq!(
        schema.field_with_name("fixed").unwrap().data_type(),
        &prestige::arrow::datatypes::DataType::FixedSizeBinary(4)
    );
}
