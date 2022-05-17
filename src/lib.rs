#![deny(
unused_import_braces,
unused_qualifications,
trivial_numeric_casts
)]
#![deny(
unused_qualifications,
variant_size_differences,
stable_features,
unreachable_pub
)]
#![deny(
non_shorthand_field_patterns,
unused_attributes,
unused_imports,
unused_extern_crates
)]
#![deny(
renamed_and_removed_lints,
stable_features,
unused_allocation,
unused_comparisons,
bare_trait_objects
)]
#![deny(
const_err,
unused_must_use,
unused_mut,
unused_unsafe,
private_in_public
)]

pub use rocksdb::Transaction as TransactionInternal;
#[macro_use]
pub mod common;

pub mod storage;
pub mod storage_versioned;
