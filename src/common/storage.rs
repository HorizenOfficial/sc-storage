use rocksdb::{ColumnFamily, Options, Error};
use rocksdb::transactions::ops::{GetColumnFamilies, CreateCf};
use crate::common::InternalRef;

pub trait ColumnFamiliesManager: InternalRef {

    // Trait for managing column families of Storage/StorageVersioned

    // Returns a handle for a specified column family name
    // Returns None if CF with a specified name is absent in storage
    fn get_column_family(&self, cf_name: &str) -> Option<&ColumnFamily> {
        self.db_ref()?
            .cf_handle(cf_name)
    }

    // Creates column family with a specified name
    // Reruns Ok if column family was created successfully or already exists
    // Returns Err with describing message if any error occurred during column family creation
    fn set_column_family(&mut self, cf_name: &str) -> Result<(), Error>{
        if self.get_column_family(cf_name).is_none(){
            self.db_ref_mut().ok_or(Error::new("No mutable reference for db".into()))?
                .create_cf(cf_name, &Options::default())
        } else {
            Ok(())
        }
    }

    // TODO: DropCF trait currently is not implemented for TransactionDB
    // fn delete_column_family(&self, cf_name: &str) -> bool;
}
