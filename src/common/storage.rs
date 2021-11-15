use rocksdb::{ColumnFamily, Options, Error};
use rocksdb::transactions::ops::{GetColumnFamilies, CreateCf};
use crate::common::InternalRef;

pub trait ColumnFamiliesManager: InternalRef {

    fn get_column_family(&self, cf_name: &str) -> Option<&ColumnFamily> {
        self.db_ref()?
            .cf_handle(cf_name)
    }

    fn set_column_family(&mut self, cf_name: &str) -> Result<(), Error>{
        Ok(
            if self.get_column_family(cf_name).is_none(){
                self.db_ref_mut().ok_or(Error::new("No mutable reference for db".into()))?
                    .create_cf(cf_name, &Options::default())?
            }
        )
    }

    // TODO: DropCF implemented for TransactionDB is needed
    // fn delete_column_family(&self, cf_name: &str) -> bool {
    //     self.get_db_mut()
    // }
}
