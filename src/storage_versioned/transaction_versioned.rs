use crate::TransactionInternal;
use rocksdb::{TransactionDB, Error, ColumnFamily};
use crate::common::{InternalReader, Reader, InternalRef};
use crate::common::transaction::TransactionBasic;
use crate::storage_versioned::StorageVersioned;
use rocksdb::transactions::ops::GetColumnFamilies;

pub struct TransactionVersioned<'a> {
    transaction: TransactionInternal,           // TransactionInternal started for DB in CurrentState or for some version (checkpoint) of a StorageVersioned
    storage_opt: Option<&'a StorageVersioned>,  // needed to create new version of DB in CurrentState after successful commit
    db_opt:      Option<TransactionDB>          // needed to keep opened a DB for a specific version, otherwise the DB will be dropped (i.e. closed) after the end of the constructor's scope
}

impl InternalRef for TransactionVersioned<'_> {
    fn db_ref(&self) -> Option<&TransactionDB> { None }
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB> { None }

    fn transaction_ref(&self) -> Option<&TransactionInternal> { Some(&self.transaction) }
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal> { Some(&mut self.transaction)  }
}

impl InternalReader for TransactionVersioned<'_> {}
impl Reader for TransactionVersioned<'_> {}
impl TransactionBasic for TransactionVersioned<'_> {}

impl<'a> TransactionVersioned<'a> {
    // Creates new instance of TransactionVersioned (which is a wrapper for TransactionInternal) for:
    // - CurrentState of StorageVersioned if 'storage_opt is 'Some' and 'db_opt' is 'None' ('storage_opt' contains a reference to an instance of StorageVersioned for which the TransactionInternal is created);
    // - some previous version of a StorageVersioned if 'storage_opt' is 'None' and 'db_opt' is 'Some' ('db_opt' contains an opened TransactionDB for some version of a StorageVersioned and a TransactionInternal is created for this DB);
    // NOTE: storage_opt and db_opt can't be both specified as well as being both not specified
    // Returns Result with a new instance of TransactionVersioned or with an error message if some error occurred
    pub(crate) fn new(transaction: TransactionInternal, storage_opt: Option<&'a StorageVersioned>, db_opt: Option<TransactionDB>) -> Result<Self, Error> {
        if storage_opt.is_some() && db_opt.is_some() {
            Err(Error::new("TransactionVersioned can't contain both storage and db".into()))
        } else if storage_opt.is_none() && db_opt.is_none() {
            Err(Error::new("TransactionVersioned can't be created without both storage and db".into()))
        } else {
            Ok(Self{transaction, storage_opt, db_opt})
        }
    }

    // Commits all TransactionVersioned's updates into the related StorageVersioned and creates a new version (checkpoint) of a StorageVersioned with the 'version_id' identifier
    // TransactionVersioned started for a previous version of a StorageVersioned can't be committed due to all saved storage versions should remain unchanged
    // Returns Result with an error message if some error occurred
    pub fn commit(&self, version_id: &str) -> Result<(), Error> {
        if self.db_opt.is_none(){ // commit only if the transaction is for the CurrentState
            if let Some(storage) = self.storage_opt {
                self.transaction.commit()?;
                storage.create_version(version_id)
            } else {
                Err(Error::new("Storage reference is missing".into()))
            }
        } else {
            Err(Error::new("Transaction for a previous version of a StorageVersioned can't be committed".into()))
        }
    }

    // Method for retrieving column families handles when transaction is started for a version of storage;
    // If transaction is started for the CurrentState of storage, then returns Err and the corresponding method of a StorageVersioned should be used instead.
    // If transaction is started for some version of StorageVersioned then returns Ok with:
    //  - handle for a specified by 'cf_name' column family name;
    //  - 'None' if column family with a specified name is absent in the opened version of storage.
    pub fn get_column_family(&self, cf_name: &str) -> Result<Option<&ColumnFamily>, Error> {
        if let Some(db) = self.db_opt.as_ref(){
            Ok(db.cf_handle(cf_name))
        } else {
            Err(Error::new("Current transaction is not for a storage version".into()))
        }
    }
}