use crate::TransactionInternal;
use rocksdb::{TransactionDB, Error, ColumnFamily};
use crate::common::{InternalReader, Reader, InternalRef};
use crate::common::transaction::TransactionBasic;
use crate::storage_versioned::StorageVersioned;
use rocksdb::transactions::ops::GetColumnFamilies;
use itertools::Either;

pub struct TransactionVersioned<'a> {
    // TransactionInternal started for a DB in the CurrentState or for some version (checkpoint) of a StorageVersioned
    transaction: TransactionInternal,
    // &StorageVersioned - is needed to create a new version of DB in CurrentState after successful commit
    // TransactionDB - is needed to keep opened a DB for a specific version, otherwise the DB will be dropped (i.e. closed) after the end of the constructor's scope
    storage_or_db: Either<&'a StorageVersioned, TransactionDB>
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
    // - CurrentState of StorageVersioned if 'storage' is specified ('storage' contains a reference to an instance of StorageVersioned for which the TransactionInternal is created);
    // - some previous version of a StorageVersioned if 'db' is specified ('db' contains an opened TransactionDB for some version of a StorageVersioned and a TransactionInternal is created for this DB);
    // NOTE: 'storage' and 'db' can't be both specified as well as being both not specified so the 'Either' type is used
    // Returns a new instance of TransactionVersioned
    pub(crate) fn new(transaction: TransactionInternal, storage_or_db: Either<&'a StorageVersioned, TransactionDB>) -> Self {
        Self{transaction, storage_or_db }
    }

    // Commits all TransactionVersioned's updates into the related StorageVersioned and creates a new version (checkpoint) of a StorageVersioned with the 'version_id' identifier
    // TransactionVersioned started for a previous version of a StorageVersioned can't be committed due to all saved storage versions should remain unchanged
    // Returns Result with an error message if some error occurred
    pub fn commit(&self, version_id: &str) -> Result<(), Error> {
        match self.storage_or_db.as_ref() {
            Either::Left(&storage) => {
                self.transaction.commit()?;
                storage.create_version(version_id)
            } // commit only if a transaction is created for the CurrentState
            Either::Right(_) => {
                Err(Error::new("Transaction for a previous version of a StorageVersioned can't be committed".into()))
            }
        }
    }

    // Method for retrieving column families handles when transaction is started for a version of storage;
    // If transaction is started for the CurrentState of storage, then returns Err and the corresponding method of a StorageVersioned should be used instead.
    // If transaction is started for some version of StorageVersioned then returns Ok with:
    //  - handle for a specified by 'cf_name' column family name;
    //  - 'None' if column family with a specified name is absent in the opened version of storage.
    pub fn get_column_family(&self, cf_name: &str) -> Result<Option<&ColumnFamily>, Error> {
        match self.storage_or_db.as_ref() {
            Either::Right(db) => {
                Ok(db.cf_handle(cf_name))
            }
            Either::Left(_) => {
                Err(Error::new("Current transaction is not for a storage's version".into()))
            }
        }
    }
}
