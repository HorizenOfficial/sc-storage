use crate::TransactionInternal;
use rocksdb::{TransactionDB, Error};
use crate::common::{InternalReader, Reader, InternalRef};
use crate::common::transaction::TransactionBasic;
use crate::storage_versioned::StorageVersioned;

pub struct TransactionVersioned<'a> {
    transaction: TransactionInternal,
    storage_opt: Option<&'a StorageVersioned>, // needed to create new version of DB after successful commit
    db_opt: Option<TransactionDB> // needed to keep opened a DB for a specific version, otherwise the DB will be dropped (i.e. closed) after the end of the constructor's scope
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
    // TransactionInternal is always valid if it has been successfully created, thus the returned type is not a Result<Self, Error>
    pub(crate) fn new(transaction: TransactionInternal, storage_opt: Option<&'a StorageVersioned>, db_opt: Option<TransactionDB>) -> Result<Self, Error> {
        if storage_opt.is_some() && db_opt.is_some() {
            Err(Error::new("Transaction can't contain both parent_storage and db references".into()))
        } else {
            Ok(Self{ transaction, storage_opt, db_opt })
        }
    }

    pub fn commit(&self, version_id: &str) -> Result<(), Error> {
        if self.db_opt.is_none(){ // commit only if the transaction is for the CurrentState
            if let Some(storage) = self.storage_opt {
                self.transaction.commit()?;
                storage.create_version(version_id)
            } else {
                Err(Error::new("Storage reference is missing".into()))
            }
        } else {
            Err(Error::new("Transaction for a previous version of the DB can't be committed".into()))
        }
    }
}