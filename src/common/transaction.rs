use rocksdb::{ColumnFamily, Error};
use rocksdb::transactions::ops::{PutCF, DeleteCF, Put, Delete};
use crate::common::{Reader, InternalRef};

pub trait TransactionBasic: Reader + InternalRef {

    const NO_REF: &'static str = "No reference for transaction";
    const NO_REF_MUT: &'static str = "No mutable reference for transaction";

    // Performs the specified insertions ('to_update' vector of KVs) and removals ('to_delete' vector of Keys) for the 'default' column family in a current transaction
    // Returns Result with error message if any error occurred
    fn update(&self,
              to_update: &Vec<(&[u8], &[u8])>,
              to_delete: &Vec<&[u8]>) -> Result<(), Error> {
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        for &kv in to_update {
            transaction.put(kv.0, kv.1)?
        }
        for &k in to_delete {
            transaction.delete(k)?
        }
        Ok(())
    }

    // Performs the specified insertions ('to_update' vector of KVs) and removals ('to_delete' vector of Keys) for a specified column family 'cf' in a current transaction
    // Returns Result with error message if any error occurred
    fn update_cf(&self,
              cf: &ColumnFamily,
              to_update: &Vec<(&[u8], &[u8])>,
              to_delete: &Vec<&[u8]>) -> Result<(), Error> {
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        for &kv in to_update {
            transaction.put_cf(cf, kv.0, kv.1)?
        }
        for &k in to_delete {
            transaction.delete_cf(cf, k)?
        }
        Ok(())
    }

    // Saves the current state of a transaction to which it can be rolled back later
    fn save(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        Ok(transaction.set_savepoint())
    }

    // Rolls back the current state of a transaction to the most recent savepoint.
    // Can be performed sequentially thus restoring previous savepoints in LIFO order.
    fn rollback_to_savepoint(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        transaction.rollback_to_savepoint()
    }

    // Rolls back transaction to the initial state (state at the moment when transaction was started)
    fn rollback(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        transaction.rollback()
    }
}