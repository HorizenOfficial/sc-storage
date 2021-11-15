use rocksdb::{ColumnFamily, Error};
use rocksdb::transactions::ops::{PutCF, DeleteCF, Put, Delete};
use crate::common::{Reader, InternalRef};

pub trait TransactionBasic: Reader + InternalRef {

    const NO_REF: &'static str = "No reference for transaction";
    const NO_REF_MUT: &'static str = "No mutable reference for transaction";

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

    fn save(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        Ok(transaction.set_savepoint())
    }

    fn rollback_to_savepoint(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        transaction.rollback_to_savepoint()
    }

    fn rollback(&self) -> Result<(), Error>{
        let transaction = self.transaction_ref().ok_or(Error::new(Self::NO_REF.into()))?;
        transaction.rollback()
    }
}