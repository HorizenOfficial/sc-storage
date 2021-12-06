use crate::common::transaction::TransactionBasic;
use crate::TransactionInternal;
use rocksdb::{Error, TransactionDB};
use crate::common::{InternalReader, Reader, InternalRef};

pub struct Transaction {
    transaction: TransactionInternal,
}

impl InternalRef for Transaction {
    fn db_ref(&self) -> Option<&TransactionDB> { None }
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB> { None }

    fn transaction_ref(&self) -> Option<&TransactionInternal> { Some(&self.transaction) }
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal> { Some(&mut self.transaction)  }
}

impl InternalReader for Transaction {}
impl Reader for Transaction {}
impl TransactionBasic for Transaction {}

impl Transaction {
    // TransactionInternal is always valid if it has been successfully created, thus the returned type is not a Result<Self, Error>
    pub fn new(transaction: TransactionInternal) -> Self {
        Transaction{ transaction }
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.transaction.commit()
    }
}
