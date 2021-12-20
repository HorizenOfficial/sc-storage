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
    // Creates new instance of Transaction (which is a wrapper for TransactionInternal)
    pub fn new(transaction: TransactionInternal) -> Self {
        Transaction{ transaction }
    }

    // Commits all Transaction's updates into the related Storage
    // Returns Result with an error message if some error occurred
    pub fn commit(&self) -> Result<(), Error> {
        self.transaction.commit()
    }
}
