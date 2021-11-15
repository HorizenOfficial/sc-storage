use rocksdb::{ColumnFamily, DBVector, Error, IteratorMode, DBIterator, TransactionDB};
use itertools::Itertools;
use crate::TransactionInternal;
use rocksdb::transactions::ops::{Get, GetCF, Iterate, IterateCF};
use std::collections::HashMap;

pub mod storage;
pub mod transaction;

pub trait InternalRef {
    fn db_ref(&self) -> Option<&TransactionDB>;
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB>;

    fn transaction_ref(&self) -> Option<&TransactionInternal>;
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal>;
}

pub trait InternalReader: InternalRef {

    // wrapper over internal 'get', 'get_cf', 'iterator', 'iterator_cf' of Database (or TransactionInternal for TransactionInternalReader)
    // should be implemented by Storage and Transaction to have the same StorageReader functionality

    fn get_internal(&self, key: &[u8]) -> Option<DBVector> {
        if let Some(db) = self.db_ref() {
            db.get(key).ok()?
        } else if let Some(transaction) = self.transaction_ref(){
            transaction.get(key).ok()?
        } else {
            panic!("Unknown type of reference")
        }
    }
    fn get_cf_internal(&self, cf: &ColumnFamily, key: &[u8]) -> Option<DBVector> {
        if let Some(db) = self.db_ref() {
            db.get_cf(cf, key).ok()?
        } else if let Some(transaction) = self.transaction_ref(){
            transaction.get_cf(cf, key).ok()?
        } else {
            panic!("Unknown type of reference")
        }
    }
    fn iterator_internal(&self, mode: IteratorMode) -> DBIterator {
        if let Some(db) = self.db_ref() {
            db.iterator(mode)
        } else if let Some(transaction) = self.transaction_ref(){
            transaction.iterator(mode)
        } else {
            panic!("Unknown type of reference")
        }
    }
    fn iterator_cf_internal(&self, cf: &ColumnFamily, mode: IteratorMode) -> Result<DBIterator, Error> {
        if let Some(db) = self.db_ref(){
            db.iterator_cf(cf, mode)
        } else if let Some(transaction) = self.transaction_ref(){
            transaction.iterator_cf(cf, mode)
        } else {
            panic!("Unknown type of reference")
        }
    }
}

pub trait Reader: InternalReader {

    fn get(&self, key: &[u8]) -> Option<Vec<u8>>{
        Some(self.get_internal(key)?.to_vec())
    }
    fn get_cf(&self, cf: &ColumnFamily, key: &[u8]) -> Option<Vec<u8>>{
        Some(self.get_cf_internal(cf, key)?.to_vec())
    }

    fn multi_get(&self, keys: &[&[u8]]) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
        keys.iter()
            .unique()
            .map(|&key| (key.to_vec(), self.get(key)))
            .collect()
    }
    fn multi_get_cf(&self, cf: &ColumnFamily, keys: &[&[u8]]) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
        keys.iter()
            .unique()
            .map(|&key| (key.to_vec(), self.get_cf(cf, key)))
            .collect()
    }

    fn get_all(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.iterator_internal(IteratorMode::Start)
            .map(|kv| (kv.0.to_vec(), kv.1.to_vec()))
            .collect()
    }
    // Result is returned due to a specified CF can be absent, so the error should be returned
    fn get_all_cf(&self, cf: &ColumnFamily) -> Result<HashMap<Vec<u8>, Vec<u8>>, Error> {
        Ok(
            self.iterator_cf_internal(cf,IteratorMode::Start)?
                .map(|kv| (kv.0.to_vec(), kv.1.to_vec()))
                .collect()
        )
    }

    fn is_empty(&self) -> bool {
        self.iterator_internal(IteratorMode::Start).next().is_none()
    }
    fn is_empty_cf(&self, cf: &ColumnFamily) -> Result<bool, Error> {
        Ok(self.iterator_cf_internal(cf,IteratorMode::Start)?.next().is_none())
    }
}