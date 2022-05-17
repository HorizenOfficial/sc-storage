use rocksdb::{ColumnFamily, DBVector, Error, IteratorMode, DBIterator, TransactionDB};
use itertools::Itertools;
use crate::TransactionInternal;
use rocksdb::transactions::ops::{Get, GetCF, Iterate, IterateCF};
use std::collections::HashMap;
use std::path::Path;

pub mod storage;
pub mod transaction;
#[macro_use]
pub mod jni;

pub trait InternalRef {

    // Methods for accessing actual data storage (DB or transaction)
    // These methods are used by the InternalReader trait
    // The trait should be implemented by Storage/StorageVersioned and Transaction/TransactionVersioned to have the same Reader trait

    fn db_ref(&self) -> Option<&TransactionDB>;
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB>;

    fn transaction_ref(&self) -> Option<&TransactionInternal>;
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal>;
}

pub trait InternalReader: InternalRef {

    // Wrappers for internal 'get', 'get_cf', 'iterator', 'iterator_cf' methods of TransactionDB and TransactionInternal
    // These wrappers are used by Reader trait to abstract from concrete object (TransactionDB or TransactionInternal)

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

    // Common trait for accessing data of Storage/StorageVersioned or Transaction/TransactionVersioned

    // Retrieves value for a specified key in the 'default' column family from an underlying storage or returns None in case the key is absent
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>{
        Some(self.get_internal(key)?.to_vec())
    }

    // Retrieves value for a specified key in a specified column family from an underlying storage or returns None in case the key is absent
    fn get_cf(&self, cf: &ColumnFamily, key: &[u8]) -> Option<Vec<u8>> {
        Some(self.get_cf_internal(cf, key)?.to_vec())
    }

    // Gets KV pairs for a specified list of keys in the 'default' column family from an underlying storage;
    // For the absent keys the Values in corresponding KV pairs are None.
    fn multi_get(&self, keys: &[&[u8]]) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
        keys.iter()
            .unique()
            .map(|&key| (key.to_vec(), self.get(key)))
            .collect()
    }

    // Gets KV pairs for a specified list of keys in a specified column family from an underlying storage;
    // For the absent keys the Values in corresponding KV pairs are None.
    fn multi_get_cf(&self, cf: &ColumnFamily, keys: &[&[u8]]) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
        keys.iter()
            .unique()
            .map(|&key| (key.to_vec(), self.get_cf(cf, key)))
            .collect()
    }

    // Returns iterator for all contained keys in the 'default' column family in an underlying storage
    fn get_iter(&self) -> DBIterator{
        self.iterator_internal(IteratorMode::Start)
    }

    // Returns iterator in a specified mode for all contained keys in a specified column family in an underlying storage
    // NOTE: Result is returned due to a specified CF can be absent so an error should be returned in this case
    fn get_iter_cf_mode(&self, cf: &ColumnFamily, mode: IteratorMode) -> Result<DBIterator, Error> {
        self.iterator_cf_internal(cf, mode)
    }

    // Same as get_iter_cf_mode but only for IteratorMode::Start mode
    fn get_iter_cf(&self, cf: &ColumnFamily) -> Result<DBIterator, Error> {
        self.get_iter_cf_mode(cf, IteratorMode::Start)
    }

    // Checks whether an underlying storage contains any KV-pairs in the 'default' column family
    fn is_empty(&self) -> bool {
        self.get_iter().next().is_none()
    }

    // Checks whether an underlying storage contains any KV-pairs in a specified column family
    fn is_empty_cf(&self, cf: &ColumnFamily) -> Result<bool, Error> {
        Ok(self.get_iter_cf(cf)?.next().is_none())
    }
}

// Removes the specified directory by deleting it together with all nested subdirectories
// Returns Ok Result if directory removed successfully or didn't exist or Err with a message if some error occurred
pub fn clear_path(path: &str) -> Result<(), Error> {
    let path_string = path.to_owned();
    if std::path::Path::new(path_string.as_str()).exists(){
        if std::fs::remove_dir_all(path).is_err() {
            return Err(Error::new(path_string + " can't be removed".into()));
        }
    }
    Ok(())
}

// Joins two paths (or a path and a file/directory name) concatenating them with OS-specific delimiter (such as '/' or '\')
pub fn join_path_strings(path1: &str, path2: &str) -> Result<String, Error> {
    if let Ok(path) = Path::new(path1).join(path2)
        .into_os_string().into_string(){
        Ok(path)
    } else {
        return Err(Error::new("Can't get a String for joined path".into()))
    }
}

#[cfg(test)]
// Base directory for tests data storing
// TempDir should be returned to a caller to provide automatic directory removal after the end of a caller's scope
pub fn test_dir(dir_prefix: &str) -> Result<(tempdir::TempDir, String), Error> {
    if let Ok(tmp_dir) = tempdir::TempDir::new(dir_prefix){
        if let Ok(path_string) = tmp_dir.path().to_path_buf().into_os_string().into_string(){
            Ok((tmp_dir, path_string))
        } else {
            Err(Error::new("Can't get a String for a temporary directory path".into()))
        }
    } else {
        Err(Error::new("Can't create a temporary directory".into()))
    }
}

#[cfg(test)]
// Iterates over all contained keys in the 'default' column family in an underlying storage and returns a list of all contained KV pairs
pub fn get_all(reader: &dyn Reader) -> HashMap<Vec<u8>, Vec<u8>> {
    reader.get_iter()
        .map(|kv| (kv.0.to_vec(), kv.1.to_vec()))
        .collect()
}

#[cfg(test)]
// Iterates over all contained keys in a specified column family in an underlying storage and returns a list of all contained KV pairs
// NOTE: Result is returned due to a specified CF can be absent so an error should be returned in this case
pub fn get_all_cf(reader: &dyn Reader, cf: &ColumnFamily) -> Result<HashMap<Vec<u8>, Vec<u8>>, Error> {
    Ok(
        reader.get_iter_cf(cf)?
            .map(|kv| (kv.0.to_vec(), kv.1.to_vec()))
            .collect()
    )
}
