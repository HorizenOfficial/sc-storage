use rocksdb::{TransactionDB, Options, Error};
use crate::common::storage::ColumnFamiliesManager;
use crate::storage::transaction::Transaction;
use rocksdb::transactions::ops::{TransactionBegin, OpenCF};
use std::path::Path;
use crate::common::{InternalReader, Reader, InternalRef, join_path_strings};
use crate::TransactionInternal;

pub mod transaction;
pub mod jni;

pub struct Storage{
    db: TransactionDB
}

impl InternalRef for Storage {
    fn db_ref(&self) -> Option<&TransactionDB> { Some(&self.db) }
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB> { Some(&mut self.db) }

    fn transaction_ref(&self) -> Option<&TransactionInternal> { None }
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal> { None }
}

impl InternalReader for Storage {}
impl Reader for Storage {}
impl ColumnFamiliesManager for Storage {}

impl Storage {
    // Directory for storing a current state of a storage (DB)
    const DB_DIR: &'static str = "CurrentState";

    // Opens a storage located by a specified path or creates a new one if the directory by a specified path doesn't exist and 'create_if_missing' is true
    // Returns Result with Storage instance or Err with a describing message if some error occurred
    pub fn open(path: &str, create_if_missing: bool) -> Result<Self, Error> {
        // The nested subdirectory 'DB_DIR' is needed for ability to detect if storage is not existing even if a specified by the 'path' directory exists
        let db_path = join_path_strings(path.to_owned().as_str(), Self::DB_DIR)?;

        let db_path_exists = Path::new(db_path.as_str()).exists();
        if !db_path_exists{
            if !create_if_missing {
                return Err(Error::new("No need to create a DB".into()));
            } else {
                if std::fs::create_dir_all(&db_path).is_err(){
                    return Err(Error::new("DB directory can't be created".into()))
                }
            }
        }

        let mut opts = Options::default();
        opts.create_if_missing(create_if_missing);

        Ok(
            Storage{
                db: if db_path_exists {
                    TransactionDB::open_cf_all(&opts, db_path)?
                } else {
                    TransactionDB::open_cf_default(&opts, db_path)?
                }
            }
        )
    }

    // Creates and returns a Transaction
    // Returns Err with describing message if some error occurred
    pub fn create_transaction(&self) -> Result<Transaction, Error> {
        Ok(Transaction::new(self.db.transaction_default()?))
    }
}


#[cfg(test)]
mod test {
    use crate::storage::Storage;
    use crate::common::transaction::TransactionBasic;
    use crate::common::storage::ColumnFamiliesManager;
    use crate::common::{Reader, test_dir, get_all_cf, get_all};

    #[test]
    fn storage_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_tests").unwrap();

        assert!(Storage::open(storage_path.as_str(), false).is_err());

        // just creating a storage, then reopening it with the further 'Storage::open' call
        drop(Storage::open(storage_path.as_str(), true).unwrap());

        let storage = Storage::open(storage_path.as_str(), false).unwrap();
        let tx = storage.create_transaction().unwrap();

        assert!(tx.is_empty());

        // initializing the transaction with k1, k2, k4, k5 key-values
        tx.update(&vec![
            ("k1".as_ref(), "v1".as_ref()),
            ("k2".as_ref(), "v2".as_ref()),
            ("k4".as_ref(), "v4".as_ref()),
            ("k5".as_ref(), "v5".as_ref())],
                  &vec![]).unwrap();

        assert!(!tx.is_empty());

        assert!(storage.is_empty());
        // committing all updates into the storage
        tx.commit().unwrap();
        assert!(!storage.is_empty());

        drop(tx); // closing the 'tx'

        let tx2 = storage.create_transaction().unwrap();

        assert_eq!(tx2.get(b"k4").unwrap(), b"v4");
        assert_eq!(tx2.get(b"k5").unwrap(), b"v5");

        // inserting k3 and deleting k4 and k5 key-values
        tx2.update(&vec![("k3".as_ref(), "v3".as_ref())],
                   &vec!["k4".as_ref(), "k5".as_ref()]).unwrap();

        // Test for the Reader interface
        fn test_reader(reader: &dyn Reader) {
            assert_eq!(reader.get(b"k1").unwrap(), b"v1");
            assert_eq!(reader.get(b"k2").unwrap(), b"v2");
            assert_eq!(reader.get(b"k3").unwrap(), b"v3");
            assert!(reader.get(b"k4").is_none());
            assert!(reader.get(b"k5").is_none());

            assert!(reader.multi_get(&[]).is_empty());
            let values = reader.multi_get(&[b"k1", b"k2", b"k3", b"k4", b"k5", b"k1", b"k2"]);

            assert_eq!(values.len(), 5); // duplicated keys in parameters list of 'multi_get' are counted just once
            assert_eq!(values[&b"k1".to_vec()].as_ref().unwrap(), &b"v1".to_vec());
            assert_eq!(values[&b"k2".to_vec()].as_ref().unwrap(), &b"v2".to_vec());
            assert_eq!(values[&b"k3".to_vec()].as_ref().unwrap(), &b"v3".to_vec());
            assert!(values[&b"k4".to_vec()].is_none());
            assert!(values[&b"k5".to_vec()].is_none());

            let all_values = get_all(reader);
            assert_eq!(all_values.len(), 3);
            assert_eq!(all_values[&b"k1".to_vec()], b"v1".to_vec());
            assert_eq!(all_values[&b"k2".to_vec()], b"v2".to_vec());
            assert_eq!(all_values[&b"k3".to_vec()], b"v3".to_vec());
        }

        // testing the Reader interface of the transaction
        test_reader(&tx2);
        tx2.commit().unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }

    #[test]
    fn storage_cf_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_cf_tests").unwrap();

        let mut storage_ = Storage::open(storage_path.as_str(), true).unwrap();

        assert!(storage_.get_column_family("default").is_some());
        assert!(storage_.set_column_family("cf1").is_ok());
        assert!(storage_.set_column_family("cf2").is_ok());

        drop(storage_); // closing the 'storage_'

        let storage = Storage::open(storage_path.as_str(), false).unwrap();

        assert!(storage.get_column_family("default").is_some());
        let cf1 = storage.get_column_family("cf1").unwrap();
        let cf2 = storage.get_column_family("cf2").unwrap();

        let tx = storage.create_transaction().unwrap();

        assert!(tx.is_empty_cf(cf1).unwrap() && tx.is_empty_cf(cf2).unwrap());

        // initializing the transaction with k_1, k_2, k_4, k_5 key-values both for cf1 and cf2
        tx.update_cf(cf1,
                     &vec![
                ("k11".as_ref(), "v11".as_ref()),
                ("k12".as_ref(), "v12".as_ref()),
                ("k14".as_ref(), "v14".as_ref()),
                ("k15".as_ref(), "v15".as_ref())],
                     &vec![]).unwrap();

        assert!(!tx.is_empty_cf(cf1).unwrap() && tx.is_empty_cf(cf2).unwrap());

        tx.update_cf(cf2,
                     &vec![
                ("k21".as_ref(), "v21".as_ref()),
                ("k22".as_ref(), "v22".as_ref()),
                ("k24".as_ref(), "v24".as_ref()),
                ("k25".as_ref(), "v25".as_ref())],
                     &vec![]).unwrap();

        assert!(!tx.is_empty_cf(cf1).unwrap() && !tx.is_empty_cf(cf2).unwrap());

        assert!(storage.is_empty_cf(cf1).unwrap() && storage.is_empty_cf(cf2).unwrap());
        // committing all updates into the storage
        tx.commit().unwrap();
        assert!(!storage.is_empty_cf(cf1).unwrap() && !storage.is_empty_cf(cf2).unwrap());

        drop(tx); // closing the 'tx'

        let tx2 = storage.create_transaction().unwrap();

        assert_eq!(tx2.get_cf(cf1, b"k14").unwrap(), b"v14");
        assert_eq!(tx2.get_cf(cf1, b"k15").unwrap(), b"v15");
        assert_eq!(tx2.get_cf(cf2, b"k24").unwrap(), b"v24");
        assert_eq!(tx2.get_cf(cf2, b"k25").unwrap(), b"v25");

        // inserting k_3 and deleting k_4 and k_5 key-values both for cf1 and cf2
        tx2.update_cf(cf1,
                      &vec![("k13".as_ref(), "v13".as_ref())],
                      &vec!["k14".as_ref(), "k15".as_ref()]).unwrap();
        tx2.update_cf(cf2,
                      &vec![("k23".as_ref(), "v23".as_ref())],
                      &vec!["k24".as_ref(), "k25".as_ref()]).unwrap();

        // Test for the Reader interface
        let test_reader = |reader: &dyn Reader|{
            assert_eq!(reader.get_cf(cf1, b"k11").unwrap(), b"v11");
            assert_eq!(reader.get_cf(cf1, b"k12").unwrap(), b"v12");
            assert_eq!(reader.get_cf(cf1, b"k13").unwrap(), b"v13");
            assert!(reader.get_cf(cf1, b"k14").is_none());
            assert!(reader.get_cf(cf1, b"k15").is_none());

            assert_eq!(reader.get_cf(cf2, b"k21").unwrap(), b"v21");
            assert_eq!(reader.get_cf(cf2, b"k22").unwrap(), b"v22");
            assert_eq!(reader.get_cf(cf2, b"k23").unwrap(), b"v23");
            assert!(reader.get_cf(cf2, b"k24").is_none());
            assert!(reader.get_cf(cf2, b"k25").is_none());

            assert!(reader.multi_get_cf(cf1, &[]).is_empty());
            assert!(reader.multi_get_cf(cf2, &[]).is_empty());

            let values_cf1 = reader.multi_get_cf(cf1, &[b"k11", b"k12", b"k13", b"k14",  b"k15", b"k11", b"k12"]);
            assert_eq!(values_cf1.len(), 5); // duplicated keys in parameters list of 'multi_get' are counted just once
            assert_eq!(values_cf1[&b"k11".to_vec()].as_ref().unwrap(), &b"v11".to_vec());
            assert_eq!(values_cf1[&b"k12".to_vec()].as_ref().unwrap(), &b"v12".to_vec());
            assert_eq!(values_cf1[&b"k13".to_vec()].as_ref().unwrap(), &b"v13".to_vec());
            assert!(values_cf1[&b"k14".to_vec()].is_none());
            assert!(values_cf1[&b"k15".to_vec()].is_none());

            let values_cf2 = reader.multi_get_cf(cf2, &[b"k21", b"k22", b"k23", b"k24",  b"k25", b"k21", b"k22"]);
            assert_eq!(values_cf2.len(), 5); // duplicated keys in parameters list of 'multi_get' are counted just once
            assert_eq!(values_cf2[&b"k21".to_vec()].as_ref().unwrap(), &b"v21".to_vec());
            assert_eq!(values_cf2[&b"k22".to_vec()].as_ref().unwrap(), &b"v22".to_vec());
            assert_eq!(values_cf2[&b"k23".to_vec()].as_ref().unwrap(), &b"v23".to_vec());
            assert!(values_cf2[&b"k24".to_vec()].is_none());
            assert!(values_cf2[&b"k25".to_vec()].is_none());

            let all_values_cf1 = get_all_cf(reader, cf1).unwrap();
            assert_eq!(all_values_cf1.len(), 3);
            assert_eq!(all_values_cf1[&b"k11".to_vec()], b"v11".to_vec());
            assert_eq!(all_values_cf1[&b"k12".to_vec()], b"v12".to_vec());
            assert_eq!(all_values_cf1[&b"k13".to_vec()], b"v13".to_vec());

            let all_values_cf2 = get_all_cf(reader, cf2).unwrap();
            assert_eq!(all_values_cf2.len(), 3);
            assert_eq!(all_values_cf2[&b"k21".to_vec()], b"v21".to_vec());
            assert_eq!(all_values_cf2[&b"k22".to_vec()], b"v22".to_vec());
            assert_eq!(all_values_cf2[&b"k23".to_vec()], b"v23".to_vec());
        };

        // testing the Reader interface of the transaction
        test_reader(&tx2);
        tx2.commit().unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }

    #[test]
    fn storage_transaction_basic_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_transaction_basic_tests").unwrap();

        let mut storage = Storage::open(storage_path.as_str(), true).unwrap();

        assert!(storage.set_column_family("cf1").is_ok());
        assert!(storage.set_column_family("cf2").is_ok());

        let (cf1, cf2) = (
            storage.get_column_family("cf1").unwrap(),
            storage.get_column_family("cf2").unwrap()
        );

        let tx = storage.create_transaction().unwrap();

        assert!(tx.is_empty_cf(cf1).unwrap() && tx.is_empty_cf(cf2).unwrap());

        // There are no savepoints so there is nowhere to rollback
        assert!(tx.rollback_to_savepoint().is_err());

        // Transaction can be 'rolled back' to the initial state while being in such a state
        assert!(tx.rollback().is_ok());

        // 1-st update -------------------------------------------------------------------
        tx.update_cf(cf1,
                     &vec![
                            ("k11".as_ref(), "v11".as_ref())],
                     &vec![]).unwrap();
        tx.save().unwrap();
        // 2-nd update -------------------------------------------------------------------
        tx.update_cf(cf2,
                     &vec![
                            ("k21".as_ref(), "v21".as_ref())],
                     &vec![]).unwrap();
        tx.save().unwrap();
        // 3-rd update -------------------------------------------------------------------
        tx.update_cf(cf1,
                     &vec![
                            ("k12".as_ref(), "v12".as_ref())],
                     &vec![]).unwrap();
        tx.update_cf(cf2,
                     &vec![
                            ("k22".as_ref(), "v22".as_ref())],
                     &vec![]).unwrap();
        tx.save().unwrap();
        // 4-th update -------------------------------------------------------------------
        tx.update_cf(cf2,
                     &vec![
                            ("k23".as_ref(), "v23".as_ref())],
                     &vec![]).unwrap();

        // State after all updates
        assert_eq!(tx.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(tx.get_cf(cf1, b"k12").unwrap(), b"v12");
        assert_eq!(tx.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert_eq!(tx.get_cf(cf2, b"k22").unwrap(), b"v22");
        assert_eq!(tx.get_cf(cf2, b"k23").unwrap(), b"v23");

        // Discarding the 4-th update
        tx.rollback_to_savepoint().unwrap();
        assert_eq!(tx.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(tx.get_cf(cf1, b"k12").unwrap(), b"v12");
        assert_eq!(tx.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert_eq!(tx.get_cf(cf2, b"k22").unwrap(), b"v22");
        assert!(tx.get_cf(cf2, b"k23").is_none());

        // Discarding the 3-rd update
        tx.rollback_to_savepoint().unwrap();
        assert_eq!(tx.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(tx.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert!(tx.get_cf(cf1, b"k12").is_none());
        assert!(tx.get_cf(cf2, b"k22").is_none());
        assert!(tx.get_cf(cf2, b"k23").is_none());

        // Discarding all the updates since the transaction was created (the 1-st and the 2-nd updates)
        tx.rollback().unwrap();

        // Transaction was created for an empty storage so the initial state (state after full rollback) is also empty
        assert!(tx.is_empty_cf(cf1).unwrap() && tx.is_empty_cf(cf2).unwrap());

        // There are no savepoints after a full rollback
        assert!(tx.rollback_to_savepoint().is_err());

        // Updating the transaction from initial state
        tx.update_cf(cf1,
                     &vec![
                         ("k11".as_ref(), "v11".as_ref())],
                     &vec![]).unwrap();
        tx.save().unwrap();

        // Committing the updates
        tx.commit().unwrap();

        // There are no savepoints after a transaction is committed
        assert!(tx.rollback_to_savepoint().is_err());
        // Transaction can't be rolled back after it was committed
        assert!(tx.rollback().is_err());
    }
}
