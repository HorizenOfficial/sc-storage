use rocksdb::{TransactionDB, Options, Error};
use crate::common::storage::ColumnFamiliesManager;
use crate::storage::transaction::Transaction;
use rocksdb::transactions::ops::{TransactionBegin, OpenCF};
use std::path::Path;
use crate::common::{InternalReader, Reader, InternalRef};
use crate::TransactionInternal;

pub mod transaction;

pub struct Storage{
    db: TransactionDB,
    path: String,
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

    pub fn open(path: &str, create_if_missing: bool) -> Result<Self, Error> {

        let path_string = path.to_owned();

        let db_exists = Path::new(path_string.as_str()).exists();
        if !db_exists && !create_if_missing {
            return Err(Error::new("No need to create a DB".into()));
        }

        let mut opts = Options::default();
        opts.create_if_missing(create_if_missing);

        Ok(
            Storage{
                db: if db_exists {
                    TransactionDB::open_cf_all(&opts, path)?
                } else {
                    TransactionDB::open_cf_default(&opts, path)?
                },
                path: path_string
            }
        )
    }

    pub fn create_transaction(&self) -> Result<Transaction, Error> {
        Ok(Transaction::new(self.db.transaction_default()?))
    }
}


#[cfg(test)]
mod test {
    use crate::storage::Storage;
    use std::path::Path;
    use crate::common::transaction::TransactionBasic;
    use crate::common::storage::ColumnFamiliesManager;
    use crate::common::Reader;

    fn clear_path(path: &str){
        if Path::new(path.to_owned().as_str()).exists(){
            std::fs::remove_dir_all(path).unwrap()
        }
    }

    #[test]
    fn storage_tests(){
        const STORAGE_PATH: &str = "/mnt/ramfs_dir/storage_test";
        clear_path(STORAGE_PATH);

        assert!(Storage::open(STORAGE_PATH, false).is_err());

        // just creating a storage, then reopening it with the further 'Storage::open' call
        drop(Storage::open(STORAGE_PATH, true).unwrap());

        let storage = Storage::open(STORAGE_PATH, false).unwrap();
        let trans = storage.create_transaction().unwrap();

        assert!(trans.is_empty());

        // initializing the transaction with k1, k2, k4, k5 key-values
        trans.update(&vec![
            ("k1".as_ref(), "v1".as_ref()),
            ("k2".as_ref(), "v2".as_ref()),
            ("k4".as_ref(), "v4".as_ref()),
            ("k5".as_ref(), "v5".as_ref())],
                     &vec![]).unwrap();

        assert!(!trans.is_empty());

        assert!(storage.is_empty());
        // committing all updates into the storage
        trans.commit().unwrap();
        assert!(!storage.is_empty());

        drop(trans); // closing the 'trans'

        let trans2 = storage.create_transaction().unwrap();

        assert_eq!(trans2.get(b"k4").unwrap(), b"v4");
        assert_eq!(trans2.get(b"k5").unwrap(), b"v5");

        // inserting k3 and deleting k4 and k5 key-values
        trans2.update(&vec![("k3".as_ref(), "v3".as_ref())],
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

            let all_values = reader.get_all();
            assert_eq!(all_values.len(), 3);
            assert_eq!(all_values[&b"k1".to_vec()], b"v1".to_vec());
            assert_eq!(all_values[&b"k2".to_vec()], b"v2".to_vec());
            assert_eq!(all_values[&b"k3".to_vec()], b"v3".to_vec());
        }

        // testing the Reader interface of the transaction
        test_reader(&trans2);
        trans2.commit().unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }

    #[test]
    fn storage_cf_tests(){
        const STORAGE_PATH: &str = "/mnt/ramfs_dir/storage_cf_test";
        clear_path(STORAGE_PATH);

        let mut storage_ = Storage::open(STORAGE_PATH, true).unwrap();

        assert!(storage_.get_column_family("default").is_some());
        assert!(storage_.set_column_family("cf1").is_ok());
        assert!(storage_.set_column_family("cf2").is_ok());

        drop(storage_); // closing the 'storage_'

        let storage = Storage::open(STORAGE_PATH, false).unwrap();

        assert!(storage.get_column_family("default").is_some());
        let cf1 = storage.get_column_family("cf1").unwrap();
        let cf2 = storage.get_column_family("cf2").unwrap();

        let trans = storage.create_transaction().unwrap();

        assert!(trans.is_empty_cf(cf1).unwrap() && trans.is_empty_cf(cf2).unwrap());

        // initializing the transaction with k_1, k_2, k_4, k_5 key-values both for cf1 and cf2
        trans.update_cf(cf1,
            &vec![
                ("k11".as_ref(), "v11".as_ref()),
                ("k12".as_ref(), "v12".as_ref()),
                ("k14".as_ref(), "v14".as_ref()),
                ("k15".as_ref(), "v15".as_ref())],
            &vec![]).unwrap();

        assert!(!trans.is_empty_cf(cf1).unwrap() && trans.is_empty_cf(cf2).unwrap());

        trans.update_cf(cf2,
            &vec![
                ("k21".as_ref(), "v21".as_ref()),
                ("k22".as_ref(), "v22".as_ref()),
                ("k24".as_ref(), "v24".as_ref()),
                ("k25".as_ref(), "v25".as_ref())],
            &vec![]).unwrap();

        assert!(!trans.is_empty_cf(cf1).unwrap() && !trans.is_empty_cf(cf2).unwrap());

        assert!(storage.is_empty_cf(cf1).unwrap() && storage.is_empty_cf(cf2).unwrap());
        // committing all updates into the storage
        trans.commit().unwrap();
        assert!(!storage.is_empty_cf(cf1).unwrap() && !storage.is_empty_cf(cf2).unwrap());

        drop(trans); // closing the 'trans'

        let trans2 = storage.create_transaction().unwrap();

        assert_eq!(trans2.get_cf(cf1, b"k14").unwrap(), b"v14");
        assert_eq!(trans2.get_cf(cf1, b"k15").unwrap(), b"v15");
        assert_eq!(trans2.get_cf(cf2, b"k24").unwrap(), b"v24");
        assert_eq!(trans2.get_cf(cf2, b"k25").unwrap(), b"v25");

        // inserting k_3 and deleting k_4 and k_5 key-values both for cf1 and cf2
        trans2.update_cf(cf1,
                         &vec![("k13".as_ref(), "v13".as_ref())],
                         &vec!["k14".as_ref(), "k15".as_ref()]).unwrap();
        trans2.update_cf(cf2,
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

            let all_values_cf1 = reader.get_all_cf(cf1).unwrap();
            assert_eq!(all_values_cf1.len(), 3);
            assert_eq!(all_values_cf1[&b"k11".to_vec()], b"v11".to_vec());
            assert_eq!(all_values_cf1[&b"k12".to_vec()], b"v12".to_vec());
            assert_eq!(all_values_cf1[&b"k13".to_vec()], b"v13".to_vec());

            let all_values_cf2 = reader.get_all_cf(cf2).unwrap();
            assert_eq!(all_values_cf2.len(), 3);
            assert_eq!(all_values_cf2[&b"k21".to_vec()], b"v21".to_vec());
            assert_eq!(all_values_cf2[&b"k22".to_vec()], b"v22".to_vec());
            assert_eq!(all_values_cf2[&b"k23".to_vec()], b"v23".to_vec());
        };

        // testing the Reader interface of the transaction
        test_reader(&trans2);
        trans2.commit().unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }

    #[test]
    fn storage_transaction_basic_tests(){
        const STORAGE_PATH: &str = "/mnt/ramfs_dir/storage_transaction_basic_test";
        clear_path(STORAGE_PATH);

        let mut storage = Storage::open(STORAGE_PATH, true).unwrap();

        assert!(storage.set_column_family("cf1").is_ok());
        assert!(storage.set_column_family("cf2").is_ok());

        let (cf1, cf2) = (
            storage.get_column_family("cf1").unwrap(),
            storage.get_column_family("cf2").unwrap()
        );

        let trans = storage.create_transaction().unwrap();

        assert!(trans.is_empty_cf(cf1).unwrap() && trans.is_empty_cf(cf2).unwrap());

        // 1-st update -------------------------------------------------------------------
        trans.update_cf(cf1,
                        &vec![
                            ("k11".as_ref(), "v11".as_ref())],
                        &vec![]).unwrap();
        trans.save().unwrap();
        // 2-nd update -------------------------------------------------------------------
        trans.update_cf(cf2,
                        &vec![
                            ("k21".as_ref(), "v21".as_ref())],
                        &vec![]).unwrap();
        trans.save().unwrap();
        // 3-rd update -------------------------------------------------------------------
        trans.update_cf(cf1,
                        &vec![
                            ("k12".as_ref(), "v12".as_ref())],
                        &vec![]).unwrap();
        trans.update_cf(cf2,
                        &vec![
                            ("k22".as_ref(), "v22".as_ref())],
                        &vec![]).unwrap();
        trans.save().unwrap();
        // 4-th update -------------------------------------------------------------------
        trans.update_cf(cf2,
                        &vec![
                            ("k23".as_ref(), "v23".as_ref())],
                        &vec![]).unwrap();

        // State after all updates
        assert_eq!(trans.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(trans.get_cf(cf1, b"k12").unwrap(), b"v12");
        assert_eq!(trans.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert_eq!(trans.get_cf(cf2, b"k22").unwrap(), b"v22");
        assert_eq!(trans.get_cf(cf2, b"k23").unwrap(), b"v23");

        // Discarding the 4-th update
        trans.rollback_to_savepoint().unwrap();
        assert_eq!(trans.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(trans.get_cf(cf1, b"k12").unwrap(), b"v12");
        assert_eq!(trans.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert_eq!(trans.get_cf(cf2, b"k22").unwrap(), b"v22");
        assert!(trans.get_cf(cf2, b"k23").is_none());

        // Discarding the 3-rd update
        trans.rollback_to_savepoint().unwrap();
        assert_eq!(trans.get_cf(cf1, b"k11").unwrap(), b"v11");
        assert_eq!(trans.get_cf(cf2, b"k21").unwrap(), b"v21");
        assert!(trans.get_cf(cf1, b"k12").is_none());
        assert!(trans.get_cf(cf2, b"k22").is_none());
        assert!(trans.get_cf(cf2, b"k23").is_none());

        // Discarding all the updates since the transaction was created (the 1-st and the 2-nd updates)
        trans.rollback().unwrap();

        // Transaction was created for an empty storage so the initial state (state after full rollback) is also empty
        assert!(trans.is_empty_cf(cf1).unwrap() && trans.is_empty_cf(cf2).unwrap());
    }
}
