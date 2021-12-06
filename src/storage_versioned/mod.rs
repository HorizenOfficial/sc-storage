use rocksdb::{TransactionDB, Error, Options};
use crate::common::{InternalRef, InternalReader, Reader, clear_path};
use crate::TransactionInternal;
use crate::common::storage::ColumnFamiliesManager;
use std::path::Path;
use rocksdb::transactions::ops::{OpenCF, TransactionBegin, CreateCheckpointObject};
use crate::storage_versioned::transaction_versioned::TransactionVersioned;
use std::collections::HashMap;
use fs_extra::dir::{copy, CopyOptions};
use std::fs::rename;
use std::mem::replace;
use itertools::Itertools;

pub mod transaction_versioned;

const VERSION_DELIMITER: &str = "__";
const VERSIONS_STORED: u32 = 10; // number of the latest versions to be stored

pub struct StorageVersioned {
    db: TransactionDB,
    db_path: String,
    versions_path: String,
    base_path: String
}

impl InternalRef for StorageVersioned {
    fn db_ref(&self) -> Option<&TransactionDB> { Some(&self.db) }
    fn db_ref_mut(&mut self) -> Option<&mut TransactionDB> { Some(&mut self.db) }

    fn transaction_ref(&self) -> Option<&TransactionInternal> { None }
    fn transaction_ref_mut(&mut self) -> Option<&mut TransactionInternal> { None }
}

impl InternalReader for StorageVersioned {}
impl Reader for StorageVersioned {}
impl ColumnFamiliesManager for StorageVersioned {}

impl StorageVersioned {

    const DB_DIR: &'static str = "/CurrentState";
    const VERSIONS_DIR: &'static str = "/Versions";

    pub fn open(path: &str, create_if_missing: bool) -> Result<Self, Error>{
        let db_path = path.to_owned() + Self::DB_DIR;

        // Preparing the CurrentState DB directory if it doesn't exist
        let db_exists = Path::new(db_path.as_str()).exists();
        if !db_exists {
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

        // Opening or creating the CurrentState DB
        let db =
            if db_exists {
                TransactionDB::open_cf_all(&opts, &db_path)?
            } else {
                TransactionDB::open_cf_default(&opts, &db_path)?
            };

        // Checking or creating the Versions directory
        let versions_path = path.to_owned() + Self::VERSIONS_DIR;
        if Path::new(versions_path.as_str()).exists(){
            // Scan all existing versions and check that their numbers are a consecutive set
            Self::get_all_versions_in_dir(versions_path.as_str())?;
        } else {
            // Creating the Versions directory if it doesn't exist
            if std::fs::create_dir(&versions_path).is_err(){
                drop(db);
                return if std::fs::remove_dir(&db_path).is_ok() {
                    Err(Error::new("Versions directory can't be created".into()))
                } else {
                    Err(Error::new("Versions directory can't be created; Can't clean the DB directory".into()))
                }
            }
        }

        Ok(
            Self{
                db,
                db_path: Self::absolute_path(&db_path)?,
                versions_path: Self::absolute_path(&versions_path)?,
                base_path: Self::absolute_path(&path.to_owned())?
            }
        )
    }

    fn absolute_path(path: &String) -> Result<String, Error> {
        if let Ok(path_buf) = Path::new(path.as_str()).canonicalize(){
            if let Some(path_str) = path_buf.to_str() {
                Ok(String::from(path_str))
            } else {
                Err(Error::new("Can't convert the canonicalized path into string".into()))
            }
        } else {
            Err(Error::new("Path can't be canonicalized".into()))
        }
    }

    pub fn create_transaction(&self, version_id_opt: Option<&str>) -> Result<TransactionVersioned, Error> {
        if let Some(version_id) = version_id_opt {
            let db_version = self.open_version(version_id)?;
            TransactionVersioned::new(db_version.transaction_default()?, None, Some(db_version))
        } else {
            TransactionVersioned::new(self.db.transaction_default()?, Some(self), None)
        }
    }

    pub fn rollback(&mut self, version_id: &str) -> Result<(), Error> {
        let all_versions = self.get_all_versions()?;

        if let Some(&version_number) = all_versions.get(version_id) {
            // Copying the specified version into the base directory of the Storage
            if copy(self.compose_version_path(version_id, version_number).as_str(),
                    self.base_path.as_str(),
                    &CopyOptions::new()).is_ok() {

                // Closing the DB in CurrentState directory
                drop(replace(&mut self.db, TransactionDB::dummy_db()));

                // Removing the CurrentState directory
                clear_path(self.db_path.as_str())?;

                // Renaming the copied directory with version into the 'CurrentState'
                let version_copy_path = self.base_path.to_owned() + self.compose_version_dir_name(version_id, version_number).as_str();
                if rename(version_copy_path, self.db_path.as_str()).is_ok(){
                    // Opening the copied DB and putting its handle into self.db
                    self.db = TransactionDB::open_cf_all(&Options::default(), &self.db_path)?;

                    // Removing all versions which follow the restored version
                    for (id, &num) in &all_versions {
                        if num > version_number {
                            clear_path(
                                self.compose_version_path(id, num).as_str()
                            )?;
                        }
                    }
                } else {
                    return Err(Error::new("Can't rename the copied version in the base directory".into()))
                }
            } else {
                return Err(Error::new("Can't copy the specified version into the base directory".into()))
            }
        } else {
            return Err(Error::new("Specified version doesn't exist".into()))
        }
        Ok(())
    }

    // Returns a sorted by creation order list of all existing versions IDs
    pub fn rollback_versions(&self) -> Result<Vec<String>, Error> {
        Ok(
            self.get_all_versions()?.into_iter()
                .sorted_by(|v1, v2| Ord::cmp(&v1.1, &v2.1))
                .map(|(id, _)|id).collect()
        )
    }

    // Returns the most recent version ID
    pub fn last_version(&self) -> Result<Option<String>, Error> {
        Ok(
            if let Some(last_version) = self.rollback_versions()?.last(){
                Some(last_version.to_owned())
            } else {
                None
            }
        )
    }

    // Checks if all elements of the given set are consecutive after being sorted
    fn is_consecutive_set(set: &Vec<&u32>) -> bool {
        let mut set_sorted = set.clone();
        set_sorted.sort();
        let mut prev_elem = 0u32;

        for (pos, &elem) in set_sorted.into_iter().enumerate() {
            if pos != 0 &&
               elem != prev_elem + 1 {
                return false;
            }
            prev_elem = elem;
        }
        true
    }

    fn get_all_versions_in_dir(versions_path: &str) -> Result<HashMap<String, u32>, Error> {
        let paths = std::fs::read_dir(versions_path).unwrap();
        let mut paths_count = 0;

        let num_id_map: HashMap<String, u32> = paths.into_iter()
            .flat_map(|path| { // counting the total number of subdirectories with versions
                paths_count += 1;
                path
            })
            .flat_map(|path| path.file_name().into_string())
            .flat_map(|num_id| {
                let num_id_splitted = num_id.as_str().split(VERSION_DELIMITER).collect::<Vec<&str>>();
                if num_id_splitted.len() != 2 {
                    None
                } else if let Ok(version_number) = num_id_splitted[0].to_owned().parse::<u32>(){
                    Some((num_id_splitted[1].to_owned(), version_number)) // (version_id, version_number)
                } else {
                    None
                }
            })
            .collect();

        if num_id_map.len() == paths_count {
            if Self::is_consecutive_set(&num_id_map.iter().map(|v|v.1).collect()) {
                Ok(num_id_map)
            } else {
                return Err(Error::new("Versions numbers are not consecutive".into()))
            }
        } else {
            return Err(Error::new("Subdirectories of the Versions directory are inconsistent".into()))
        }
    }

    fn get_all_versions(&self) -> Result<HashMap<String, u32>, Error>{
        Self::get_all_versions_in_dir(self.versions_path.as_str())
    }

    fn max_version_number(all_versions_numbers: &[u32]) -> Result<Option<u32>, Error> {
        if all_versions_numbers.is_empty(){
            Ok(None)
        } else if let Some(&max_version_number) = all_versions_numbers.iter().max() {
            Ok(Some(max_version_number))
        } else {
            Err(Error::new("Couldn't get maximum version number".into()))
        }
    }

    fn next_version_number(all_versions_numbers: &[u32]) -> Result<u32, Error> {
        if let Some(max_version_number) = Self::max_version_number(all_versions_numbers)? {
            Ok(max_version_number + 1)
        } else {
            Ok(0)
        }
    }

    fn compose_version_dir_name(&self, version_id: &str, version_number: u32) -> String {
        String::from("/") + version_number.to_string().as_str() + VERSION_DELIMITER + version_id
    }

    fn compose_version_path(&self, version_id: &str, version_number: u32) -> String {
        self.versions_path.to_owned() + self.compose_version_dir_name(version_id, version_number).as_str()
    }

    // Removes the oldest versions (by version number) to make the total number of existing versions the same as VERSIONS_STORED
    fn trim_versions(&self) -> Result<(), Error> {
        let all_versions = self.get_all_versions()?;

        if all_versions.len() > VERSIONS_STORED as usize {
            let max_version_number = Self::max_version_number(
                all_versions.iter()
                    .map(|vn|*vn.1).collect::<Vec<u32>>().as_slice()
            )?.ok_or(Error::new("Missing the maximal version number".into()))?;

            assert!(max_version_number >= VERSIONS_STORED);

            let min_version_number = max_version_number - VERSIONS_STORED + 1;
            for (id, &num) in &all_versions {
                if num < min_version_number {
                    clear_path(
                        self.compose_version_path(id, num).as_str()
                    )?;
                }
            }
        }
        Ok(())
    }

    fn create_version(&self, version_id: &str) -> Result<(), Error> {
        let all_versions = self.get_all_versions()?;

        // Checking if the specified version_id already exists among all saved versions
        if all_versions.get(version_id).is_none() {
            let all_versions_numbers = all_versions.iter()
                .map(|vn|*vn.1).collect::<Vec<u32>>();
            let next_version_number = Self::next_version_number(
                all_versions_numbers.as_slice()
            )?;

            let version_path_str = self.compose_version_path(version_id, next_version_number);
            let version_path = Path::new(&version_path_str);

            if !version_path.exists(){
                self.db.create_checkpoint_object()?.create_checkpoint(version_path)?;
                self.trim_versions()
            } else {
                Err(Error::new("Checkpoint already exists".into()))
            }
        } else {
            Err(Error::new("Specified version already exists".into()))
        }
    }

    fn open_version(&self, version_id: &str) -> Result<TransactionDB, Error> {
        if let Some(version_number) = self.get_all_versions()?.get(version_id){
            let version_path_str = self.compose_version_path(version_id, *version_number);
            let version_path = Path::new(&version_path_str);

            if version_path.exists(){
                TransactionDB::open_cf_all(&Options::default(), &version_path)
            } else {
                Err(Error::new("Specified version can't be opened".into()))
            }
        } else {
            Err(Error::new("Specified version doesn't exist".into()))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::storage_versioned::{StorageVersioned, VERSIONS_STORED};
    use crate::common::{Reader, clear_path};
    use crate::common::transaction::TransactionBasic;
    use crate::common::storage::ColumnFamiliesManager;
    use rand::Rng;
    use itertools::Itertools;

    // TODO: Change to /tmp/ to make tests run in other environments
    fn test_dir(subdir: &str) -> String {
        String::from("/mnt/ramfs_dir/") + subdir
    }

    fn gen_versions_ids(versions_num: u32) -> Vec<String> {
        let mut rng = rand::thread_rng();
        (0.. versions_num).into_iter()
            .map(|_|rng.gen::<u128>().to_string()).collect()
    }

    #[test]
    fn storage_is_consecutive_set_tests(){
        assert!(StorageVersioned::is_consecutive_set(&vec![]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&0]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&1]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&0, &1]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&5, &6, &7]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&0, &1, &2, &3, &4, &5]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&5, &4, &3, &2, &1, &0]));
        assert!(StorageVersioned::is_consecutive_set(&vec![&4, &2, &1, &0, &5, &3]));

        assert!(!StorageVersioned::is_consecutive_set(&vec![&0, &0]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&1, &1]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&0, &1, &2, &0]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&5, &4, &3, &2, &0]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&0, &2, &3, &4, &5]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&0, &1, &2, &3, &5]));
        assert!(!StorageVersioned::is_consecutive_set(&vec![&4, &2, &0, &5, &3]));
    }

    #[test]
    fn storage_versioned_versions_trimming_tests(){
        let storage_path = test_dir("storage_versioned_versions_trimming_tests");
        clear_path(storage_path.as_str()).unwrap();

        let storage = StorageVersioned::open(storage_path.as_str(), true).unwrap();

        let versions_ids = gen_versions_ids(VERSIONS_STORED * 2);

        versions_ids.iter().for_each(
            |version_id|{
                // Creating versions
                assert!(
                    storage.create_transaction(None) // creating transaction for a current state of Storage
                        .unwrap()
                        .commit(version_id.as_str())
                        .is_ok()
                );
            }
        );

        let min_index_of_existing_version = versions_ids.len() - VERSIONS_STORED as usize;

        versions_ids.iter().enumerate().for_each(
            |(i, version_id)| {
                if i < min_index_of_existing_version {
                    assert!(storage.create_transaction(Some(version_id)).is_err())
                } else {
                    assert!(storage.create_transaction(Some(version_id)).is_ok())
                }
            }
        );
    }

    #[test]
    fn storage_versioned_rollback_tests(){
        let storage_path = test_dir("storage_versioned_rollback_tests");
        clear_path(storage_path.as_str()).unwrap();

        let mut storage = StorageVersioned::open(storage_path.as_str(), true).unwrap();

        assert!(storage.last_version().unwrap().is_none());

        let versions_ids = gen_versions_ids(VERSIONS_STORED * 2);
        let mut versions_content: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        versions_ids.iter().enumerate().for_each(
            |(i, version_id)|{
                // Creating versions with new (number, version) KV-pair contained for each version
                let tx = storage.create_transaction(None).unwrap();

                versions_content.push(
                    (Vec::from(i.to_be_bytes()), version_id.bytes().collect::<Vec<u8>>())
                );
                let kv = versions_content.last().unwrap();

                assert!(tx.update(&vec![(&kv.0.as_slice(), &kv.1.as_slice())], &vec![]).is_ok());
                assert!(tx.commit(version_id.as_str()).is_ok());

                if i < VERSIONS_STORED as usize {
                    assert_eq!(storage.rollback_versions().unwrap(), versions_ids[0 ..= i]);
                } else {
                    assert_eq!(storage.rollback_versions().unwrap(), versions_ids[(i - VERSIONS_STORED as usize + 1) ..= i]);
                }
                assert_eq!(storage.last_version().unwrap().unwrap(), version_id.to_owned());
            }
        );

        let min_index_of_existing_version = versions_ids.len() - VERSIONS_STORED as usize;

        // Rollback to non-existing version
        assert!(storage.rollback(versions_ids[min_index_of_existing_version - 1].as_str()).is_err());
        // Rollback to the latest version, which contains the current state; Doesn't change the versions set
        assert!(storage.rollback(versions_ids[versions_ids.len() - 1].as_str()).is_ok());

        // Closing the storage
        drop(storage);

        // Rollbacks to the previous versions
        for i in 1.. VERSIONS_STORED as usize {
            // Reopening the storage to imitate different sessions between rollbacks (storage in this scope is dropped in the end of each iteration)
            let mut storage = StorageVersioned::open(storage_path.as_str(), true).unwrap();

            assert!(storage.rollback(versions_ids[versions_ids.len() - 1 - i].as_str()).is_ok());

            // KV-pairs are exactly the same as should be for a current version
            assert_eq!(storage.get_all().into_iter().sorted().collect::<Vec<_>>(),
                       versions_content[..= versions_content.len() - 1 - i])
        }

        // Updating the storage after it has been rolled back
        // (using versions_ids.len() as Key to ensure it has the biggest value among all existing keys in the storage)
        let last_kv = (Vec::from(versions_ids.len().to_be_bytes()), Vec::from("value"));

        storage = StorageVersioned::open(storage_path.as_str(), true).unwrap();
        {
            let tx = storage.create_transaction(None).unwrap();
            assert!(tx.update(&vec![(&last_kv.0, &last_kv.1)], &vec![]).is_ok());
            assert!(tx.commit("last").is_ok());
        }
        assert_eq!(storage.last_version().unwrap().unwrap(), "last");
        let storage_content = storage.get_all();
        assert_eq!(storage_content.get(&last_kv.0).unwrap(), &last_kv.1);

        // All KV-pairs for the version to which storage was rolled back + last_kv
        // (last_kv has the biggest value of Key so it is a last element in a sorted array)
        assert_eq!(storage_content.into_iter().sorted().collect::<Vec<_>>(),
                   [&versions_content[..= versions_content.len() - 1 - (VERSIONS_STORED as usize - 1)], &[last_kv]].concat())
    }

    #[test]
    fn storage_versioned_versioning_tests(){
        let storage_path = test_dir("storage_versioned_versioning_test");
        clear_path(storage_path.as_str()).unwrap();

        let storage = StorageVersioned::open(storage_path.as_str(), true).unwrap();

        let mut rng = rand::thread_rng();
        let versions_ids: Vec<String> = (0u32.. VERSIONS_STORED).into_iter()
            .map(|_|rng.gen::<u128>().to_string()).collect();

        versions_ids.iter().for_each(
            |version_id|{
                // Trying to create transaction for a not yet existing version
                assert!(storage.create_transaction(Some(version_id)).is_err());

                // Trying to save current state into the same version in contexts of different transactions
                assert!(
                    storage.create_transaction(None) // creating transaction for a current state of Storage
                        .unwrap()
                        .commit(version_id.as_str())
                        .is_ok()
                );
                assert!(
                    storage.create_transaction(None)
                        .unwrap()
                        .commit(version_id.as_str())
                        .is_err() // can't commit more than once to the same version
                )
            }
        );

        versions_ids.iter().for_each(
            |version_id| {
                assert!(
                    storage.create_transaction(Some(version_id)) // creating transaction for a previous state (version) of Storage
                        .unwrap()
                        .commit("some_version_id")
                        .is_err() // transaction created for previous version can't be committed
                )
            }
        );
    }

    #[test]
    fn storage_versioned_tests(){
        let storage_path = test_dir("storage_versioned_test");
        clear_path(storage_path.as_str()).unwrap();

        assert!(StorageVersioned::open(storage_path.as_str(), false).is_err());

        // just creating a storage, then reopening it with the further 'StorageVersioned::open' call
        drop(StorageVersioned::open(storage_path.as_str(), true).unwrap());

        let storage = StorageVersioned::open(storage_path.as_str(), false).unwrap();

        let tx = storage.create_transaction(None).unwrap();

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
        tx.commit("version_id1").unwrap();
        assert!(!storage.is_empty());

        drop(tx); // closing the 'tx'

        let tx2 = storage.create_transaction(None).unwrap();

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

            let all_values = reader.get_all();
            assert_eq!(all_values.len(), 3);
            assert_eq!(all_values[&b"k1".to_vec()], b"v1".to_vec());
            assert_eq!(all_values[&b"k2".to_vec()], b"v2".to_vec());
            assert_eq!(all_values[&b"k3".to_vec()], b"v3".to_vec());
        }

        // testing the Reader interface of the transaction
        test_reader(&tx2);
        tx2.commit("version_id2").unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }

    #[test]
    fn storage_versioned_cf_tests(){
        let storage_path = test_dir("storage_versioned_cf_test");
        clear_path(storage_path.as_str()).unwrap();

        let mut storage_ = StorageVersioned::open(storage_path.as_str(), true).unwrap();

        assert!(storage_.get_column_family("default").is_some());
        assert!(storage_.set_column_family("cf1").is_ok());
        assert!(storage_.set_column_family("cf2").is_ok());

        drop(storage_); // closing the 'storage_'

        let storage = StorageVersioned::open(storage_path.as_str(), false).unwrap();

        assert!(storage.get_column_family("default").is_some());
        let cf1 = storage.get_column_family("cf1").unwrap();
        let cf2 = storage.get_column_family("cf2").unwrap();

        let tx = storage.create_transaction(None).unwrap();

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
        tx.commit("version_id1").unwrap();
        assert!(!storage.is_empty_cf(cf1).unwrap() && !storage.is_empty_cf(cf2).unwrap());

        drop(tx); // closing the 'trans'

        let tx2 = storage.create_transaction(None).unwrap();

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
        test_reader(&tx2);
        tx2.commit("version_id2").unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }
}