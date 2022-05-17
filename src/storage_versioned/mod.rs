use rocksdb::{TransactionDB, Error, Options};
use crate::common::{InternalRef, InternalReader, Reader, clear_path, join_path_strings};
use crate::TransactionInternal;
use crate::common::storage::ColumnFamiliesManager;
use std::path::Path;
use rocksdb::transactions::ops::{OpenCF, TransactionBegin, CreateCheckpointObject};
use crate::storage_versioned::transaction_versioned::TransactionVersioned;
use std::collections::HashMap;
use fs_extra::dir::{copy, CopyOptions};
use std::fs::rename;
use itertools::{Itertools, Either};

pub mod transaction_versioned;
pub mod jni;

// Delimiter between version number and version ID in a version (i.e. checkpoint) directory name
const VERSION_DELIMITER: &str = "__";

pub struct StorageVersioned {
    db: TransactionDB,      // handle of an opened DB which contains current state of a storage
    db_path: String,        // absolute path to the 'CurrentState' directory (which contains the DB with current state)
    versions_path: String,  // absolute path to the 'Versions' directory (which contains storage's versions)
    base_path: String,      // absolute path to the storage (directory which contains the 'CurrentState' and 'Versions' subdirectories)
    versions_stored: usize  // number of the latest versions of storage to be stored
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

    // Directory for storing a current state of a storage (DB)
    const DB_DIR: &'static str = "CurrentState";
    // Directory for storing versions of the storage (Checkpoints)
    const VERSIONS_DIR: &'static str = "Versions";

    // Opens a storage located by a specified path or creates a new one if the directory by a specified path doesn't exist and 'create_if_missing' is true
    // The 'versions_stored' parameter specifies how many latest versions (0 or more) should be stored for a storage.
    // If at the moment of opening of an existing storage there are more saved versions than 'versions_stored' specifies, then the oldest versions will be removed.
    // Returns Result with StorageVersioned instance or Err with a describing message if some error occurred
    pub fn open(path: &str, create_if_missing: bool, versions_stored: usize) -> Result<Self, Error>{
        let db_path = join_path_strings(path.to_owned().as_str(), Self::DB_DIR)?;

        // Preparing the CurrentState DB directory if it doesn't exist
        let db_path_exists = Path::new(db_path.as_str()).exists();
        if !db_path_exists {
            if !create_if_missing {
                return Err(Error::new("No need to create a DB (DB does not exist and the create_if_missing == false)".into()));
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
            if db_path_exists {
                TransactionDB::open_cf_all(&opts, &db_path)?
            } else {
                TransactionDB::open_cf_default(&opts, &db_path)?
            };

        // Creating the Versions directory if it doesn't exist
        let versions_path = join_path_strings(path.to_owned().as_str(), Self::VERSIONS_DIR)?;
        if !Path::new(versions_path.as_str()).exists(){
            if std::fs::create_dir(&versions_path).is_err(){
                drop(db);
                return if std::fs::remove_dir(&db_path).is_ok() {
                    Err(Error::new("Versions directory can't be created".into()))
                } else {
                    Err(Error::new("Versions directory can't be created; Can't clean the DB directory".into()))
                }
            }
        }

        let storage = Self{
            db,
            db_path: Self::absolute_path(&db_path)?,
            versions_path: Self::absolute_path(&versions_path)?,
            base_path: Self::absolute_path(&path.to_owned())?,
            versions_stored
        };
        // Setting the number of recent versions according to the value of 'self.versions_stored'
        // This method internally also scans all existing versions and checks that their numbers are a consecutive set (by calling 'get_all_versions')
        storage.trim_versions()?;

        Ok(storage)
    }

    // Creates a transaction for a current state of storage if 'version_id_opt' is 'None', or for a specified previous version of the storage otherwise.
    // Returns Result with TransactionVersioned or with Error message if some error occurred
    pub fn create_transaction(&self, version_id_opt: Option<&str>) -> Result<TransactionVersioned, Error> {
        Ok(
            if let Some(version_id) = version_id_opt {
                let db_version = self.open_version(version_id)?;
                TransactionVersioned::new(db_version.transaction_default()?, Either::Right(db_version))
            } else {
                TransactionVersioned::new(self.db.transaction_default()?, Either::Left(self))
            }
        )
    }

    // Rollbacks current state of the storage to a specified with 'version_id' previous version.
    // All saved versions after the 'version_id' are deleted if rollback is successful.
    // Returns Result with error message if some error occurs
    pub fn rollback(&mut self, version_id: &str) -> Result<(), Error> {
        let all_versions = self.get_all_versions()?;

        if let Some(&version_number) = all_versions.get(version_id) {
            // Copying the specified version into the base directory of the Storage
            if copy(self.compose_version_path(version_id, version_number)?.as_str(),
                    self.base_path.as_str(),
                    &CopyOptions::new()).is_ok() {

                // Closing DB in the CurrentState directory
                // NOTE: is equivalent to drop(replace(&mut self.db, TransactionDB::dummy_db()));
                self.db = TransactionDB::dummy_db();

                // Removing the CurrentState directory
                clear_path(self.db_path.as_str())?;

                let version_copy_path =
                    join_path_strings(self.base_path.as_str(),
                                     self.compose_version_dir_name(version_id, version_number).as_str())?;

                // Renaming the copied version's directory to the 'CurrentState'
                if rename(version_copy_path, self.db_path.as_str()).is_ok(){
                    // Opening the copied DB and putting its handle into 'self.db'
                    self.db = TransactionDB::open_cf_all(&Options::default(), &self.db_path)?;

                    // Removing all versions which follow the restored version
                    for (id, &num) in &all_versions {
                        if num > version_number {
                            clear_path(
                                self.compose_version_path(id, num)?.as_str()
                            )?;
                        }
                    }
                    Ok(())
                } else {
                    Err(Error::new("Can't rename the copied version in the base directory".into()))
                }
            } else {
                Err(Error::new("Can't copy the specified version into the base directory".into()))
            }
        } else {
            Err(Error::new("Specified version doesn't exist".into()))
        }
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

    // Converts path into absolute format with Path::canonicalize method
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

    // Checks if all elements of a given set form a contiguous sequence when being sorted
    fn is_contiguous_set(set: &Vec<usize>) -> bool {
        let mut set_sorted = set.clone();
        set_sorted.sort();
        let mut prev_elem = 0usize;

        for (pos, elem) in set_sorted.into_iter().enumerate() {
            if pos != 0 &&
               elem != prev_elem + 1 {
                return false;
            }
            prev_elem = elem;
        }
        true
    }

    // Retrieves a list of all subdirectories from the 'Version' directory,
    // then creates a HashMap of (VersionID -> VersionNumber) from directories names,
    // then checks that all VersionNumbers are contiguous
    // Returns Result with full list of available storage versions as HashMap<VersionID, VersionNumber> or error message if some error occurred
    fn get_all_versions(&self) -> Result<HashMap<String, usize>, Error> {
        // Retrieving a list of all subdirectories from the 'Versions' directory
        let paths = std::fs::read_dir(self.versions_path.as_str()).unwrap();
        let mut paths_count = 0;

        let id_to_num: HashMap<String, usize> = paths.into_iter()
            .flat_map(|path|{    // counting the total number of subdirectories with versions
                paths_count += 1;
                path
            })
            .flat_map(|path|            // extracting versions' directories names from paths
                path.file_name().into_string()
            )
            .flat_map(|num_id|{           // parsing directories names into (version_id, version_number)
                let num_id_splitted = num_id.as_str().split(VERSION_DELIMITER).collect::<Vec<&str>>();
                if num_id_splitted.len() != 2 { // directory name should contain only two delimited parts
                    None
                } else if let Ok(version_number) = num_id_splitted[0].to_owned().parse::<usize>(){ // parsing the first part as a number
                    // the second part remains to be a string and is placed as a Key into the HashMap
                    Some((num_id_splitted[1].to_owned(), version_number)) // (version_id, version_number)
                } else {
                    None
                }
            })
            .collect();

        // Checking that all directories have been successfully parsed
        if id_to_num.len() == paths_count {
            // Checking that all versions numbers are a contiguous sequence
            if Self::is_contiguous_set(&id_to_num.iter().map(|v|*v.1).collect()) {
                Ok(id_to_num)
            } else {
                Err(Error::new("Versions' numbers are not contiguous".into()))
            }
        } else {
            Err(Error::new("Versions' directories names weren't parsed successfully".into()))
        }
    }

    // Returns the next number for a given list of versions' numbers or 0 if the list is empty
    fn next_version_number(all_versions_numbers: &[usize]) -> Result<usize, Error> {
        if let Some(&max_version_number) = all_versions_numbers.iter().max() {
            Ok(max_version_number + 1)
        } else { // there are no versions (numbers) so start with 0 the numbering
            Ok(0)
        }
    }

    // Composes directory name for a specified version ID and its number as '/versionNumber__versionID'
    fn compose_version_dir_name(&self, version_id: &str, version_number: usize) -> String {
        version_number.to_string() + VERSION_DELIMITER + version_id
    }

    // Composes absolute path for a specified version as: self.versions_path + '/' + version_dir_name
    fn compose_version_path(&self, version_id: &str, version_number: usize) -> Result<String, Error> {
        join_path_strings(self.versions_path.as_str(),
                          self.compose_version_dir_name(version_id, version_number).as_str())
    }

    // Removes the oldest versions (by version number) to make the total number of existing versions the same as 'self.versions_stored'
    fn trim_versions(&self) -> Result<(), Error> {
        let all_versions = self.get_all_versions()?;

        if all_versions.len() > self.versions_stored {
            if let Some(max_version_number) = all_versions.iter().map(|vn|*vn.1).max(){
                assert!(max_version_number >= self.versions_stored);

                let min_version_number = max_version_number - self.versions_stored + 1;
                for (id, &num) in &all_versions {
                    if num < min_version_number {
                        clear_path(
                            self.compose_version_path(id, num)?.as_str()
                        )?;
                    }
                }
            } else {
                return Err(Error::new("Can't get the maximum version number".into()))
            }
        }
        Ok(())
    }

    // Creates a new storage's version (checkpoint of the CurrentState) in the 'Versions' directory.
    // The name of version's directory is composed of a specified 'version_id' and the version's number
    // which is the next after the most recent previous version's number.
    // Removes the versions which are older than the most recent 'self.versions_stored' versions.
    // Returns Result with error message if a version with specified ID already exists or some other error occurred
    fn create_version(&self, version_id: &str) -> Result<(), Error> {
        if self.versions_stored > 0 { // no need to create any version in other case
            let all_versions = self.get_all_versions()?;

            // Checking if the specified 'version_id' already exists among all saved versions
            if all_versions.get(version_id).is_none() {
                let all_versions_numbers = all_versions.iter()
                    .map(|vn|*vn.1).collect::<Vec<usize>>();
                let next_version_number = Self::next_version_number(
                    all_versions_numbers.as_slice()
                )?;

                let version_path_str = self.compose_version_path(version_id, next_version_number)?;
                let version_path = Path::new(&version_path_str);

                // Creating checkpoint in a directory by 'version_path'
                self.db.create_checkpoint_object()?.create_checkpoint(version_path)?;
                // Removing the checkpoints which are not in a sliding window of 'self.versions_stored' size
                self.trim_versions()
            } else {
                Err(Error::new("Specified version already exists".into()))
            }
        } else {
            Ok(())
        }
    }

    // Opens a specified by 'version_id' version (checkpoint) of a storage and returns it's TransactionDB handle.
    // If the specified version doesn't exist or can't be opened then returns an Error with corresponding message
    fn open_version(&self, version_id: &str) -> Result<TransactionDB, Error> {
        if let Some(version_number) = self.get_all_versions()?.get(version_id){
            let version_path_str = self.compose_version_path(version_id, *version_number)?;
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
    use crate::storage_versioned::StorageVersioned;
    use crate::common::{Reader, test_dir, get_all, get_all_cf};
    use crate::common::transaction::TransactionBasic;
    use crate::common::storage::ColumnFamiliesManager;
    use rand::Rng;
    use itertools::Itertools;

    // Number of the latest versions of a storage to be stored
    const VERSIONS_STORED: usize = 10;

    fn gen_versions_ids(versions_num: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        (0.. versions_num).into_iter()
            .map(|_|rng.gen::<u128>().to_string()).collect()
    }

    #[test]
    fn storage_versioned_is_contiguous_set_tests(){
        assert!(StorageVersioned::is_contiguous_set(&vec![]));
        assert!(StorageVersioned::is_contiguous_set(&vec![0]));
        assert!(StorageVersioned::is_contiguous_set(&vec![1]));
        assert!(StorageVersioned::is_contiguous_set(&vec![0, 1]));
        assert!(StorageVersioned::is_contiguous_set(&vec![5, 6, 7]));
        assert!(StorageVersioned::is_contiguous_set(&vec![0, 1, 2, 3, 4, 5]));
        assert!(StorageVersioned::is_contiguous_set(&vec![5, 4, 3, 2, 1, 0]));
        assert!(StorageVersioned::is_contiguous_set(&vec![4, 2, 1, 0, 5, 3]));

        assert!(!StorageVersioned::is_contiguous_set(&vec![0, 0]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![1, 1]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![0, 1, 2, 0]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![5, 4, 3, 2, 0]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![0, 2, 3, 4, 5]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![0, 1, 2, 3, 5]));
        assert!(!StorageVersioned::is_contiguous_set(&vec![4, 2, 0, 5, 3]));
    }

    #[test]
    fn storage_versioned_versions_trimming_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_versioned_versions_trimming_tests").unwrap();

        let storage = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();

        let versions_ids = gen_versions_ids(VERSIONS_STORED * 2);

        // Creating versions
        versions_ids.iter().for_each(
            |version_id|{
                // Creating and committing transaction for a current state of storage
                assert!(
                    storage.create_transaction(None).unwrap()
                        .commit(version_id.as_str()).is_ok()
                );
            }
        );

        let min_index_of_existing_version = versions_ids.len() - VERSIONS_STORED;

        // Checking versions for existence
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
        assert_ne!(VERSIONS_STORED, 0, "Rollback test can't be run without any versions of a storage");

        let (_tmp_dir, storage_path) = test_dir("storage_versioned_rollback_tests").unwrap();

        let mut storage = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();
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

                if i < VERSIONS_STORED {
                    assert_eq!(storage.rollback_versions().unwrap(), versions_ids[0 ..= i]);
                } else {
                    assert_eq!(storage.rollback_versions().unwrap(), versions_ids[(i - VERSIONS_STORED + 1) ..= i]);
                }
                assert_eq!(storage.last_version().unwrap().unwrap(), version_id.to_owned());
            }
        );

        let min_index_of_existing_version = versions_ids.len() - VERSIONS_STORED;

        // Rollback to non-existing version
        assert!(storage.rollback(versions_ids[min_index_of_existing_version - 1].as_str()).is_err());
        // Rollback to the latest version, which contains the current state; Doesn't change the versions set
        assert!(storage.rollback(versions_ids[versions_ids.len() - 1].as_str()).is_ok());

        // Closing the storage
        drop(storage);

        // Rollbacks to the previous versions
        for i in 1.. VERSIONS_STORED {
            // Reopening the storage to imitate different sessions between rollbacks (storage in this scope is dropped in the end of each iteration)
            let mut storage = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();

            assert!(storage.rollback(versions_ids[versions_ids.len() - 1 - i].as_str()).is_ok());

            // KV-pairs are exactly the same as should be for a current version
            assert_eq!(get_all(&storage).into_iter().sorted().collect::<Vec<_>>(),
                       versions_content[..= versions_content.len() - 1 - i])
        }

        // Updating the storage after it has been rolled back
        // (using versions_ids.len() as Key to ensure it has the biggest value among all existing keys in the storage)
        let last_kv = (Vec::from(versions_ids.len().to_be_bytes()), Vec::from("value"));

        storage = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();
        {
            let tx = storage.create_transaction(None).unwrap();
            assert!(tx.update(&vec![(&last_kv.0, &last_kv.1)], &vec![]).is_ok());
            assert!(tx.commit("last").is_ok());
        }
        assert_eq!(storage.last_version().unwrap().unwrap(), "last");
        let storage_content = get_all(&storage);
        assert_eq!(storage_content.get(&last_kv.0).unwrap(), &last_kv.1);

        // All KV-pairs for the version to which storage was rolled back + last_kv
        // (last_kv has the biggest value of Key so it is a last element in a sorted array)
        assert_eq!(storage_content.into_iter().sorted().collect::<Vec<_>>(),
                   [&versions_content[..= versions_content.len() - 1 - (VERSIONS_STORED - 1)], &[last_kv]].concat())
    }

    #[test]
    fn storage_versioned_versioning_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_versioned_versioning_test").unwrap();

        let storage = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();

        let mut rng = rand::thread_rng();
        let versions_ids: Vec<String> = (0.. VERSIONS_STORED).into_iter()
            .map(|_|rng.gen::<u128>().to_string()).collect();

        // Creating versions of the storage
        versions_ids.iter().for_each(
            |version_id|{
                // Trying to create transaction for a not yet existing version
                assert!(storage.create_transaction(Some(version_id)).is_err());

                // Creating versions with new (version_id, version_id) KV-pair contained for each version
                let tx = storage.create_transaction(
                    None // creating transaction for a current state of Storage
                ).unwrap();

                let version_id_bytes = version_id.bytes().collect::<Vec<u8>>();
                assert!(tx.update(&vec![(&version_id_bytes.as_slice(), &version_id_bytes.as_slice())], &vec![]).is_ok());
                assert!(tx.commit(version_id.as_str()).is_ok()); // committing updates and creating new version

                // Trying to save the current state into an existing version
                assert!(
                    storage.create_transaction(None).unwrap()
                        .commit(version_id.as_str()).is_err() // can't commit more than once with the same version_id
                );
            }
        );

        // Opening the created versions
        versions_ids.iter().for_each(
            |version_id|{
                // Creating transaction for a previous state (version) of a storage
                let tx = storage.create_transaction(Some(version_id)).unwrap();

                // Explicitly accessing the default CF in transaction
                let default_cf = tx.get_column_family("default").unwrap().unwrap();
                let version_id_bytes = version_id.bytes().collect::<Vec<u8>>();
                assert_eq!(tx.get_cf(default_cf, version_id_bytes.as_slice()).unwrap(), version_id_bytes);

                // Transaction for a storage version can be updated
                assert!(tx.update_cf(default_cf,
                                     &vec![("key".as_ref(), "val".as_ref())],
                                     &vec![version_id_bytes.as_slice()]).is_ok()
                );

                // Transaction for a storage version can't be committed
                assert!(tx.commit("some_version_id").is_err());

                // Trying to open the same version twice (only one transaction per version can be active)
                assert!(storage.create_transaction(Some(version_id)).is_err());
                // New transaction for the same version can be opened only after an existing transaction is closed
                drop(tx);
                assert!(storage.create_transaction(Some(version_id)).is_ok());
            }
        );
    }

    #[test]
    fn storage_versioned_tests(){
        let (_tmp_dir, storage_path) = test_dir("storage_versioned_test").unwrap();

        assert!(StorageVersioned::open(storage_path.as_str(), false, VERSIONS_STORED).is_err());

        // just creating a storage, then reopening it with the further 'StorageVersioned::open' call
        drop(StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap());

        let storage = StorageVersioned::open(storage_path.as_str(), false, VERSIONS_STORED).unwrap();
        let tx = storage.create_transaction(None).unwrap();
        assert!(tx.is_empty());

        // Initializing the transaction with k1, k2, k4, k5 key-values
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

            let all_values = get_all(reader);
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
        let (_tmp_dir, storage_path) = test_dir("storage_versioned_cf_tests").unwrap();

        let mut storage_ = StorageVersioned::open(storage_path.as_str(), true, VERSIONS_STORED).unwrap();

        assert!(storage_.get_column_family("default").is_some());
        assert!(storage_.set_column_family("cf1").is_ok());
        assert!(storage_.set_column_family("cf2").is_ok());

        drop(storage_); // closing the 'storage_'

        let storage = StorageVersioned::open(storage_path.as_str(), false, VERSIONS_STORED).unwrap();

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

        drop(tx); // closing the 'tx'

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
        tx2.commit("version_id2").unwrap();
        // testing the Reader interface of the storage
        test_reader(&storage);
    }
}
