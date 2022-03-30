use std::ptr::null_mut;
use jni::JNIEnv;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jboolean, jbyteArray, jint, jobject, jobjectArray};
use crate::common::jni::{cf_manager, create_cf_java_object, create_jarray, create_storage_java_object, create_transaction_java_object, create_transaction_versioned_java_object, exception::_throw_inner, reader, transaction_basic, unwrap_mut_ptr, unwrap_ptr};
use crate::common::storage::{ColumnFamiliesManager, DEFAULT_CF_NAME};
use crate::storage_versioned::StorageVersioned;
use crate::storage_versioned::transaction_versioned::TransactionVersioned;

// ------------------------------------- StorageVersioned JNI wrappers -------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeOpen(
    _env: JNIEnv,
    _class: JClass,
    _storage_path: JString,
    _create_if_missing: jboolean,
    _versions_stored: jint
) -> jobject
{
    let storage_path = _env.get_string(_storage_path)
        .expect("Should be able to read _storage_path jstring as JavaStr");

    if _versions_stored < 0 {
        throw!(
                &_env, "java/lang/Exception",
                "Number of stored versions can't be negative",
                JObject::null().into_inner()
            )
    }

    match StorageVersioned::open(
        storage_path.to_str().expect("Should be able to convert the storage_path to Rust String"),
        _create_if_missing != 0,
        _versions_stored as usize
    ){
        Ok(storage) => {
            let storage_class = _env.find_class("com/horizen/storageVersioned/StorageVersioned")
                .expect("Should be able to find class StorageVersioned");
            create_storage_java_object(&_env, &storage_class, storage)
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot open the versioned storage: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _storage: *mut StorageVersioned,
){
    if !_storage.is_null(){
        drop(unsafe { Box::from_raw(_storage) })
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeCreateTransaction(
    _env: JNIEnv,
    _storage: JObject,
    _version_id: JString
) -> jobject
{
    let storage = unwrap_ptr::<StorageVersioned>(&_env, _storage);
    let version_id_javastr_opt =
        if _version_id.into_inner() != null_mut() {
            Some(
                _env.get_string(_version_id)
                    .expect("Should be able to read _version_id jstring as JavaStr")
            )
        } else {
            None
        };

    let version_id_opt =
        if let Some(version_id_javastr) = version_id_javastr_opt.as_ref() {
            Some(
                version_id_javastr.to_str().expect("Should be able to convert the version_id to Rust string")
            )
        } else {
            None
        };

    if let Ok(transaction) = storage.create_transaction(version_id_opt){
        let transaction_class = _env.find_class("com/horizen/storageVersioned/TransactionVersioned")
            .expect("Should be able to find class TransactionVersioned");
        if version_id_opt.is_none(){
            let default_cf = storage.get_column_family(DEFAULT_CF_NAME)
                .expect("Should be able to get the default column family from StorageVersioned");
            create_transaction_java_object(&_env, &transaction_class, transaction, default_cf)
        } else { // Transaction is opened for some version of the storage so it has its own underlying DB with CFs descriptors
            // Default CF descriptor should be retrieved from transaction
            create_transaction_versioned_java_object(&_env, &transaction_class, transaction)
        }

    } else {
        JObject::null().into_inner()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeRollback(
    _env: JNIEnv,
    _storage: JObject,
    _version_id: JString
){
    let storage = unwrap_mut_ptr::<StorageVersioned>(&_env, _storage);

    let version_id = _env
        .get_string(_version_id)
        .expect("Should be able to read _cf_name jstring as JavaStr");

    match storage.rollback(
        version_id.to_str()
            .expect("Should be able to convert the version_id to Rust String")){
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot rollback the storage: {:?}", e).as_str()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeRollbackVersions(
    _env: JNIEnv,
    _storage: JObject
)-> jobjectArray {
    let storage = unwrap_ptr::<StorageVersioned>(&_env, _storage);
    match storage.rollback_versions() {
        Ok(rollback_versions) => {
            let string_class = _env
                .find_class("java/lang/String")
                .expect("Should be able to find String class");

            let default_string = _env.new_string("")
                .expect("Should be able to convert Rust string to Java String");

            let jstrings = rollback_versions.iter()
                .map(|version|{
                    _env.new_string(version)
                        .expect("Should be able to convert Rust string to Java String").into_inner()
                }).collect::<Vec<_>>();

            create_jarray(
                &_env,
                string_class,
                default_string.into_inner(),
                jstrings
            )
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot get versions of the storage: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeLastVersion(
    _env: JNIEnv,
    _storage: JObject,
    _version_id: JString
)-> jobject {
    let storage = unwrap_ptr::<StorageVersioned>(&_env, _storage);
    match storage.last_version() {
        Ok(last_version_opt) => {
            if let Some(last_version) = last_version_opt {
                _env.new_string(last_version)
                    .expect("Should be able to convert Rust string to Java String").into_inner()
            } else {
                JObject::null().into_inner()
            }
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot get last version of the storage: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeSetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
){
    cf_manager::set_column_family(
        unwrap_mut_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf_name
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeGetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
) -> jobject
{
    cf_manager::get_column_family(
        unwrap_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf_name
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeGet(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _key: jbyteArray
) -> jbyteArray
{
    reader::get(
        unwrap_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf, _key
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeMultiGet(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    reader::multi_get(
        unwrap_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf, _keys
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeIsEmpty(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
) -> jboolean
{
    reader::is_empty(
        unwrap_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_StorageVersioned_nativeGetIter(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _mode: jint,
    _starting_key: jbyteArray,
    _direction: jint
) -> jobject
{
    reader::get_iter(
        unwrap_ptr::<StorageVersioned>(&_env, _storage),
        _env, _cf, _mode, _starting_key, _direction
    )
}

// ------------------------------------- Transaction JNI wrappers -------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _transaction: *mut TransactionVersioned,
){
    if !_transaction.is_null(){
        drop(unsafe { Box::from_raw(_transaction) })
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeCommit(
    _env: JNIEnv,
    _transaction: JObject,
    _version_id: JString
) {
    let transaction = unwrap_ptr::<TransactionVersioned>(&_env, _transaction);
    let version_id = _env.get_string(_version_id)
        .expect("Should be able to read _version_id jstring as JavaStr");

    match transaction.commit(version_id.to_str().expect("Should be able to convert the version_id to Rust String")){
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot commit the transaction: {:?}", e).as_str()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeGetColumnFamily(
    _env: JNIEnv,
    _transaction: JObject,
    _cf_name: JString
) -> jobject
{
    let transaction = unwrap_ptr::<TransactionVersioned>(&_env, _transaction);

    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read _cf_name jstring as JavaStr");

    match transaction.get_column_family(
        cf_name.to_str()
            .expect("Should be able to convert the cf_name to Rust String")){
        Ok(cf_opt) => {
            if let Some(cf_ref) = cf_opt {
                create_cf_java_object(&_env, cf_ref)
            } else {
                JObject::null().into_inner()
            }
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot get column family for previous version of the storage: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeGet(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _key: jbyteArray
) -> jbyteArray
{
    reader::get(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env, _cf, _key,
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeMultiGet(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    reader::multi_get(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env, _cf, _keys
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeIsEmpty(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
) -> jboolean
{
    reader::is_empty(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env, _cf
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeGetIter(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _mode: jint,
    _starting_key: jbyteArray,
    _direction: jint
) -> jobject
{
    reader::get_iter(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env, _cf, _mode, _starting_key, _direction
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeUpdate(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _to_update: JObject,      // Map<byte[], byte[]>
    _to_delete: jobjectArray  // byte[][]
){
    transaction_basic::update(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env, _cf, _to_update, _to_delete
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeSave(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::save(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeRollbackToSavepoint(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::rollback_to_savepoint(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storageVersioned_TransactionVersioned_nativeRollback(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::rollback(
        unwrap_ptr::<TransactionVersioned>(&_env, _transaction),
        _env
    )
}
