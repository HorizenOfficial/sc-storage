use jni::JNIEnv;
use jni::objects::{JClass, JValue, JString, JObject};
use rocksdb::ColumnFamily;
use jni::sys::{jobject, jlong, jboolean, jbyteArray, jobjectArray, JNI_TRUE, JNI_FALSE};
use std::ptr::null_mut;
use crate::common::jni::{unwrap_ptr, unwrap_mut_ptr, map_to_java_map, create_java_object, java_array_to_vec_byte, java_map_to_vec_byte};
use crate::common::storage::ColumnFamiliesManager;
use crate::common::Reader;
use crate::common::transaction::TransactionBasic;
use crate::storage::Storage;
use crate::storage::transaction::Transaction;

// ------------------------------------- Storage JNI wrappers -------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeOpen(
    _env: JNIEnv,
    _class: JClass,
    _storage_path: JString,
    _create_if_missing: jboolean
) -> jobject
{
    let storage_path = _env.get_string(_storage_path)
        .expect("Should be able to read jstring as Rust String");

    if let Ok(storage) = Storage::open(
        storage_path.to_str().unwrap(),
        _create_if_missing != 0
    ){
        let storage_class = _env.find_class("com/horizen/storage/Storage")
            .expect("Should be able to find class Storage");
        create_java_object(&_env, &storage_class, storage)
    } else {
        null_mut()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _storage: *mut Storage,
)
{
    if !_storage.is_null(){
        drop(unsafe { Box::from_raw(_storage) })
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeGet(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _key: jbyteArray
) -> jbyteArray
{
    let storage = unwrap_ptr::<Storage>(&_env, _storage);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    let key = _env.convert_byte_array(_key)
        .expect("Should be able to convert to Rust byte array");

    if let Some(value) = storage.get_cf(cf, key.as_slice()){
        _env.byte_array_from_slice(value.as_slice())
            .expect("Should be able to convert Rust slice into jbytearray")
    } else {
        null_mut()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeMultiGet(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    let storage = unwrap_ptr::<Storage>(&_env, _storage);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);
    let keys = java_array_to_vec_byte(&_env, _keys);

    let key_values = storage.multi_get_cf(
        cf,
        keys.iter().map(|k|k.as_slice()).collect::<Vec<_>>().as_slice()
    );
    map_to_java_map(&_env, &key_values)
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeIsEmpty(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
) -> jboolean {
    let storage = unwrap_ptr::<Storage>(&_env, _storage);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Ok(is_empty) = storage.is_empty_cf(cf){
        is_empty as jboolean
    } else {
        JNI_TRUE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeSetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
) -> jboolean {
    let storage = unwrap_mut_ptr::<Storage>(&_env, _storage);

    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read jstring as Rust String");

    if let Ok(()) = storage.set_column_family(cf_name.to_str().unwrap()){
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeGetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
) -> jobject
{
    let storage = unwrap_ptr::<Storage>(&_env, _storage);

    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read jstring as Rust String");

    if let Some(cf_ref) = storage.get_column_family(cf_name.to_str().unwrap()){
        let column_family_class = _env.find_class("com/horizen/common/ColumnFamily")
            .expect("Should be able to find class ColumnFamily");
        // Converting the cf_ref into a raw pointer then converting the raw pointer into jlong
        let column_family_ptr: jlong = jlong::from(
            cf_ref as *const ColumnFamily as i64
        );
        // Create and return new Java-ColumnFamily
        _env.new_object(column_family_class, "(J)V", &[JValue::Long(column_family_ptr)])
            .expect("Should be able to create new Java-object")
            .into_inner()
    } else {
        null_mut()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeCreateTransaction(
    _env: JNIEnv,
    _storage: JObject
) -> jobject
{
    let storage = unwrap_ptr::<Storage>(&_env, _storage);

    if let Ok(transaction) = storage.create_transaction(){
        let transaction_class = _env.find_class("com/horizen/storage/Transaction")
            .expect("Should be able to find class Transaction");
        create_java_object(&_env, &transaction_class, transaction)
    } else {
        null_mut()
    }
}
// ------------------------------------- Transaction JNI wrappers -------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _transaction: *mut Transaction,
)
{
    if !_transaction.is_null(){
        drop(unsafe { Box::from_raw(_transaction) })
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeGet(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _key: jbyteArray
) -> jbyteArray
{
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    let key = _env.convert_byte_array(_key)
        .expect("Should be able to convert to Rust byte array");

    if let Some(value) = transaction.get_cf(cf, key.as_slice()){
        _env.byte_array_from_slice(value.as_slice())
            .expect("Should be able to convert Rust slice into jbytearray")
    } else {
        null_mut()
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeMultiGet(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);
    let keys = java_array_to_vec_byte(&_env, _keys);

    let key_values = transaction.multi_get_cf(
        cf,
        keys.iter().map(|k|k.as_slice()).collect::<Vec<_>>().as_slice()
    );
    map_to_java_map(&_env, &key_values)
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeIsEmpty(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
) -> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Ok(is_empty) = transaction.is_empty_cf(cf){
        is_empty as jboolean
    } else {
        JNI_TRUE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeCommit(
    _env: JNIEnv,
    _transaction: JObject,
)-> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    if let Ok(()) = transaction.commit(){
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeUpdate(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _to_update: JObject,      // Map<byte[], byte[]>
    _to_delete: jobjectArray  // byte[][]
)-> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Some(to_update) = java_map_to_vec_byte(&_env, _to_update){
        let to_delete = java_array_to_vec_byte(&_env, _to_delete);

        if let Ok(()) = transaction.update_cf(
            cf,
            &to_update.iter().map(|kv| (kv.0.as_slice(), kv.1.as_slice())).collect(),
            &to_delete.iter().map(|k| k.as_slice()).collect()
        ){
            JNI_TRUE
        } else {
            JNI_FALSE
        }
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeSave(
    _env: JNIEnv,
    _transaction: JObject,
)-> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    if let Ok(()) = transaction.save(){
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeRollbackToSavepoint(
    _env: JNIEnv,
    _transaction: JObject,
)-> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    if let Ok(()) = transaction.rollback_to_savepoint(){
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeRollback(
    _env: JNIEnv,
    _transaction: JObject,
)-> jboolean {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    if let Ok(()) = transaction.rollback(){
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}
