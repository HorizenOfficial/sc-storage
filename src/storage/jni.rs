use jni::JNIEnv;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jobject, jboolean, jbyteArray, jobjectArray, jint};
use crate::common::jni::{unwrap_ptr, create_java_object, exception::_throw_inner, unwrap_mut_ptr};
use crate::storage::Storage;
use crate::storage::transaction::Transaction;
use crate::common::jni::reader;
use crate::common::jni::transaction_basic;
use crate::common::jni::cf_manager;

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

    match Storage::open(
        storage_path.to_str().unwrap(),
        _create_if_missing != 0
    ){
        Ok(storage) => {
            let storage_class = _env.find_class("com/horizen/storage/Storage")
                .expect("Should be able to find class Storage");
            create_java_object(&_env, &storage_class, storage)
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot open storage: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _storage: *mut Storage,
){
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
    reader::get(
        unwrap_ptr::<Storage>(&_env, _storage),
        _env, _cf, _key
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeMultiGet(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    reader::multi_get(
        unwrap_ptr::<Storage>(&_env, _storage),
        _env, _cf, _keys
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeIsEmpty(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
) -> jboolean
{
    reader::is_empty(
        unwrap_ptr::<Storage>(&_env, _storage),
        _env, _cf
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeGetIter(
    _env: JNIEnv,
    _storage: JObject,
    _cf: JObject,
    _mode: jint,
    _starting_key: jbyteArray,
    _direction: jint
) -> jobject
{
    reader::get_iter(
        unwrap_ptr::<Storage>(&_env, _storage),
        _env, _cf, _mode, _starting_key, _direction
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeSetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
){
    cf_manager::set_column_family(
        unwrap_mut_ptr::<Storage>(&_env, _storage),
        _env, _cf_name
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Storage_nativeGetColumnFamily(
    _env: JNIEnv,
    _storage: JObject,
    _cf_name: JString
) -> jobject
{
    cf_manager::get_column_family(
        unwrap_ptr::<Storage>(&_env, _storage),
        _env, _cf_name
    )
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
        JObject::null().into_inner()
    }
}

// ------------------------------------- Transaction JNI wrappers -------------------------------------

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeCommit(
    _env: JNIEnv,
    _transaction: JObject,
) {
    let transaction = unwrap_ptr::<Transaction>(&_env, _transaction);
    match transaction.commit(){
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
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _transaction: *mut Transaction,
){
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
    reader::get(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env, _cf, _key
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeMultiGet(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    reader::multi_get(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env, _cf, _keys
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeIsEmpty(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
) -> jboolean
{
    reader::is_empty(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env, _cf
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeGetIter(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _mode: jint,
    _starting_key: jbyteArray,
    _direction: jint
) -> jobject
{
    reader::get_iter(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env, _cf, _mode, _starting_key, _direction
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeUpdate(
    _env: JNIEnv,
    _transaction: JObject,
    _cf: JObject,
    _to_update: JObject,      // Map<byte[], byte[]>
    _to_delete: jobjectArray  // byte[][]
){
    transaction_basic::update(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env, _cf, _to_update, _to_delete
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeSave(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::save(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeRollbackToSavepoint(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::rollback_to_savepoint(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env
    )
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_storage_Transaction_nativeRollback(
    _env: JNIEnv,
    _transaction: JObject,
){
    transaction_basic::rollback(
        unwrap_ptr::<Transaction>(&_env, _transaction),
        _env
    )
}
