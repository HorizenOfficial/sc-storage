
use jni::objects::JObject;
use jni::JNIEnv;
use jni::sys::jobjectArray;
use rocksdb::ColumnFamily;
use crate::common::jni::{unwrap_ptr, java_map_to_vec_byte, java_array_to_vec_byte, exception::_throw_inner};
use crate::common::transaction::TransactionBasic;

pub(crate) fn update(
    transaction: &dyn TransactionBasic,
    _env: JNIEnv,
    _cf: JObject,
    _to_update: JObject,      // Map<byte[], byte[]>
    _to_delete: jobjectArray  // byte[][]
){
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Some(to_update) = java_map_to_vec_byte(&_env, _to_update){
        let to_delete = java_array_to_vec_byte(&_env, _to_delete);

        match transaction.update_cf(
            cf,
            &to_update.iter().map(|kv| (kv.0.as_slice(), kv.1.as_slice())).collect(),
            &to_delete.iter().map(|k| k.as_slice()).collect()
        ) {
            Ok(()) => {}
            Err(e) => {
                throw!(
                    &_env, "java/lang/Exception",
                    format!("Cannot update column family of the transaction: {:?}", e).as_str()
                )
            }
        }
    } else {
        throw!(
                &_env, "java/lang/Exception",
                format!("nativeUpdate: Cannot convert java map to a vector of pairs").as_str()
            )
    }
}

pub(crate) fn save(
    transaction: &dyn TransactionBasic,
    _env: JNIEnv
){
    match transaction.save() {
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot save the transaction: {:?}", e).as_str()
            )
        }
    }
}

pub(crate) fn rollback_to_savepoint(
    transaction: &dyn TransactionBasic,
    _env: JNIEnv
){
    match transaction.rollback_to_savepoint(){
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot rollback the transaction to save point: {:?}", e).as_str()
            )
        }
    }
}

pub(crate) fn rollback(
    transaction: &dyn TransactionBasic,
    _env: JNIEnv
){
    match transaction.rollback(){
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot rollback the transaction: {:?}", e).as_str()
            )
        }
    }
}
