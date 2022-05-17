use itertools::Itertools;
use jni::objects::JObject;
use jni::JNIEnv;
use rocksdb::ColumnFamily;
use crate::common::jni::{unwrap_ptr, exception::_throw_inner, java_list_to_vec_byte};
use crate::common::transaction::TransactionBasic;

pub(crate) fn update(
    transaction: &dyn TransactionBasic,
    _env: JNIEnv,
    _cf:  JObject,
    _keys_to_update:   JObject, // List<byte[]>
    _values_to_update: JObject, // List<byte[]>
    _keys_to_delete:   JObject  // List<byte[]>
){
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Some(keys_to_update) = java_list_to_vec_byte(&_env, _keys_to_update){
        if let Some(values_to_update) = java_list_to_vec_byte(&_env, _values_to_update){
            if keys_to_update.len() != values_to_update.len(){
                throw!(
                    &_env, "java/lang/Exception",
                    "List of Keys to update should be of the same length as the list of Values"
                )
            }
            if let Some(keys_to_delete) = java_list_to_vec_byte(&_env, _keys_to_delete){
                let to_update_map = keys_to_update.iter().zip(values_to_update).collect_vec();
                match transaction.update_cf(
                    cf,
                    &to_update_map.iter().map(|kv| (kv.0.as_slice(), kv.1.as_slice())).collect(),
                    &keys_to_delete.iter().map(|k| k.as_slice()).collect()
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
                    format!("Cannot convert Java list of keys to delete to a Rust vector").as_str()
                )
            }
        } else {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot convert Java list of values to a Rust vector").as_str()
            )
        }
    } else {
        throw!(
            &_env, "java/lang/Exception",
            format!("Cannot convert Java list of keys to a Rust vector").as_str()
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
