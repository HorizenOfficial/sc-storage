use crate::common::Reader;
use jni::objects::JObject;
use jni::JNIEnv;
use jni::sys::{jbyteArray, jobjectArray, jobject, jboolean, JNI_TRUE, jint};
use rocksdb::ColumnFamily;
use crate::common::jni::{unwrap_ptr, java_array_to_vec_byte, map_to_java_map, create_java_object, exception::_throw_inner};
use crate::common::jni::iterator::{parse_starting_key, parse_iterator_mode};

pub(crate) fn get(
    reader: &dyn Reader,
    _env: JNIEnv,
    _cf: JObject,
    _key: jbyteArray
) -> jbyteArray
{
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    let key = _env.convert_byte_array(_key)
        .expect("Should be able to convert _key to Rust byte array");

    if let Some(value) = reader.get_cf(cf, key.as_slice()){
        _env.byte_array_from_slice(value.as_slice())
            .expect("Should be able to convert Rust slice into jbytearray")
    } else {
        JObject::null().into_inner()
    }
}

pub(crate) fn multi_get(
    reader: &dyn Reader,
    _env: JNIEnv,
    _cf: JObject,
    _keys: jobjectArray
) -> jobject
{
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);
    let keys = java_array_to_vec_byte(&_env, _keys);

    let key_values = reader.multi_get_cf(
        cf,
        keys.iter().map(|k|k.as_slice()).collect::<Vec<_>>().as_slice()
    );
    map_to_java_map(&_env, &key_values)
}

pub(crate) fn is_empty(
    reader: &dyn Reader,
    _env: JNIEnv,
    _cf: JObject,
) -> jboolean
{
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    if let Ok(is_empty) = reader.is_empty_cf(cf){
        is_empty as jboolean
    } else {
        JNI_TRUE
    }
}

pub(crate) fn get_iter(
    reader: &dyn Reader,
    _env: JNIEnv,
    _cf: JObject,
    _mode: jint,
    _starting_key: jbyteArray,
    _direction: jint
) -> jobject
{
    let cf = unwrap_ptr::<ColumnFamily>(&_env, _cf);

    let starting_key = match parse_starting_key(&_env, _mode, _starting_key) {
        Ok(parsed_starting_key) => { parsed_starting_key }
        Err(e) => {
            throw!(
                    &_env, "java/lang/Exception",
                    format!("Cannot parse the iterator's starting key: {:?}", e).as_str(),
                    JObject::null().into_inner()
                );
        }
    };

    let mode = match parse_iterator_mode(_mode, starting_key.as_slice(), _direction) {
        Ok(parsed_mode) => { parsed_mode }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot parse the iterator's mode: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    };

    match reader.get_iter_cf_mode(cf, mode){
        Ok(iter) => {
            let db_iterator_class = _env.find_class("com/horizen/common/DBIterator")
                .expect("Should be able to find class DBIterator");
            create_java_object(&_env, &db_iterator_class, iter)
        }
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot get iterator for the specified column family: {:?}", e).as_str(),
                JObject::null().into_inner()
            )
        }
    }
}
