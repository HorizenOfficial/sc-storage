use jni::JNIEnv;
use jni::objects::{JObject, JString, JValue};
use crate::common::jni::exception::_throw_inner;
use crate::common::storage::ColumnFamiliesManager;
use jni::sys::{jobject, jlong};
use rocksdb::ColumnFamily;

pub(crate) fn set_column_family(
    cf_manager: &mut dyn ColumnFamiliesManager,
    _env: JNIEnv,
    _cf_name: JString
){
    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read jstring as Rust String");

    match cf_manager.set_column_family(cf_name.to_str().unwrap()) {
        Ok(()) => {}
        Err(e) => {
            throw!(
                &_env, "java/lang/Exception",
                format!("Cannot set column family: {:?}", e).as_str()
            )
        }
    }
}

pub(crate) fn get_column_family(
    cf_manager: &dyn ColumnFamiliesManager,
    _env: JNIEnv,
    _cf_name: JString
) -> jobject
{
    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read jstring as Rust String");

    if let Some(cf_ref) = cf_manager.get_column_family(cf_name.to_str().unwrap()){
        let column_family_class = _env.find_class("com/horizen/common/ColumnFamily")
            .expect("Should be able to find class ColumnFamily");
        // Converting the cf_ref into a raw pointer then converting the raw pointer into jlong
        let column_family_ptr: jlong = jlong::from(
            cf_ref as *const ColumnFamily as i64
        );
        // Create and return new Java-ColumnFamily
        _env.new_object(column_family_class, "(J)V", &[JValue::Long(column_family_ptr)])
            .expect("Should be able to create ColumnFamily Java-object")
            .into_inner()
    } else {
        JObject::null().into_inner()
    }
}
