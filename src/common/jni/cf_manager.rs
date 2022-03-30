use jni::JNIEnv;
use jni::objects::{JObject, JString};
use crate::common::jni::exception::_throw_inner;
use crate::common::storage::ColumnFamiliesManager;
use jni::sys::jobject;
use crate::common::jni::create_cf_java_object;

pub(crate) fn set_column_family(
    cf_manager: &mut dyn ColumnFamiliesManager,
    _env: JNIEnv,
    _cf_name: JString
){
    let cf_name = _env
        .get_string(_cf_name)
        .expect("Should be able to read _cf_name jstring as JavaStr");

    match cf_manager.set_column_family(cf_name.to_str().expect("Should be able to convert the cf_name to Rust String")) {
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
        .expect("Should be able to read _cf_name jstring as JavaStr");

    if let Some(cf_ref) =
        cf_manager.get_column_family(
            cf_name.to_str()
                .expect("Should be able to convert the cf_name to Rust String"))
    {
        create_cf_java_object(&_env, cf_ref)
    } else {
        JObject::null().into_inner()
    }
}
