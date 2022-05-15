use jni::JNIEnv;
use jni::objects::{JObject, JClass, JValue, JMap};
use std::any::TypeId;
use crate::storage::Storage;
use rocksdb::{ColumnFamily, DBIterator};
use crate::storage::transaction::Transaction;
use std::collections::HashMap;
use jni::sys::{jobject, jlong, jobjectArray};
use jni::signature::JavaType;
use crate::common::storage::{ColumnFamiliesManager, DEFAULT_CF_NAME};
use crate::storage_versioned::StorageVersioned;
use crate::storage_versioned::transaction_versioned::TransactionVersioned;

#[macro_use]
pub mod exception;
pub mod iterator;
pub mod reader;
pub mod transaction_basic;
pub mod cf_manager;

fn read_raw_pointer<'a, T>(input: *const T) -> &'a T {
    assert!(!input.is_null());
    unsafe { &*input }
}

fn read_mut_raw_pointer<'a, T>(input: *mut T) -> &'a mut T {
    assert!(!input.is_null());
    unsafe { &mut *input }
}

fn get_field_name<'a, T: 'static>() -> &'a str {
    if TypeId::of::<T>() == TypeId::of::<Storage>(){
        "storagePointer"
    }
    else if TypeId::of::<T>() == TypeId::of::<Transaction>(){
        "transactionPointer"
    }
    else if TypeId::of::<T>() == TypeId::of::<StorageVersioned>(){
        "storageVersionedPointer"
    }
    else if TypeId::of::<T>() == TypeId::of::<TransactionVersioned>(){
        "transactionVersionedPointer"
    }
    else if TypeId::of::<T>() == TypeId::of::<ColumnFamily>(){
        "columnFamilyPointer"
    }
    else if TypeId::of::<T>() == TypeId::of::<DBIterator>(){
        "dbIteratorPointer"
    }
    else {
        panic!("Unknown type of a pointer")
    }
}

pub fn get_raw_ptr<T: 'static>(env: &JNIEnv, ptr: JObject) -> *mut T {
    let field_name = get_field_name::<T>();
    let fe = env.get_field(ptr, field_name, "J")
        .expect(&("Should be able to get field ".to_owned() + field_name));
    fe.j().unwrap() as *mut T
}

pub fn unwrap_ptr<'a, T: 'static>(env: &JNIEnv, ptr: JObject) -> &'a T {
    read_raw_pointer(get_raw_ptr(env, ptr))
}

pub fn unwrap_mut_ptr<'a, T: 'static>(env: &JNIEnv, ptr: JObject) -> &'a mut T {
    read_mut_raw_pointer(get_raw_ptr(env, ptr))
}

// Creates a wrapping Java-object (this is an object containing a pointer to Rust-object,
// which is passed as a single parameter to the Java-object's constructor)
pub fn create_java_object<T>(env: &JNIEnv, class: &JClass, rust_object: T) -> jobject {
    // Wrapping rust_object with a Box and getting a raw pointer as jlong
    let rust_object_ptr: jlong = jlong::from(
        Box::into_raw(Box::new(rust_object)) as i64
    );
    // Create and return new Java-object
    env.new_object(*class, "(J)V", &[JValue::Long(rust_object_ptr)])
        .expect("Should be able to create new Java-object")
        .into_inner()
}

// Creates a Transaction/TransactionVersioned Java-object when a transaction is created for Storage/StorageVersioned
// NOTE: Transaction/TransactionVersioned on the Java side need to have the Default ColumnFamily descriptor,
//       so the supplied Default CF is passed as a 2-nd argument to the Transaction/TransactionVersioned Java-constructor
pub fn create_transaction_java_object<T>(env: &JNIEnv, class: &JClass, transaction_object: T, default_cf_ref: &ColumnFamily) -> jobject {
    // Wrapping transaction_object with a Box and getting a raw pointer as jlong
    let transaction_object_ptr: jlong = jlong::from(
        Box::into_raw(Box::new(transaction_object)) as i64
    );
    // Converting the default_cf_ref into a raw pointer then converting the raw pointer into jlong
    let default_column_family_ptr: jlong = jlong::from(
        default_cf_ref as *const ColumnFamily as i64
    );
    // Create and return new Transaction Java-object with default CF in the 2-nd parameter
    env.new_object(*class, "(JJ)V",
                   &[JValue::Long(transaction_object_ptr), JValue::Long(default_column_family_ptr)])
        .expect("Should be able to create new Java-object with Default CF")
        .into_inner()
}

// Creates a TransactionVersioned Java-object when a transaction is created for a previous version of StorageVersioned
// NOTE: when TransactionVersioned is created for some previous version of a StorageVersioned,
//       it contains a separately opened instance of the RocksDB thus the Default ColumnFamily (as well as all other CFs)
//       should be retrieved for this DB instance, not for the DB instance residing in StorageVersioned.
//       So the Default CF descriptor is retrieved with the 'get_column_family' of the TransactionVersioned API
//       instead of using the externally supplied Default CF of StorageVersioned.
pub fn create_transaction_versioned_java_object(env: &JNIEnv, class: &JClass, transaction_object: TransactionVersioned) -> jobject {
    // Wrapping transaction_object with a Box and getting a raw pointer as jlong
    let transaction_object_ptr: jlong = jlong::from(
        Box::into_raw(Box::new(transaction_object)) as i64
    );
    // ColumnFamily reference should be taken from a moved into the Box object of TransactionVersioned
    let default_cf_ref = read_raw_pointer(transaction_object_ptr as *const TransactionVersioned)
        .get_column_family(DEFAULT_CF_NAME).expect("Should be able to call the 'get_column_family'")
        .expect("Should be able to get the default column family");
    // Converting the default_cf_ref into a raw pointer then converting the raw pointer into jlong
    let default_column_family_ptr: jlong = jlong::from(
        default_cf_ref as *const ColumnFamily as i64
    );
    // Create and return new Transaction Java-object with default CF in the 2-nd parameter
    env.new_object(*class, "(JJ)V",
                   &[JValue::Long(transaction_object_ptr), JValue::Long(default_column_family_ptr)])
        .expect("Should be able to create new Java-object with Default CF")
        .into_inner()
}

// Creates a Storage/StorageVersioned Java-object
// NOTE: Storage and StorageVersioned on the Java side need to have the Default ColumnFamily descriptor,
//       so it is passed as a 2-nd argument to the Storage/StorageVersioned Java-constructor
pub fn create_storage_java_object<T: ColumnFamiliesManager>(env: &JNIEnv, class: &JClass, storage_object: T) -> jobject {
    // Wrapping storage_object with a Box and getting a raw pointer as jlong
    let storage_object_ptr: jlong = jlong::from(
        Box::into_raw(Box::new(storage_object)) as i64
    );
    // ColumnFamily reference should be taken from a moved into the Box object of Storage or StorageVersioned
    let default_cf_ref = read_raw_pointer(storage_object_ptr as *const T)
        .get_column_family(DEFAULT_CF_NAME)
        .expect("Should be able to get the default column family");
    // Converting the default_cf_ref into a raw pointer then converting the raw pointer into jlong
    let default_column_family_ptr: jlong = jlong::from(
        default_cf_ref as *const ColumnFamily as i64
    );
    // Create and return new Storage Java-object with default CF in the 2-nd parameter
    env.new_object(*class, "(JJ)V",
                   &[JValue::Long(storage_object_ptr), JValue::Long(default_column_family_ptr)])
        .expect("Should be able to create new Java-object with Default CF")
        .into_inner()
}

// Creates a ColumnFamily Java-object
pub fn create_cf_java_object(env: &JNIEnv, cf_ref: &ColumnFamily, cf_name: &str) -> jobject {
    let column_family_class = env.find_class("com/horizen/common/ColumnFamily")
        .expect("Should be able to find class ColumnFamily");
    // Converting the cf_ref into a raw pointer then converting the raw pointer into jlong
    let column_family_ptr: jlong = jlong::from(
        cf_ref as *const ColumnFamily as i64
    );
    let cf_name_string = env.new_string(cf_name)
        .expect("Should be able to create String for CF name");

    // Create and return new Java-ColumnFamily
    env.new_object(column_family_class,
                   "(JLjava/lang/String;)V",
                   &[JValue::Long(column_family_ptr), cf_name_string.into()])
       .expect("Should be able to create ColumnFamily Java-object")
       .into_inner()
}

// Creates jobjectArray from specified vector of jobjects of specified class
pub fn create_jarray(env: &JNIEnv, obj_class: JClass, default_obj: jobject, objects: Vec<jobject>) -> jobjectArray {
    let java_array = env
        .new_object_array(objects.len() as i32, obj_class, default_obj)
        .expect("Should be able to create array of jobjects");

    for (i, obj) in objects.into_iter().enumerate() {
        env.set_object_array_element(java_array, i as i32, obj)
            .expect("Should be able to add object to java array");
    }
    java_array
}

// Creates a SimpleEntry Java-object (key-value container) containing a specified key-value pair
pub fn create_jentry(_env: &JNIEnv, key: &[u8], value: &[u8]) -> jobject {
    let jkey = _env
        .byte_array_from_slice(key)
        .expect("Cannot write Key to jbyteArray");

    let jvalue = _env
        .byte_array_from_slice(value)
        .expect("Cannot write Value to jbyteArray");

    let entry_class = _env
        .find_class("java/util/AbstractMap$SimpleEntry")
        .expect("Should be able to find AbstractMap.SimpleEntry class");

    let jentry = _env
        .new_object(entry_class,
                    "(Ljava/lang/Object;Ljava/lang/Object;)V",
                    &[jkey.into(), jvalue.into()])
        .expect("Should be able to create AbstractMap.SimpleEntry object");

    jentry.into_inner()
}

// Converts HashMap<Vec<u8>, Option<Vec<u8>>> to Java HashMap<byte[], Optional<byte[]>>
pub fn map_to_java_map(_env: &JNIEnv, hash_map: &HashMap<Vec<u8>, Option<Vec<u8>>>) -> jobject {
    let hash_map_class = _env
        .find_class("java/util/HashMap")
        .expect("Should be able to find HashMap class");

    let jhash_map = _env
        .new_object(hash_map_class, "()V", &[])
        .expect("Should be able to create HashMap object");

    let put = _env.get_method_id(
        hash_map_class,
        "put",
        "(Ljava/lang/Object;Ljava/lang/Object;\
             )Ljava/lang/Object;",
    ).expect("Should be able to get the 'put' method ID of HashMap object");

    let otional_class = _env
        .find_class("java/util/Optional")
        .expect("Should be able to find Optional class");

    hash_map.iter().for_each(|kv|{
        let jkey = _env
            .byte_array_from_slice(kv.0.as_slice())
            .expect("Cannot write Key to jbyteArray");

        let jvalue_opt = {
            if let Some(value) = kv.1 {
                let jvalue = _env
                    .byte_array_from_slice(value.as_slice())
                    .expect("Cannot write Value to jbyteArray");

                _env.call_static_method(
                    otional_class,
                    "of",
                    "(Ljava/lang/Object;)Ljava/util/Optional;",
                    &[JValue::from(jvalue)],
                ).expect("Should be able to create new value for Optional")
            } else { // None
                _env.call_static_method(
                    otional_class,
                    "empty",
                    "()Ljava/util/Optional;",
                    &[]
                ).expect("Should be able to create empty value for Optional.empty()")
            }
        };

        _env.call_method_unchecked(
            jhash_map,
            put,
            JavaType::Object("java/lang/Object".into()),
            vec![JValue::from(jkey), jvalue_opt].as_slice()
        ).expect("Should be able to call the 'put' method of HashMap object");
    });

    jhash_map.into_inner()
}

// Converts Map<byte[], byte[]> to Vec<(Vec<u8>, Vec<u8>)>
pub fn java_map_to_vec_byte(_env: &JNIEnv, _map: JObject) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if let Ok(to_update_jmap) = JMap::from_env(&_env, _map){
        if let Ok(iter) = to_update_jmap.iter(){
            Some(
                iter.map(|kv|{
                    let key = _env
                        .convert_byte_array(kv.0.cast())
                        .expect("Should be able to convert Key to Rust byte array");

                    let value = _env
                        .convert_byte_array(kv.1.cast())
                        .expect("Should be able to convert Value to Rust byte array");
                    (key, value)
                }).collect::<Vec<_>>()
            )
        } else {
            None
        }
    } else {
        None
    }
}

// Converts Java byte[][] to Vec<Vec<u8>>
pub fn java_array_to_vec_byte(_env: &JNIEnv, java_array: jobjectArray) -> Vec<Vec<u8>> {
    let java_array_size = _env
        .get_array_length(java_array)
        .expect("Should be able to get custom_fields size");

    if java_array_size > 0 {
        (0.. java_array_size).map(|i|{
            let jobj = _env
                .get_object_array_element(java_array, i)
                .unwrap_or_else(|_| panic!("Should be able to get elem {} of java_array", i));

            let vec = _env.convert_byte_array(jobj.cast())
                .expect("Should be able to convert to Rust byte array");
            vec
        }).collect::<Vec<Vec<u8>>>()
    } else {
        vec![]
    }
}
