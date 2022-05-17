use jni::JNIEnv;
use jni::objects::{JClass, JObject};
use rocksdb::{DBIterator, IteratorMode, Error, Direction};
use jni::sys::{jobject, jbyteArray};
use crate::common::jni::{unwrap_mut_ptr, create_jentry};

const ITER_MODE_START: i32 = 0;
const ITER_MODE_END: i32 = 1;
const ITER_MODE_FROM: i32 = 2;

const ITER_DIRECTION_FORWARD: i32 = 0;
const ITER_DIRECTION_REVERSE: i32 = 1;

pub(crate) fn parse_starting_key(env: &JNIEnv, mode: i32, jstarting_key: jbyteArray) -> Result<Vec<u8>, Error> {
    if jstarting_key.is_null(){
        if mode == ITER_MODE_FROM {
            Err(Error::new("Starting key should be specified for the iterator's mode 'From'".into()))
        } else {
            Ok(vec![])
        }
    } else {
        Ok(env.convert_byte_array(jstarting_key)
            .expect("Should be able to convert starting key to Rust byte array"))
    }
}

pub(crate) fn parse_iterator_mode(mode: i32,
                                  starting_key: &[u8],
                                  direction: i32) -> Result<IteratorMode, Error> {
    match mode {
        ITER_MODE_START => { Ok(IteratorMode::Start) }
        ITER_MODE_END => { Ok(IteratorMode::End) }
        ITER_MODE_FROM => {
            match direction {
                ITER_DIRECTION_FORWARD => { Ok(IteratorMode::From(starting_key, Direction::Forward)) }
                ITER_DIRECTION_REVERSE => { Ok(IteratorMode::From(starting_key, Direction::Reverse)) }
                _ => { Err(Error::new(format!("Invalid iterator's direction: {:?}", direction).into())) }
            }
        }
        _ => { Err(Error::new(format!("Invalid iterator's mode: {:?}", mode).into())) }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_common_DBIterator_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _iterator: *mut DBIterator,
){
    if !_iterator.is_null(){
        drop(unsafe { Box::from_raw(_iterator) })
    }
}

#[no_mangle]
pub extern "system" fn Java_com_horizen_common_DBIterator_nativeNext(
    _env: JNIEnv,
    _iterator: JObject,
) -> jobject
{
    let iterator = unwrap_mut_ptr::<DBIterator>(&_env, _iterator);
    if let Some((key, value)) = iterator.next(){
        create_jentry(&_env, key.as_ref(), value.as_ref())
    } else {
        JObject::null().into_inner()
    }
}
