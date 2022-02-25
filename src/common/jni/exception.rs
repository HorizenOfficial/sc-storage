use jni::JNIEnv;

pub(crate) fn _throw_inner(env: &JNIEnv, exception: &str, description: &str) {
    // Do nothing if there is a pending Java-exception that will be thrown
    // automatically by the JVM when the native method returns.
    if !env.exception_check().unwrap() {
        let exception_class = env
            .find_class(exception)
            .unwrap_or_else(|_| panic!("Unable to find {} class", exception));

        env.throw_new(exception_class, description)
            .unwrap_or_else(|_| panic!("Should be able to throw {}", exception));
    }
}

/// Throws exception and exits from the function from within this macro is called
/// returning $default or nothing (if the function returns void)
macro_rules! throw {
    ($env:expr, $exception:expr, $description:expr, $default: expr) => {{
        _throw_inner($env, $exception, $description);
        return $default;
    }};

    ($env:expr, $exception:expr, $description:expr) => {{
        _throw_inner($env, $exception, $description);
        return;
    }};
}
