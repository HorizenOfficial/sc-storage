package com.horizen.common;

import com.horizen.librust.Library;

import java.util.AbstractMap;
import java.util.Optional;

public class DBIterator implements AutoCloseable {

    // Constants specifying the Mode and the Direction of DBIterator which should be the same as on the Rust side of JNI
    // in 'sc_storage/src/common/jni/iterator.rs'
    public static class Mode {
        public static int Start = 0;
        public static int End = 1;
        public static int From = 2;
    }
    public static class Direction {
        public static int Forward = 0;
        public static int Reverse = 1;
    }

    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long dbIteratorPointer;

    public void checkPointer() throws IllegalStateException {
        if (dbIteratorPointer == 0)
            throw new IllegalStateException("DBIterator instance was freed");
    }

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of DBIterator
    private DBIterator(long dbIteratorPointer) {
        this.dbIteratorPointer = dbIteratorPointer;
    }

    private static native void nativeClose(long dbIteratorPointer);
    private native AbstractMap.SimpleEntry<byte[], byte[]> nativeNext();

    // Closes a storage (frees Rust memory from DBIterator object)
    public void closeDBIterator() {
        if (dbIteratorPointer != 0) {
            nativeClose(this.dbIteratorPointer);
            dbIteratorPointer = 0;
        }
    }

    @Override
    public void close() {
        closeDBIterator();
    }

    // Returns a next Key-Value entry or Optional.empty() if there are no more entries
    public Optional<AbstractMap.SimpleEntry<byte[], byte[]>> next(){
        checkPointer();
        AbstractMap.SimpleEntry<byte[], byte[]> kv = nativeNext();
        if(kv != null){
            return Optional.of(kv);
        } else {
            return Optional.empty();
        }
    }
}
