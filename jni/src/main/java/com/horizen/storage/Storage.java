package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.librust.Library;

import java.util.Map;
import java.util.Optional;

public class Storage implements AutoCloseable {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long storagePointer;

    public void checkPointer() throws IllegalStateException {
        if (storagePointer == 0)
            throw new IllegalStateException("Storage instance was freed");
    }

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of Storage
    private Storage(long storagePointer) {
        this.storagePointer = storagePointer;
    }

    // Gates to the Rust-side API
    private static native Storage nativeOpen(String storagePath, boolean createIfMissing);
    private static native void nativeClose(long storagePointer);

    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native Transaction nativeCreateTransaction();
    private native ColumnFamily nativeGetColumnFamily(String cf_name);
    private native boolean nativeSetColumnFamily(String cf_name);

    public static Optional<Storage> open(String storagePath, boolean createIfMissing) {
        Storage storage = nativeOpen(storagePath, createIfMissing);
        if(storage != null){
            return Optional.of(storage);
        } else {
            return Optional.empty();
        }
    }

    // Checks if Storage is correctly opened
    public boolean isOpened(){
        return storagePointer != 0;
    }

    // Closes storage (frees Rust memory from Storage object)
    public void closeStorage() {
        if (storagePointer != 0) {
            nativeClose(this.storagePointer);
            storagePointer = 0;
        }
    }

    @Override
    public void close() {
        checkPointer();
        closeStorage();
    }

    public Optional<byte[]> get(ColumnFamily cf, byte[] key){
        checkPointer();
        byte[] value = nativeGet(cf, key);
        if(value != null){
            return Optional.of(value);
        } else {
            return Optional.empty();
        }
    }

    public Map<byte[], Optional<byte[]>> get(ColumnFamily cf, byte[][] keys){
        checkPointer();
        return nativeMultiGet(cf, keys);
    }

    public byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue){
        return get(cf, key).orElse(defaultValue);
    }

    public boolean isEmpty(ColumnFamily cf) {
        checkPointer();
        return nativeIsEmpty(cf);
    }

    public Optional<Transaction> createTransaction(){
        checkPointer();
        Transaction transaction = nativeCreateTransaction();
        if(transaction != null){
            return Optional.of(transaction);
        } else {
            return Optional.empty();
        }
    }

    public Optional<ColumnFamily> getColumnFamily(String cf_name){
        checkPointer();
        ColumnFamily cf = nativeGetColumnFamily(cf_name);
        if(cf != null){
            return Optional.of(cf);
        } else {
            return Optional.empty();
        }
    }

    public boolean setColumnFamily(String cf_name){
        checkPointer();
        return nativeSetColumnFamily(cf_name);
    }
}
