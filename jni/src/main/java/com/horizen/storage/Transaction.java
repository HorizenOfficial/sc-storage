package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.librust.Library;

import java.util.Map;
import java.util.Optional;

public class Transaction implements AutoCloseable {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long transactionPointer;

    public void checkPointer() throws IllegalStateException {
        if (transactionPointer == 0)
            throw new IllegalStateException("Transaction instance was freed");
    }

    // Gates to the Rust-side API
    private static native void nativeClose(long transactionPointer);
    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native boolean nativeCommit();
    private native boolean nativeUpdate(ColumnFamily cf, Map<byte[], byte[]> toUpdate, byte[][] toDelete);
    private native boolean nativeSave();
    private native boolean nativeRollbackToSavepoint();
    private native boolean nativeRollback();

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of Transaction
    private Transaction(long transactionPointer) {
        this.transactionPointer = transactionPointer;
    }

    // Closes transaction (frees Rust memory from Transaction object)
    public void closeTransaction() {
        if (transactionPointer != 0) {
            nativeClose(this.transactionPointer);
            transactionPointer = 0;
        }
    }

    @Override
    public void close() {
        checkPointer();
        closeTransaction();
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

    public boolean commit(){
        checkPointer();
        return nativeCommit();
    }
    public boolean update(ColumnFamily cf, Map<byte[], byte[]> toUpdate, byte[][] toDelete){
        checkPointer();
        return nativeUpdate(cf, toUpdate, toDelete);
    }

    public boolean save(){
        checkPointer();
        return nativeSave();
    }

    public boolean rollbackToSavepoint(){
        checkPointer();
        return nativeRollbackToSavepoint();
    }

    public boolean rollback(){
        checkPointer();
        return nativeRollback();
    }
}
