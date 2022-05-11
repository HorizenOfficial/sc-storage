package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.*;
import com.horizen.librust.Library;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Transaction implements DefaultReader, DefaultTransactionBasic, AutoCloseable {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long transactionPointer;
    final private ColumnFamily defaultCf;

    public void checkPointer() throws IllegalStateException {
        if (transactionPointer == 0)
            throw new IllegalStateException("Transaction instance was freed");
    }

    // Gates to the Rust-side API
    private static native void nativeClose(long transactionPointer);
    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native void nativeCommit() throws Exception;
    private native void nativeUpdate(ColumnFamily cf, Map<byte[], byte[]> toUpdate, byte[][] toDelete) throws Exception;
    private native void nativeSave() throws Exception;
    private native void nativeRollbackToSavepoint() throws Exception;
    private native void nativeRollback() throws Exception;
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of Transaction
    private Transaction(long transactionPointer, long defaultColumnFamilyPointer) {
        this.transactionPointer = transactionPointer;
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer);
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
        closeTransaction();
    }

    public ColumnFamily defaultCf() {
        return defaultCf;
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

    public Map<byte[], Optional<byte[]>> get(ColumnFamily cf, Set<byte[]> keys){
        checkPointer();
        return nativeMultiGet(cf, keys.toArray(new byte[0][0]));
    }

    public byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue){
        return get(cf, key).orElse(defaultValue);
    }

    public boolean isEmpty(ColumnFamily cf) {
        checkPointer();
        return nativeIsEmpty(cf);
    }

    public DBIterator getIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'Start' mode
        return nativeGetIter(cf, DBIterator.Mode.Start, null, 0);
    }

    public DBIterator getRIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'End' mode
        return nativeGetIter(cf, DBIterator.Mode.End, null, 0);
    }

    public DBIterator getIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Forward);
    }

    public DBIterator getRIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Reverse);
    }

    public void commit() throws Exception {
        checkPointer();
        nativeCommit();
    }
    public void update(ColumnFamily cf, Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception {
        checkPointer();
        nativeUpdate(cf, toUpdate, toDelete.toArray(new byte[0][0]));
    }

    public void save() throws Exception {
        checkPointer();
        nativeSave();
    }

    public void rollbackToSavepoint() throws Exception {
        checkPointer();
        nativeRollbackToSavepoint();
    }

    public void rollback() throws Exception {
        checkPointer();
        nativeRollback();
    }
}
