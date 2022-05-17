package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.*;
import com.horizen.librust.Library;

import java.util.List;
import java.util.Optional;

import static com.horizen.common.interfaces.ColumnFamilyManager.DEFAULT_CF_NAME;

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
    private native List<byte[]> nativeMultiGet(ColumnFamily cf, List<byte[]> keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native void nativeCommit() throws Exception;
    private native void nativeUpdate(ColumnFamily cf, List<byte[]> keysToUpdate, List<byte[]> valuesToUpdate, List<byte[]> keysToDelete) throws Exception;
    private native void nativeSave() throws Exception;
    private native void nativeRollbackToSavepoint() throws Exception;
    private native void nativeRollback() throws Exception;
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of Transaction
    private Transaction(long transactionPointer, long defaultColumnFamilyPointer) {
        this.transactionPointer = transactionPointer;
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer, DEFAULT_CF_NAME);
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

    // Retrieves a value for a specified key in a specified column family
    // or returns Optional.empty() in case the key is absent
    public Optional<byte[]> get(ColumnFamily cf, byte[] key){
        checkPointer();
        byte[] value = nativeGet(cf, key);
        if(value != null){
            return Optional.of(value);
        } else {
            return Optional.empty();
        }
    }

    // Retrieves the values correspondingly to a specified list of keys in a specified column family
    // For the absent keys the values in the corresponding positions are null
    public List<byte[]> get(ColumnFamily cf, List<byte[]> keys){
        checkPointer();
        return nativeMultiGet(cf, keys);
    }

    // Retrieves a value for a specified key in a specified column family
    // or returns 'defaultValue' in case the key is absent
    public byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue){
        return get(cf, key).orElse(defaultValue);
    }

    // Checks whether a transaction contains any Key-Value pairs in a specified column family
    public boolean isEmpty(ColumnFamily cf) {
        checkPointer();
        return nativeIsEmpty(cf);
    }

    // Returns forward iterator for all contained keys in a specified column family
    // Throws Exception with error message if any error occurred
    public DBIterator getIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'Start' mode
        return nativeGetIter(cf, DBIterator.Mode.Start, null, 0);
    }

    // Returns reverse iterator for all contained keys in a specified column family
    // Throws Exception with error message if any error occurred
    public DBIterator getRIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'End' mode
        return nativeGetIter(cf, DBIterator.Mode.End, null, 0);
    }

    // Returns forward iterator starting from a specified key for all contained keys in a specified column family
    // Throws Exception with error message if any error occurred
    public DBIterator getIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Forward);
    }

    // Returns reverse iterator starting from a specified key for all contained keys in a specified column family
    // Throws Exception with error message if any error occurred
    public DBIterator getRIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Reverse);
    }

    // Commits all Transaction's updates into the related Storage
    // Throws Exception with error message if any error occurred
    public void commit() throws Exception {
        checkPointer();
        nativeCommit();
    }

    // Performs the specified insertions ('keysToUpdate' and 'valuesToUpdate' vectors of Keys and corresponding to them Values)
    // and removals ('keysToDelete' vector of Keys) for a specified column family 'cf' in a current transaction
    // Throws Exception with error message if any error occurred
    public void update(ColumnFamily cf, List<byte[]> keysToUpdate, List<byte[]> valuesToUpdate, List<byte[]> keysToDelete) throws Exception {
        checkPointer();
        nativeUpdate(cf, keysToUpdate, valuesToUpdate, keysToDelete);
    }

    // Saves the current state of a transaction to which it can be rolled back later
    public void save() throws Exception {
        checkPointer();
        nativeSave();
    }

    // Rolls back the current state of a transaction to the most recent savepoint.
    // Can be performed sequentially thus restoring previous savepoints in LIFO order.
    public void rollbackToSavepoint() throws Exception {
        checkPointer();
        nativeRollbackToSavepoint();
    }

    // Rolls back transaction to the initial state (state at the moment when transaction was started)
    public void rollback() throws Exception {
        checkPointer();
        nativeRollback();
    }
}
