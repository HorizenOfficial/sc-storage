package com.horizen.storageVersioned;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.DefaultReader;
import com.horizen.common.interfaces.DefaultTransactionBasic;
import com.horizen.librust.Library;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.horizen.common.interfaces.ColumnFamilyManager.DEFAULT_CF_NAME;

public class TransactionVersioned implements DefaultReader, DefaultTransactionBasic, AutoCloseable {

    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long transactionVersionedPointer;
    final private ColumnFamily defaultCf;

    public void checkPointer() throws IllegalStateException {
        if (transactionVersionedPointer == 0)
            throw new IllegalStateException("Transaction instance was freed");
    }

    // Gates to the Rust-side API
    private static native void nativeClose(long transactionPointer);
    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native void nativeCommit(String versionId) throws Exception;
    private native void nativeUpdate(ColumnFamily cf, Map<byte[], byte[]> toUpdate, byte[][] toDelete) throws Exception;
    private native void nativeSave() throws Exception;
    private native void nativeRollbackToSavepoint() throws Exception;
    private native void nativeRollback() throws Exception;
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;
    private native ColumnFamily nativeGetColumnFamily(String cf_name) throws Exception;

    // Constructor is intended to be called from inside the Rust environment for setting a raw pointer to a Rust-instance of Transaction
    private TransactionVersioned(long transactionVersionedPointer, long defaultColumnFamilyPointer) {
        this.transactionVersionedPointer = transactionVersionedPointer;
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer, DEFAULT_CF_NAME);
    }

    // Closes transaction (frees Rust memory from Transaction object)
    public void closeTransaction() {
        if (transactionVersionedPointer != 0) {
            nativeClose(this.transactionVersionedPointer);
            transactionVersionedPointer = 0;
        }
    }

    @Override
    public void close() {
        closeTransaction();
    }

    @Override
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

    // Retrieves Key-Value pairs for a specified list of keys in a specified column family.
    // For the absent keys the values in corresponding Key-Value pairs are Optional.empty()
    public Map<byte[], Optional<byte[]>> get(ColumnFamily cf, Set<byte[]> keys){
        checkPointer();
        return nativeMultiGet(cf, keys.toArray(new byte[0][0]));
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

    // Commits all transaction's updates into the related StorageVersioned
    // and creates a new version (checkpoint) of a StorageVersioned with the 'versionId' identifier
    // TransactionVersioned started for a previous version of a StorageVersioned can't be committed due to
    // all saved storage versions should remain unchanged
    // Throws Exception with an error message if some error occurred
    public void commit(String versionId) throws Exception {
        checkPointer();
        nativeCommit(versionId);
    }

    // Performs the specified insertions ('toUpdate' vector of Key-Values) and removals ('toDelete' vector of Keys)
    // for a specified column family 'cf' in a current transaction
    // Throws Exception with error message if any error occurred
    public void update(ColumnFamily cf, Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception {
        checkPointer();
        nativeUpdate(cf, toUpdate, toDelete.toArray(new byte[0][0]));
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

    // Method for retrieving column families handles when transaction is started for a version of storage;
    // If transaction is started for the CurrentState of storage, then throws exception and the corresponding method of a StorageVersioned should be used instead.
    // If transaction is started for some version of StorageVersioned then returns:
    //  - handle for a specified by 'cfName' column family name;
    //  - Optional.empty() if column family with a specified name is absent in the opened version of storage.
    public Optional<ColumnFamily> getColumnFamily(String cfName) throws Exception {
        checkPointer();
        ColumnFamily cf = nativeGetColumnFamily(cfName);
        if(cf != null){
            return Optional.of(cf);
        } else {
            return Optional.empty();
        }
    }
}
