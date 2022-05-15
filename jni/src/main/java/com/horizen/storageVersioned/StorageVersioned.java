package com.horizen.storageVersioned;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.ColumnFamilyManager;
import com.horizen.common.interfaces.DefaultReader;
import com.horizen.librust.Library;

import java.util.*;

public class StorageVersioned implements DefaultReader, ColumnFamilyManager, AutoCloseable {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long storageVersionedPointer;
    private ColumnFamily defaultCf;

    public void checkPointer() throws IllegalStateException {
        if (storageVersionedPointer == 0)
            throw new IllegalStateException("StorageVersioned instance was freed");
    }

    // Constructor is intended to be called from inside the Rust environment for setting a raw pointer to a Rust-instance of Storage
    private StorageVersioned(long storageVersionedPointer, long defaultColumnFamilyPointer) {
        this.storageVersionedPointer = storageVersionedPointer;
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer, DEFAULT_CF_NAME);
    }

    // Gates to the Rust-side API
    private static native StorageVersioned nativeOpen(String storagePath, boolean createIfMissing, int versionsStored) throws Exception;
    private static native void nativeClose(long storagePointer);

    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native TransactionVersioned nativeCreateTransaction(String versionId);
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;
    private native ColumnFamily nativeGetColumnFamily(String cf_name);
    private native void nativeSetColumnFamily(String cf_name) throws Exception;
    private native void nativeRollback(String version_id) throws Exception;
    private native String[] nativeRollbackVersions() throws Exception;
    private native String nativeLastVersion() throws Exception;

    // Opens a storage located by a specified 'storagePath' or creates a new one if the directory by a specified path doesn't exist and the 'create_if_missing' flag is true
    // The 'versionsStored' parameter specifies how many latest versions (0 or more) should be stored for a storage.
    // If at the moment of opening of an existing storage there are more saved versions than 'versions_stored' specifies, then the oldest versions will be removed.
    // Returns StorageVersioned instance or throws Exception with a describing message if some error occurred
    public static StorageVersioned open(String storagePath, boolean createIfMissing, int versionsStored) throws Exception {
        return nativeOpen(storagePath, createIfMissing, versionsStored);
    }

    // Checks if Storage is correctly opened
    public boolean isOpened(){
        return storageVersionedPointer != 0;
    }

    // Closes storage (frees Rust memory from Storage object)
    public void closeStorage() {
        if (storageVersionedPointer != 0) {
            nativeClose(this.storageVersionedPointer);
            storageVersionedPointer = 0;
        }
    }

    @Override
    public void close() {
        closeStorage();
    }

    // Returns the default column family
    public ColumnFamily defaultCf() {
        return defaultCf;
    }

    // Retrieves a value for a specified key in a specified column family
    // from an underlying storage or returns Optional.empty() in case the key is absent
    public Optional<byte[]> get(ColumnFamily cf, byte[] key){
        checkPointer();
        byte[] value = nativeGet(cf, key);
        if(value != null){
            return Optional.of(value);
        } else {
            return Optional.empty();
        }
    }

    // Retrieves Key-Value pairs for a specified list of keys in a specified column family from an underlying storage.
    // For the absent keys the values in corresponding Key-Value pairs are Optional.empty()
    public Map<byte[], Optional<byte[]>> get(ColumnFamily cf, Set<byte[]> keys){
        checkPointer();
        return nativeMultiGet(cf, keys.toArray(new byte[0][0]));
    }

    // Retrieves a value for a specified key in a specified column family
    // from an underlying storage or returns 'defaultValue' in case the key is absent
    public byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue){
        return get(cf, key).orElse(defaultValue);
    }

    // Checks whether an underlying storage contains any Key-Value pairs in a specified column family
    public boolean isEmpty(ColumnFamily cf) {
        checkPointer();
        return nativeIsEmpty(cf);
    }

    // Creates a transaction for a specified previous version of the storage in 'versionIdOpt',
    // or for a current state of the storage if 'versionIdOpt' is 'Optional.empty().
    // Returns Optional.of(TransactionVersioned) when transaction is created
    // or 'Optional.empty()' if transaction can't be created
    public Optional<TransactionVersioned> createTransaction(Optional<String> versionIdOpt){
        checkPointer();
        String versionId = null;
        if (versionIdOpt.isPresent()){
            versionId = versionIdOpt.get();
        }
        TransactionVersioned transaction = nativeCreateTransaction(versionId);
        if(transaction != null){
            return Optional.of(transaction);
        } else {
            return Optional.empty();
        }
    }

    // Returns forward iterator for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    public DBIterator getIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'Start' mode
        return nativeGetIter(cf, DBIterator.Mode.Start, null, 0);
    }

    // Returns reverse iterator for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    public DBIterator getRIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'End' mode
        return nativeGetIter(cf, DBIterator.Mode.End, null, 0);
    }

    // Returns forward iterator starting from a specified key for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    public DBIterator getIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Forward);
    }

    // Returns reverse iterator starting from a specified key for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    public DBIterator getRIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, startingKey, DBIterator.Direction.Reverse);
    }

    // Returns a handle for a specified column family name
    // Returns Optional.empty() if column family with a specified name is absent in storage
    public Optional<ColumnFamily> getColumnFamily(String cf_name){
        checkPointer();
        ColumnFamily cf = nativeGetColumnFamily(cf_name);
        if(cf != null){
            return Optional.of(cf);
        } else {
            return Optional.empty();
        }
    }

    // Creates column family with a specified name
    // Successfully returns if column family was created successfully or already exists
    // Throws Exception with describing message if any error occurred during column family creation
    public void setColumnFamily(String cf_name) throws Exception {
        checkPointer();
        nativeSetColumnFamily(cf_name);
    }

    // Rollbacks current state of the storage to a specified with 'version_id' previous version.
    // All saved versions after the 'version_id' are deleted if rollback is successful.
    // Throws Exception with error message if some error occurs
    public void rollback(String version_id) throws Exception {
        checkPointer();
        nativeRollback(version_id);
        // Re-initializing the default CF's descriptor;
        // NOTE: Default CF should be always existing in an underlying storage,
        //       so there is no need to check a returned value with 'isPresent'
        defaultCf = getColumnFamily(DEFAULT_CF_NAME).get();
    }

    // Returns a sorted by creation order list of all existing versions' IDs
    // Throws Exception with error message if some error occurs
    public List<String> rollbackVersions() throws Exception {
        checkPointer();
        return new ArrayList<>(Arrays.asList(nativeRollbackVersions()));
    }

    // Returns ID of the most recent version among all saved versions of the storage
    // Throws Exception with error message if some error occurs
    public Optional<String> lastVersion() throws Exception {
        checkPointer();
        String lastVersion = nativeLastVersion();
        if(lastVersion != null){
            return Optional.of(lastVersion);
        } else {
            return Optional.empty();
        }
    }
}
