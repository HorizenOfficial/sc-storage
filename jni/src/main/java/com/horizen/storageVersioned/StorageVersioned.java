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
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer);
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

    public DBIterator getIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'Start' mode
        return nativeGetIter(cf, DBIterator.Mode.Start, null, 0);
    }

    public DBIterator getRIter(ColumnFamily cf) throws Exception {
        // The 'starting_key', and 'direction' parameters are ignored for the 'End' mode
        return nativeGetIter(cf, DBIterator.Mode.End, null, 0);
    }

    public DBIterator getIterFrom(ColumnFamily cf, byte[] starting_key) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, starting_key, DBIterator.Direction.Forward);
    }

    public DBIterator getRIterFrom(ColumnFamily cf, byte[] starting_key) throws Exception {
        return nativeGetIter(cf, DBIterator.Mode.From, starting_key, DBIterator.Direction.Reverse);
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

    public void setColumnFamily(String cf_name) throws Exception {
        checkPointer();
        nativeSetColumnFamily(cf_name);
    }

    public void rollback(String version_id) throws Exception {
        checkPointer();
        nativeRollback(version_id);
        // Re-initializing the default CF's descriptor;
        // NOTE: Default CF should be always existing in an underlying storage,
        //       so there is no need to check a returned value with 'isPresent'
        defaultCf = getColumnFamily(DEFAULT_CF_NAME).get();
    }

    public List<String> rollbackVersions() throws Exception {
        checkPointer();
        return new ArrayList<>(Arrays.asList(nativeRollbackVersions()));
    }

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
