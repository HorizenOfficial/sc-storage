package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.ColumnFamilyManager;
import com.horizen.common.interfaces.DefaultReader;
import com.horizen.librust.Library;

import java.util.List;
import java.util.Optional;

public class Storage implements DefaultReader, ColumnFamilyManager, AutoCloseable {

    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private long storagePointer;
    final private ColumnFamily defaultCf;

    public void checkPointer() throws IllegalStateException {
        if (storagePointer == 0)
            throw new IllegalStateException("Storage instance was freed");
    }

    // Constructor is intended to be called from inside the Rust environment for setting a raw pointer to a Rust-instance of Storage
    private Storage(long storagePointer, long defaultColumnFamilyPointer) {
        this.storagePointer = storagePointer;
        this.defaultCf = new ColumnFamily(defaultColumnFamilyPointer, DEFAULT_CF_NAME);
    }

    // Gates to the Rust-side API
    private static native Storage nativeOpen(String storagePath, boolean createIfMissing) throws Exception;
    private static native void nativeClose(long storagePointer);

    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native List<byte[]> nativeMultiGet(ColumnFamily cf, List<byte[]> keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native Transaction nativeCreateTransaction() throws Exception;
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;
    private native ColumnFamily nativeGetColumnFamily(String cf_name);
    private native void nativeSetColumnFamily(String cf_name) throws Exception;

    // Opens a storage located by a specified path or creates a new one
    // if the directory by a specified path doesn't exist and 'createIfMissing' is true
    // Returns Storage instance or throws Exception with a describing message if some error occurred
    public static Storage open(String storagePath, boolean createIfMissing) throws Exception {
        return nativeOpen(storagePath, createIfMissing);
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
        closeStorage();
    }

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

    // Retrieves the values correspondingly to a specified list of keys in a specified column family from an underlying storage.
    // For the absent keys the values in the corresponding positions are null
    public List<byte[]> get(ColumnFamily cf, List<byte[]> keys){
        checkPointer();
        return nativeMultiGet(cf, keys);
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

    // Creates and returns a Transaction
    // Throws Exception with error message if any error occurred
    public Transaction createTransaction() throws Exception{
        checkPointer();
        return nativeCreateTransaction();
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
}
