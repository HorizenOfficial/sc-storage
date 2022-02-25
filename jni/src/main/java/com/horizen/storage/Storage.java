package com.horizen.storage;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;
import com.horizen.common.interfaces.ColumnFamilyManager;
import com.horizen.common.interfaces.Reader;
import com.horizen.librust.Library;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Storage implements Reader, ColumnFamilyManager, AutoCloseable {

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
    private static native Storage nativeOpen(String storagePath, boolean createIfMissing) throws Exception;
    private static native void nativeClose(long storagePointer);

    private native byte[] nativeGet(ColumnFamily cf, byte[] key);
    private native Map<byte[], Optional<byte[]>> nativeMultiGet(ColumnFamily cf, byte[][] keys);
    private native boolean nativeIsEmpty(ColumnFamily cf);
    private native Transaction nativeCreateTransaction();
    private native DBIterator nativeGetIter(ColumnFamily cf, int mode, byte[] starting_key, int direction) throws Exception;
    private native ColumnFamily nativeGetColumnFamily(String cf_name);
    private native boolean nativeSetColumnFamily(String cf_name) throws Exception;

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

    public Optional<Transaction> createTransaction(){
        checkPointer();
        Transaction transaction = nativeCreateTransaction();
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

    public boolean setColumnFamily(String cf_name) throws Exception {
        checkPointer();
        return nativeSetColumnFamily(cf_name);
    }
}
