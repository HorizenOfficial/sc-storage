package com.horizen;

import com.horizen.common.ColumnFamily;
import com.horizen.storage.Storage;
import com.horizen.storage.Transaction;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;

public class StorageTest {

    void deleteDirectory(String directoryPath) {
        File directoryToBeDeleted = new File(directoryPath);
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file.getAbsolutePath());
            }
        }
        directoryToBeDeleted.delete();
    }

    String defaultCf = "default";
    byte[] defaultValue = "defaultValue".getBytes();

    String cf1String = "cf1";

    byte[] k1Bytes = "k1".getBytes();
    byte[] k2Bytes = "k2".getBytes();
    byte[] k3Bytes = "k3".getBytes();
    byte[] k4Bytes = "k4".getBytes();

    byte[] v1Bytes = "v1".getBytes();
    byte[] v2Bytes = "v2".getBytes();
    byte[] v3Bytes = "v3".getBytes();
    byte[] v4Bytes = "v4".getBytes();

    @Test
    public void testStorage() throws Exception {
        String testStoragePath = "/tmp/jniStorageTest";
        deleteDirectory(testStoragePath);

        assertFalse(Storage.open(testStoragePath, false).isPresent());

        Optional<Storage> storage_new_opt = Storage.open(testStoragePath, true);
        assertTrue(storage_new_opt.isPresent());

        storage_new_opt.get().close();
        assertFalse(storage_new_opt.get().isOpened());

        Optional<Storage> storage_opt = Storage.open(testStoragePath, false);
        assertTrue(storage_opt.isPresent());
        Storage storage = storage_opt.get();

        assertFalse(storage.getColumnFamily(cf1String).isPresent());
        assertTrue(storage.setColumnFamily(cf1String));

        Optional<ColumnFamily> cf1_opt = storage.getColumnFamily(cf1String);
        assertTrue(cf1_opt.isPresent());
        ColumnFamily cf1 = cf1_opt.get();

        Optional<ColumnFamily> cf_default_opt = storage.getColumnFamily(defaultCf);
        assertTrue(cf_default_opt.isPresent());
        ColumnFamily cf_default = cf_default_opt.get();

        assertTrue(storage.isEmpty(cf_default));
        assertTrue(storage.isEmpty(cf1));

        assertFalse(storage.get(cf_default, k1Bytes).isPresent());
        assertArrayEquals(storage.getOrElse(cf1, k1Bytes, defaultValue), defaultValue);

        Optional<Transaction> transactionOpt = storage.createTransaction();
        assertTrue(transactionOpt.isPresent());
        Transaction transaction = transactionOpt.get();

        assertTrue(transaction.isEmpty(cf_default));
        assertTrue(transaction.isEmpty(cf1));

        assertFalse(transaction.get(cf_default, k1Bytes).isPresent());
        assertArrayEquals(transaction.getOrElse(cf1, k1Bytes, defaultValue), defaultValue);

        HashMap<byte[], byte[]> kvToInsert = new HashMap<>();
        kvToInsert.put(k1Bytes, v1Bytes);
        kvToInsert.put(k2Bytes, v2Bytes);
        kvToInsert.put(k3Bytes, v3Bytes);
        kvToInsert.put(k4Bytes, v4Bytes);
        byte[][] kToDelete = {k2Bytes, k3Bytes};

        assertTrue(transaction.save());

        assertTrue(transaction.update(cf1, kvToInsert, new byte[][]{}));
        assertFalse(transaction.isEmpty(cf1));

        assertTrue(transaction.rollbackToSavepoint());
        assertTrue(transaction.isEmpty(cf1));

        assertTrue(transaction.update(cf1, kvToInsert, new byte[][]{}));
        assertFalse(transaction.isEmpty(cf1));

        assertTrue(transaction.rollback());
        assertTrue(transaction.isEmpty(cf1));

        assertTrue(transaction.update(cf1, kvToInsert, new byte[][]{}));
        assertTrue(transaction.update(cf1, new HashMap<>(), kToDelete));
        
        {
            Optional<byte[]> v1 = transaction.get(cf1, k1Bytes);
            assertTrue(v1.isPresent());
            assertArrayEquals(v1.get(), v1Bytes);

            assertTrue(transaction.get(cf1, k1Bytes).isPresent());
            assertFalse(transaction.get(cf1, k2Bytes).isPresent());
            assertFalse(transaction.get(cf1, k3Bytes).isPresent());
            assertTrue(transaction.get(cf1, k4Bytes).isPresent());

            byte[][] keysToGet = {k1Bytes, k2Bytes, k3Bytes, k4Bytes};
            Map<byte[], Optional<byte[]>> kvs = transaction.get(cf1, keysToGet);

            assertEquals(kvs.keySet().size(), 4);
            kvs.forEach((key, valueOpt) -> {
                if (Arrays.equals(key, k1Bytes)) {
                    assertTrue(valueOpt.isPresent() && Arrays.equals(valueOpt.get(), v1Bytes));
                } else if (Arrays.equals(key, k2Bytes)) {
                    assertFalse(valueOpt.isPresent());
                } else if (Arrays.equals(key, k3Bytes)) {
                    assertFalse(valueOpt.isPresent());
                } else if (Arrays.equals(key, k4Bytes)) {
                    assertTrue(valueOpt.isPresent() && Arrays.equals(valueOpt.get(), v4Bytes));
                } else {
                    throw new IllegalArgumentException("Invalid key");
                }
            });
        }

        assertTrue(storage.isEmpty(cf1));
        assertTrue(transactionOpt.get().commit());
        assertFalse(storage.isEmpty(cf1));

        {
            Optional<byte[]> v1 = storage.get(cf1, k1Bytes);
            assertTrue(v1.isPresent());
            assertArrayEquals(v1.get(), v1Bytes);

            assertTrue(storage.get(cf1, k1Bytes).isPresent());
            assertFalse(storage.get(cf1, k2Bytes).isPresent());
            assertFalse(storage.get(cf1, k3Bytes).isPresent());
            assertTrue(storage.get(cf1, k4Bytes).isPresent());

            byte[][] keysToGet = {k1Bytes, k2Bytes, k3Bytes, k4Bytes};
            Map<byte[], Optional<byte[]>> kvs = storage.get(cf1, keysToGet);

            assertEquals(kvs.keySet().size(), 4);
            kvs.forEach((key, valueOpt) -> {
                if (Arrays.equals(key, k1Bytes)) {
                    assertTrue(valueOpt.isPresent() && Arrays.equals(valueOpt.get(), v1Bytes));
                } else if (Arrays.equals(key, k2Bytes)) {
                    assertFalse(valueOpt.isPresent());
                } else if (Arrays.equals(key, k3Bytes)) {
                    assertFalse(valueOpt.isPresent());
                } else if (Arrays.equals(key, k4Bytes)) {
                    assertTrue(valueOpt.isPresent() && Arrays.equals(valueOpt.get(), v4Bytes));
                } else {
                    throw new IllegalArgumentException("Invalid key");
                }
            });
        }
        transactionOpt.get().close();
    }
}
