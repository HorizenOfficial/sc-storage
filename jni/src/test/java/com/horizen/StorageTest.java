package com.horizen;

import com.horizen.common.ColumnFamilyManagerTest;
import com.horizen.common.ReaderTest;
import com.horizen.common.TransactionBasicTest;
import com.horizen.storage.Storage;
import com.horizen.storage.Transaction;
import org.junit.Test;

import java.util.*;

import static com.horizen.common.Utils.deleteDirectory;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class StorageTest {
    @Test
    public void testStorage() throws Exception {
        String testStoragePath = "/tmp/jniStorageTest";
        deleteDirectory(testStoragePath);

        try{
            Storage.open(testStoragePath, false);
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot open storage: Error { message: \"No need to create a DB (DB does not exist and the create_if_missing == false)\" }",
                    e.getMessage()
            );
        }

        Storage storage_new = Storage.open(testStoragePath, true);
        assertTrue(storage_new.isOpened());
        storage_new.close();
        assertFalse(storage_new.isOpened());

        Storage storage = Storage.open(testStoragePath, false);
        assertTrue(storage.isOpened());

        ColumnFamilyManagerTest.TestCFs testCFs = ColumnFamilyManagerTest.initialize(storage);

        Optional<Transaction> transactionOpt = storage.createTransaction();
        assertTrue(transactionOpt.isPresent());
        Transaction transaction = transactionOpt.get();

        // The Default CF value inside the transaction is the same as the retrieved one with 'getColumnFamily' method of the Storage
        assertTrue(testCFs.defaultCf.equals(transaction.defaultCf()));

        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));
        assertTrue(transaction.isEmpty());

        TransactionBasicTest.TestData testData = new TransactionBasicTest.TestData();

        testCFs.cfs.forEach(cf -> assertTrue(TransactionBasicTest.run(transaction, cf, testData.toInsert)));

        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));
        assertTrue(transaction.isEmpty());

        testCFs.cfs.forEach(cf -> {
            if(!cf.equals(testCFs.defaultCf)) { // the Default CF will be implicitly updated with the 'defaultUpdate' method
                assertTrue(TransactionBasicTest.update(transaction, cf, testData.toInsert, testData.toDelete));
            }
        });
        assertTrue(TransactionBasicTest.defaultUpdate(transaction, testData.toInsert, testData.toDelete));

        testCFs.cfs.forEach(cf -> assertFalse(transaction.isEmpty(cf)));
        assertFalse(transaction.isEmpty());

        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(transaction, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(transaction, testData.toRemain, testData.toDelete));

        testCFs.cfs.forEach(cf -> assertTrue(storage.isEmpty(cf)));
        assertTrue(storage.isEmpty());

        transaction.commit();

        testCFs.cfs.forEach(cf -> assertFalse(storage.isEmpty(cf)));
        assertFalse(storage.isEmpty());

        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(storage, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(storage, testData.toRemain, testData.toDelete));
    }
}
