package com.horizen;

import com.horizen.common.ColumnFamilyManagerTest;
import com.horizen.common.ReaderTest;
import com.horizen.common.TransactionBasicTest;
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

        ColumnFamilyManagerTest.TestCFs testCFs = ColumnFamilyManagerTest.run(storage);

        Optional<Transaction> transactionOpt = storage.createTransaction();
        assertTrue(transactionOpt.isPresent());
        Transaction transaction = transactionOpt.get();

        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));

        TransactionBasicTest.TestData testData = new TransactionBasicTest.TestData();

        testCFs.cfs.forEach(cf -> assertTrue(TransactionBasicTest.run(transaction, cf, testData.toInsert)));

        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));
        testCFs.cfs.forEach(cf -> assertTrue(TransactionBasicTest.update(transaction, cf, testData.toInsert, testData.toDelete)));
        testCFs.cfs.forEach(cf -> assertFalse(transaction.isEmpty(cf)));

        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(transaction, cf, testData.toRemain, testData.toDelete)));

        testCFs.cfs.forEach(cf -> assertTrue(storage.isEmpty(cf)));
        transaction.commit();
        testCFs.cfs.forEach(cf -> assertFalse(storage.isEmpty(cf)));

        transaction.close();

        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(storage, cf, testData.toRemain, testData.toDelete)));
    }
}
