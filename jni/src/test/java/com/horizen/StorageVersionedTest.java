package com.horizen;

import com.horizen.common.ColumnFamily;
import com.horizen.common.ColumnFamilyManagerTest;
import com.horizen.common.ReaderTest;
import com.horizen.common.TransactionBasicTest;
import com.horizen.storageVersioned.StorageVersioned;
import com.horizen.storageVersioned.TransactionVersioned;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.horizen.common.Utils.deleteDirectory;
import static org.junit.Assert.*;

public class StorageVersionedTest {

    private final static String emptyVersionId = "v0";
    private final static String nonEmptyVersionId = "v1";

    void saveState(StorageVersioned storage, String versionId) throws Exception {
        Optional<TransactionVersioned> transactionEmptyOpt = storage.createTransaction(Optional.empty());
        assertTrue(transactionEmptyOpt.isPresent());
        TransactionVersioned transactionEmpty = transactionEmptyOpt.get();
        transactionEmpty.commit(Optional.of(versionId));
    }

    void testEmptyStorageVersioning(
            StorageVersioned storage
    ) throws Exception {
        // Version with the same versionId can't be created
        try{
            saveState(storage, emptyVersionId);
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot commit the transaction: Error { message: \"Specified version already exists\" }",
                    e.getMessage()
            );
        }

        // There is only one version of the storage with 'emptyVersionId' ID
        List<String> versions = storage.rollbackVersions();
        assertTrue(versions.size() == 1 &&
                Objects.equals(versions.get(0), emptyVersionId));
        Optional<String> lastVersionOpt = storage.lastVersion();
        assertTrue(lastVersionOpt.isPresent() && lastVersionOpt.get().equals(emptyVersionId));
    }

    void testUpdatedStorageVersioning(StorageVersioned storage) throws Exception {
        // There are 2 versions of the storage with specified version IDs
        List<String> versions = storage.rollbackVersions();
        assertTrue(versions.size() == 2 &&
                Objects.equals(versions.get(0), emptyVersionId) &&
                Objects.equals(versions.get(1), nonEmptyVersionId));
        Optional<String> lastVersionOpt = storage.lastVersion();
        assertTrue(lastVersionOpt.isPresent() && lastVersionOpt.get().equals(nonEmptyVersionId));
    }

    void testVersionsImages(
            StorageVersioned storage,
            TransactionBasicTest.TestData testData
    ) {
        Function<TransactionVersioned, List<ColumnFamily>> getCFs =
                transaction ->
                        ColumnFamilyManagerTest.cfNames.stream().map(cfName -> {
                            Optional<ColumnFamily> cfOpt = Optional.empty();
                            try {
                                cfOpt = transaction.getColumnFamily(cfName);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            assertTrue(cfOpt.isPresent());
                            return cfOpt.get();
                        }).collect(Collectors.toList());

        Optional<TransactionVersioned> transactionEmptyOpt = storage.createTransaction(Optional.of(emptyVersionId));
        assertTrue(transactionEmptyOpt.isPresent());
        TransactionVersioned transactionEmpty = transactionEmptyOpt.get();
        // Transaction created for a specified version of a storage has its own CFs
        List<ColumnFamily> emptyCFs = getCFs.apply(transactionEmpty);
        // All CFs of an empty version of the storage are empty
        emptyCFs.forEach(cf -> assertTrue(transactionEmpty.isEmpty(cf)));
        assertTrue(transactionEmpty.isEmpty());

        Optional<TransactionVersioned> transactionNonEmptyOpt = storage.createTransaction(Optional.of(nonEmptyVersionId));
        assertTrue(transactionNonEmptyOpt.isPresent());
        TransactionVersioned transactionNonEmpty = transactionNonEmptyOpt.get();
        // Transaction created for a specified version of a storage has its own CFs
        List<ColumnFamily> nonEmptyCFs = getCFs.apply(transactionNonEmpty);
        // All CFs of a non-empty version of the storage are non-empty
        nonEmptyCFs.forEach(cf -> assertFalse(transactionNonEmpty.isEmpty(cf)));
        assertFalse(transactionNonEmpty.isEmpty());

        // The content of the non-empty version of the storage corresponds to the 'testData'
        nonEmptyCFs.forEach(cf -> assertTrue(ReaderTest.run(transactionNonEmpty, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(transactionNonEmpty, testData.toRemain, testData.toDelete));
    }
    void testStorageRollback(
            StorageVersioned storage,
            TransactionBasicTest.TestData testData
    ) throws Exception {
        try{
            storage.rollback("non_existing_version");
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot rollback the storage: Error { message: \"Specified version doesn't exist\" }",
                    e.getMessage()
            );
        }

        storage.rollback(nonEmptyVersionId);
        // Re-initializing CFs descriptors after rolling back the storage due to the storage's internal state is re-initialized
        ColumnFamilyManagerTest.TestCFs testCFs = ColumnFamilyManagerTest.get(storage);

        // All CFs of the non-empty version of the storage are non-empty
        testCFs.cfs.forEach(cf -> assertFalse(storage.isEmpty(cf)));
        assertFalse(storage.isEmpty());
        // The content of the non-empty version of the storage corresponds to the 'testData'
        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(storage, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(storage, testData.toRemain, testData.toDelete));

        storage.rollback(emptyVersionId);
        testCFs = ColumnFamilyManagerTest.get(storage);

        // All CFs of the empty version of the storage are empty
        testCFs.cfs.forEach(cf -> assertTrue(storage.isEmpty(cf)));
        assertTrue(storage.isEmpty());
    }

    @Test
    public void testStorage() throws Exception {
        int versionsStored = 5;

        String testStorageVersionedPath = "/tmp/jniStorageVersionedTest";
        deleteDirectory(testStorageVersionedPath);

        try{
            StorageVersioned.open(testStorageVersionedPath, false, versionsStored);
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot open the versioned storage: Error { message: \"No need to create a DB (DB does not exist and the create_if_missing == false)\" }",
                    e.getMessage()
            );
        }

        try{
            StorageVersioned.open(testStorageVersionedPath, true, -1);
            fail();
        } catch (Exception e){
            assertEquals(
                    "Number of stored versions can't be negative",
                    e.getMessage()
            );
        }

        StorageVersioned storage_new = StorageVersioned.open(testStorageVersionedPath, true, versionsStored);
        assertTrue(storage_new.isOpened());
        storage_new.close();
        assertFalse(storage_new.isOpened());

        StorageVersioned storage = StorageVersioned.open(testStorageVersionedPath, false, versionsStored);
        assertTrue(storage.isOpened());

        ColumnFamilyManagerTest.TestCFs testCFs = ColumnFamilyManagerTest.initialize(storage);

        Optional<TransactionVersioned> transactionOpt = storage.createTransaction(Optional.empty());
        assertTrue(transactionOpt.isPresent());
        TransactionVersioned transaction = transactionOpt.get();

        // The Default CF value inside the transaction is the same as the retrieved one with 'getColumnFamily' method of the Storage
        assertTrue(testCFs.defaultCf.equals(transaction.defaultCf()));

        try{
            transaction.getColumnFamily(ColumnFamilyManagerTest.defaultCf);
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot get column family for previous version of the storage: Error { message: \"Current transaction is not for a storage's version\" }",
                    e.getMessage()
            );
        }

        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));
        assertTrue(transaction.isEmpty());

        TransactionBasicTest.TestData testData = new TransactionBasicTest.TestData();

        testCFs.cfs.forEach(cf -> assertTrue(TransactionBasicTest.run(transaction, cf, testData.toInsert)));

        // All CFs of the initial transaction are empty
        testCFs.cfs.forEach(cf -> assertTrue(transaction.isEmpty(cf)));
        assertTrue(transaction.isEmpty());

        testCFs.cfs.forEach(cf -> {
            if(!cf.equals(testCFs.defaultCf)) { // the Default CF will be implicitly updated with the 'defaultUpdate' method
                assertTrue(TransactionBasicTest.update(transaction, cf, testData.toInsert, testData.toDelete));
            }
        });
        assertTrue(TransactionBasicTest.defaultUpdate(transaction, testData.toInsert, testData.toDelete));

        // All CFs of the updated transaction are non-empty
        testCFs.cfs.forEach(cf -> assertFalse(transaction.isEmpty(cf)));
        assertFalse(transaction.isEmpty());

        // The content of the updated transaction corresponds to the 'testData'
        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(transaction, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(transaction, testData.toRemain, testData.toDelete));

        // All CFs of the storage are empty
        testCFs.cfs.forEach(cf -> assertTrue(storage.isEmpty(cf)));
        assertTrue(storage.isEmpty());

        // There are no versions of the storage
        assertTrue(storage.rollbackVersions().isEmpty());
        assertFalse(storage.lastVersion().isPresent());

        // Creating version of the empty storage
        saveState(storage, emptyVersionId);
        testEmptyStorageVersioning(storage);

        transaction.commit(Optional.of(nonEmptyVersionId));
        testUpdatedStorageVersioning(storage);

        // All CFs of the storage are non-empty
        testCFs.cfs.forEach(cf -> assertFalse(storage.isEmpty(cf)));
        assertFalse(storage.isEmpty());

        // The content of the updated storage corresponds to the 'testData'
        testCFs.cfs.forEach(cf -> assertTrue(ReaderTest.run(storage, cf, testData.toRemain, testData.toDelete)));
        assertTrue(ReaderTest.runDefault(storage, testData.toRemain, testData.toDelete));

        testVersionsImages(storage, testData);
        testStorageRollback(storage, testData);
    }
}
