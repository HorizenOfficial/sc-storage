package com.horizen.common;

import com.horizen.common.interfaces.DefaultTransactionBasic;
import com.horizen.common.interfaces.Reader;
import com.horizen.common.interfaces.TransactionBasic;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TransactionBasicTest {

    public static class TestData {

        static byte[] k1Bytes = "k1".getBytes();
        static byte[] k2Bytes = "k2".getBytes();
        static byte[] k3Bytes = "k3".getBytes();
        static byte[] k4Bytes = "k4".getBytes();

        static AbstractMap.SimpleEntry<byte[], byte[]> entry1 = new AbstractMap.SimpleEntry<>(k1Bytes, "v1".getBytes());
        static AbstractMap.SimpleEntry<byte[], byte[]> entry2 = new AbstractMap.SimpleEntry<>(k2Bytes, "v2".getBytes());
        static AbstractMap.SimpleEntry<byte[], byte[]> entry3 = new AbstractMap.SimpleEntry<>(k3Bytes, "v3".getBytes());
        static AbstractMap.SimpleEntry<byte[], byte[]> entry4 = new AbstractMap.SimpleEntry<>(k4Bytes, "v4".getBytes());

        public ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> toInsert = new ArrayList<>(Arrays.asList(entry1, entry2, entry3, entry4));
        public ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> toRemain = new ArrayList<>();
        public ArrayList<byte[]> toDelete = new ArrayList<>(Arrays.asList(k2Bytes, k3Bytes));

        public TestData(){
            toInsert.forEach(kv -> {
                if(!toDelete.contains(kv.getKey())){
                    toRemain.add(kv);
                }
            });
        }
    }

    public static boolean run(TransactionBasic transaction,
                              ColumnFamily cf,
                              ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> kvToInsertList) {
        try{
            transaction.rollbackToSavepoint();
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot rollback the transaction to save point: Error { message: \"NotFound: \" }",
                    e.getMessage()
            );
        }

        try{
            ArrayList<byte[]> keysToInsert = new ArrayList<>();
            ArrayList<byte[]> valuesToInsert = new ArrayList<>();
            kvToInsertList.forEach(kv -> {
                keysToInsert.add(kv.getKey());
                valuesToInsert.add(kv.getValue());
            });

            transaction.save();

            transaction.update(cf, keysToInsert, valuesToInsert, new ArrayList<>());
            assertFalse(((Reader)transaction).isEmpty(cf));

            transaction.rollbackToSavepoint();
            assertTrue(((Reader)transaction).isEmpty(cf));

            transaction.update(cf, keysToInsert, valuesToInsert, new ArrayList<>());
            assertFalse(((Reader)transaction).isEmpty(cf));

            transaction.rollback();
            assertTrue(((Reader)transaction).isEmpty(cf));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean update(TransactionBasic transaction,
                                 ColumnFamily cf,
                                 List<AbstractMap.SimpleEntry<byte[], byte[]>> kvToInsertList,
                                 List<byte[]> kToDelete) {
        try {
            ArrayList<byte[]> keysToInsert = new ArrayList<>();
            ArrayList<byte[]> valuesToInsert = new ArrayList<>();
            kvToInsertList.forEach(kv -> {
                keysToInsert.add(kv.getKey());
                valuesToInsert.add(kv.getValue());
            });

            try{
                transaction.update(cf, keysToInsert, new ArrayList<>(), kToDelete);
                fail();
            } catch (Exception e){
                assertEquals(
                        "List of Keys to update should be of the same length as the list of Values",
                        e.getMessage()
                );
            }

            try{
                transaction.update(cf, new ArrayList<>(), valuesToInsert, kToDelete);
                fail();
            } catch (Exception e){
                assertEquals(
                        "List of Keys to update should be of the same length as the list of Values",
                        e.getMessage()
                );
            }

            transaction.update(cf, new ArrayList<>(), new ArrayList<>(), kToDelete);

            // Two separate calls to test the 'update' method with empty lists
            transaction.update(cf, keysToInsert, valuesToInsert, new ArrayList<>());
            transaction.update(cf, new ArrayList<>(), new ArrayList<>(), kToDelete);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean defaultUpdate(DefaultTransactionBasic transaction,
                                        List<AbstractMap.SimpleEntry<byte[], byte[]>> kvToInsertList,
                                        List<byte[]> kToDelete) {
        try {
            ArrayList<byte[]> keysToInsert = new ArrayList<>();
            ArrayList<byte[]> valuesToInsert = new ArrayList<>();
            kvToInsertList.forEach(kv -> {
                keysToInsert.add(kv.getKey());
                valuesToInsert.add(kv.getValue());
            });

            try{
                transaction.update(keysToInsert, new ArrayList<>(), kToDelete);
                fail();
            } catch (Exception e){
                assertEquals(
                        "List of Keys to update should be of the same length as the list of Values",
                        e.getMessage()
                );
            }

            try{
                transaction.update(new ArrayList<>(), valuesToInsert, kToDelete);
                fail();
            } catch (Exception e){
                assertEquals(
                        "List of Keys to update should be of the same length as the list of Values",
                        e.getMessage()
                );
            }

            // Two separate calls to test the 'update' method with empty lists
            transaction.update(keysToInsert, valuesToInsert, new ArrayList<>());
            transaction.update(new ArrayList<>(), new ArrayList<>(), kToDelete);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
