package com.horizen.common;

import com.horizen.common.ColumnFamily;
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
        public Set<byte[]> toDelete = new HashSet<>(Arrays.asList(k2Bytes, k3Bytes));

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
            HashMap<byte[], byte[]> kvToInsert = new HashMap<>();
            kvToInsertList.forEach(kv -> kvToInsert.put(kv.getKey(), kv.getValue()));

            transaction.save();

            transaction.update(cf, kvToInsert, new HashSet<>());
            assertFalse(((Reader)transaction).isEmpty(cf));

            transaction.rollbackToSavepoint();
            assertTrue(((Reader)transaction).isEmpty(cf));

            transaction.update(cf, kvToInsert, new HashSet<>());
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
                              ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> kvToInsertList,
                              Set<byte[]> kToDelete) {
        try {
            HashMap<byte[], byte[]> kvToInsert = new HashMap<>();
            kvToInsertList.forEach(kv -> kvToInsert.put(kv.getKey(), kv.getValue()));

            transaction.update(cf, kvToInsert, new HashSet<>());
            transaction.update(cf, new HashMap<>(), kToDelete);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
