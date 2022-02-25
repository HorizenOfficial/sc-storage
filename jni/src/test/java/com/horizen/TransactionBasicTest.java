package com.horizen;

import com.horizen.common.ColumnFamily;
import com.horizen.common.interfaces.Reader;
import com.horizen.common.interfaces.TransactionBasic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TransactionBasicTest {

    public static void test(TransactionBasic transaction,
                            ColumnFamily cf,
                            HashMap<byte[], byte[]> kvToInsert,
                            Set<byte[]> kToDelete) throws Exception {
        try{
            transaction.rollbackToSavepoint();
            fail();
        } catch (Exception e){
            assertEquals(
                    "Cannot rollback the transaction to save point: Error { message: \"NotFound: \" }",
                    e.getMessage()
            );
        }

        transaction.save();

        transaction.update(cf, kvToInsert, new HashSet<>());
        assertFalse(((Reader)transaction).isEmpty(cf));

        transaction.rollbackToSavepoint();
        assertTrue(((Reader)transaction).isEmpty(cf));

        transaction.update(cf, kvToInsert, new HashSet<>());
        assertFalse(((Reader)transaction).isEmpty(cf));

        transaction.rollback();
        assertTrue(((Reader)transaction).isEmpty(cf));

        transaction.update(cf, kvToInsert, new HashSet<>());
        transaction.update(cf, new HashMap<>(), kToDelete);
    }
}
