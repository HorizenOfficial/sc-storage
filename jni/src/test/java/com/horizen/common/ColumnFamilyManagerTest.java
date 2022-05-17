package com.horizen.common;

import com.horizen.common.interfaces.ColumnFamilyManager;

import java.util.*;

import static org.junit.Assert.*;

public class ColumnFamilyManagerTest {

    public static class TestCFs {

        public ArrayList<ColumnFamily> cfs = new ArrayList<>();
        public ColumnFamily defaultCf;

        TestCFs(List<ColumnFamily> cfs){
            this.cfs.addAll(cfs);
            defaultCf = cfs.get(0);
        }
    }

    public final static String defaultCf = ColumnFamilyManager.DEFAULT_CF_NAME;
    public final static ArrayList<String> cfNames = new ArrayList<>(Arrays.asList(defaultCf, "cf1", "cf2"));

    // Initializes a set of column families used for tests and checks the correctness of CF managing functionality
    public static TestCFs initialize(ColumnFamilyManager cfManager) throws Exception {
        for (String cfName : cfNames){
            if(!Objects.equals(cfName, defaultCf)){ // the default CF should be already existing in an empty storage, no need to create it
                assertFalse(cfManager.getColumnFamily(cfName).isPresent());
                cfManager.setColumnFamily(cfName);
            }
        }
        return get(cfManager);
    }

    public static TestCFs get(ColumnFamilyManager cfManager) {
        List<ColumnFamily> cfs = new ArrayList<>();

        for(String cfName : cfNames){
            Optional<ColumnFamily> cf_opt = cfManager.getColumnFamily(cfName);
            assertTrue(cf_opt.isPresent());
            ColumnFamily cf = cf_opt.get();

            assertEquals(cf.name, cfName); // checking correctness of ColumnFamily's name
            cfs.add(cf);
        }
        return new TestCFs(cfs);
    }
}
