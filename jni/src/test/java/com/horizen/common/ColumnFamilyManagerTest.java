package com.horizen.common;

import com.horizen.common.interfaces.ColumnFamilyManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public final static String cf1String = "cf1";
    public final static ArrayList<String> cfNames = new ArrayList<>(Arrays.asList(defaultCf, cf1String));

    public static TestCFs initialize(ColumnFamilyManager cfManager) throws Exception {
        assertFalse(cfManager.getColumnFamily(cf1String).isPresent());
        cfManager.setColumnFamily(cf1String);

        Optional<ColumnFamily> cf1_opt = cfManager.getColumnFamily(cf1String);
        assertTrue(cf1_opt.isPresent());

        // Default CF should be already existing in an empty storage
        Optional<ColumnFamily> cf_default_opt = cfManager.getColumnFamily(defaultCf);
        assertTrue(cf_default_opt.isPresent());

        return new TestCFs(Arrays.asList(cf_default_opt.get(), cf1_opt.get()));
    }

    public static TestCFs get(ColumnFamilyManager cfManager) {
        Optional<ColumnFamily> cf1_opt = cfManager.getColumnFamily(cf1String);
        assertTrue(cf1_opt.isPresent());

        Optional<ColumnFamily> cf_default_opt = cfManager.getColumnFamily(defaultCf);
        assertTrue(cf_default_opt.isPresent());

        return new TestCFs(Arrays.asList(cf_default_opt.get(), cf1_opt.get()));
    }
}
