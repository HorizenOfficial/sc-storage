package com.horizen.common;

import com.horizen.common.interfaces.ColumnFamilyManager;
import com.horizen.common.interfaces.Reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnFamilyManagerTest {

    public static class TestCFs {
        public ArrayList<ColumnFamily> cfs = new ArrayList<>();

        TestCFs(List<ColumnFamily> cfs){
            this.cfs.addAll(cfs);
        }
    }

    private final static String defaultCf = "default";
    private final static String cf1String = "cf1";

    public static TestCFs run(ColumnFamilyManager cfManager) throws Exception {
        assertFalse(cfManager.getColumnFamily(cf1String).isPresent());
        cfManager.setColumnFamily(cf1String);

        Optional<ColumnFamily> cf1_opt = cfManager.getColumnFamily(cf1String);
        assertTrue(cf1_opt.isPresent());
        ColumnFamily cf1 = cf1_opt.get();

        Optional<ColumnFamily> cf_default_opt = cfManager.getColumnFamily(defaultCf);
        assertTrue(cf_default_opt.isPresent());
        ColumnFamily cf_default = cf_default_opt.get();

        assertTrue(((Reader)cfManager).isEmpty(cf_default));
        assertTrue(((Reader)cfManager).isEmpty(cf1));

        return new TestCFs(Arrays.asList(cf_default, cf1));
    }
}
