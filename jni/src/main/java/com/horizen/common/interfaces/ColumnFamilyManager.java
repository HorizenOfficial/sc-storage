package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

import java.util.Optional;

public interface ColumnFamilyManager {
    Optional<ColumnFamily> getColumnFamily(String cf_name);
    void setColumnFamily(String cf_name) throws Exception;
}
