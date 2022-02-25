package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

import java.util.Map;
import java.util.Set;

public interface TransactionBasic {
    void update(ColumnFamily cf, Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception;
    void save() throws Exception;
    void rollbackToSavepoint() throws Exception;
    void rollback() throws Exception;
}
