package com.horizen.common.interfaces;

import java.util.Map;
import java.util.Set;

public interface DefaultTransactionBasic extends TransactionBasic, DefaultColumnFamily {
    default void update(Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception {
        update(defaultCf(), toUpdate, toDelete);
    }
}
