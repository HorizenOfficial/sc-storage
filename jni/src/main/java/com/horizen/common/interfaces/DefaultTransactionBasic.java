package com.horizen.common.interfaces;

import java.util.Map;
import java.util.Set;

// Interface for updating the default column family of Transaction/TransactionVersioned
public interface DefaultTransactionBasic extends TransactionBasic, DefaultColumnFamily {

    // Performs the specified insertions ('toUpdate' vector of Key-Values) and removals ('toDelete' vector of Keys)
    // for the 'default' column family in a current transaction
    // Throws Exception with error message if any error occurred
    default void update(Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception {
        update(defaultCf(), toUpdate, toDelete);
    }
}
