package com.horizen.common.interfaces;

import java.util.List;

// Interface for updating the default column family of Transaction/TransactionVersioned
public interface DefaultTransactionBasic extends TransactionBasic, DefaultColumnFamily {

    // Performs the specified insertions ('keysToUpdate' and 'valuesToUpdate' vectors of Keys and corresponding to them Values)
    // and removals ('keysToDelete' vector of Keys) for the default column family in a current transaction
    // Throws Exception with error message if any error occurred
    default void update(List<byte[]> keysToUpdate, List<byte[]> valuesToUpdate, List<byte[]> keysToDelete) throws Exception {
        update(defaultCf(), keysToUpdate, valuesToUpdate, keysToDelete);
    }
}
