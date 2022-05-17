package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

import java.util.List;

// Interface for basic functionality of Transaction/TransactionVersioned
public interface TransactionBasic {

    // Performs the specified insertions ('keysToUpdate' and 'valuesToUpdate' vectors of Keys and corresponding to them Values)
    // and removals ('keysToDelete' vector of Keys) for a specified column family 'cf' in a current transaction
    // Throws Exception with error message if any error occurred
    void update(ColumnFamily cf, List<byte[]> keysToUpdate, List<byte[]> valuesToUpdate, List<byte[]> keysToDelete) throws Exception;

    // Saves the current state of a transaction to which it can be rolled back later
    void save() throws Exception;

    // Rolls back the current state of a transaction to the most recent savepoint.
    // Can be performed sequentially thus restoring previous savepoints in LIFO order.
    void rollbackToSavepoint() throws Exception;

    // Rolls back transaction to the initial state (state at the moment when transaction was started)
    void rollback() throws Exception;
}
