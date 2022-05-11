package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

import java.util.Map;
import java.util.Set;

// Interface for basic functionality of Transaction/TransactionVersioned
public interface TransactionBasic {

    // Performs the specified insertions ('toUpdate' vector of Key-Values) and removals ('toDelete' vector of Keys)
    // for a specified column family 'cf' in a current transaction
    // Throws Exception with error message if any error occurred
    void update(ColumnFamily cf, Map<byte[], byte[]> toUpdate, Set<byte[]> toDelete) throws Exception;

    // Saves the current state of a transaction to which it can be rolled back later
    void save() throws Exception;

    // Rolls back the current state of a transaction to the most recent savepoint.
    // Can be performed sequentially thus restoring previous savepoints in LIFO order.
    void rollbackToSavepoint() throws Exception;

    // Rolls back transaction to the initial state (state at the moment when transaction was started)
    void rollback() throws Exception;
}
