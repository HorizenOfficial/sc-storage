package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

// Interface for retrieving content from Storage/StorageVersioned and Transaction/TransactionVersioned
public interface Reader {

    // Retrieves value for a specified key in a specified column family
    // from an underlying storage or returns Optional.empty() in case the key is absent
    Optional<byte[]> get(ColumnFamily cf, byte[] key);

    // Retrieves Key-Value pairs for a specified list of keys in a specified column family from an underlying storage.
    // For the absent keys the values in corresponding Key-Value pairs are Optional.empty()
    Map<byte[], Optional<byte[]>> get(ColumnFamily cf,  Set<byte[]> keys);

    // Retrieves a value for a specified key in a specified column family
    // from an underlying storage or returns 'defaultValue' in case the key is absent
    byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue);

    // Checks whether an underlying storage contains any Key-Value pairs in a specified column family
    boolean isEmpty(ColumnFamily cf);

    // Returns forward iterator for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    DBIterator getIter(ColumnFamily cf) throws Exception;

    // Returns reverse iterator for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    DBIterator getRIter(ColumnFamily cf) throws Exception;

    // Returns forward iterator starting from a specified key for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    DBIterator getIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception;

    // Returns reverse iterator starting from a specified key for all contained keys in a specified column family in an underlying storage
    // Throws Exception with error message if any error occurred
    DBIterator getRIterFrom(ColumnFamily cf, byte[] startingKey) throws Exception;
}
