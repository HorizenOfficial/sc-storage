package com.horizen.common.interfaces;

import com.horizen.common.DBIterator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// Interface for retrieving default column family's content from Storage/StorageVersioned and Transaction/TransactionVersioned
public interface DefaultReader extends Reader, DefaultColumnFamily {

    // Retrieves a value for a specified key (in the default column family)
    // from an underlying storage or returns Optional.empty() in case the key is absent
    default Optional<byte[]> get(byte[] key){
        return get(defaultCf(), key);
    }

    // Retrieves the values correspondingly to a specified list of keys (in the default column family) from an underlying storage.
    // For the absent keys the values in the corresponding positions are null
    default List<byte[]> get(List<byte[]> keys){
        return get(defaultCf(), keys);
    }

    // Retrieves a value for a specified key (in the default column family)
    // from an underlying storage or returns 'defaultValue' in case the key is absent
    default byte[] getOrElse(byte[] key, byte[] defaultValue){
        return getOrElse(defaultCf(), key, defaultValue);
    }

    // Checks whether an underlying storage contains any Key-Value pairs (in the default column family)
    default boolean isEmpty(){
        return isEmpty(defaultCf());
    }

    // Returns forward iterator for all contained keys in the 'default' column family in an underlying storage
    // Throws Exception with error message if any error occurred
    default DBIterator getIter() throws Exception {
        return getIter(defaultCf());
    }

    // Returns reverse iterator for all contained keys in the 'default' column family in an underlying storage
    // Throws Exception with error message if any error occurred
    default DBIterator getRIter() throws Exception {
        return getRIter(defaultCf());
    }

    // Returns forward iterator starting from a specified key for all contained keys in the 'default' column family in an underlying storage
    // Throws Exception with error message if any error occurred
    default DBIterator getIterFrom(byte[] startingKey) throws Exception {
        return getIterFrom(defaultCf(), startingKey);
    }

    // Returns reverse iterator starting from a specified key for all contained keys in the 'default' column family in an underlying storage
    // Throws Exception with error message if any error occurred
    default DBIterator getRIterFrom(byte[] startingKey) throws Exception {
        return getRIterFrom(defaultCf(), startingKey);
    }
}
