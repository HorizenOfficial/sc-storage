package com.horizen.common.interfaces;

import com.horizen.common.DBIterator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface DefaultReader extends Reader, DefaultColumnFamily {
    default Optional<byte[]> get(byte[] key){
        return get(defaultCf(), key);
    }
    default Map<byte[], Optional<byte[]>> get(Set<byte[]> keys){
        return get(defaultCf(), keys);
    }
    default byte[] getOrElse(byte[] key, byte[] defaultValue){
        return getOrElse(defaultCf(), key, defaultValue);
    }
    default boolean isEmpty(){
        return isEmpty(defaultCf());
    }
    default DBIterator getIter() throws Exception {
        return getIter(defaultCf());
    }
    default DBIterator getRIter() throws Exception {
        return getRIter(defaultCf());
    }
    default DBIterator getIterFrom(byte[] starting_key) throws Exception {
        return getIterFrom(defaultCf(), starting_key);
    }
    default DBIterator getRIterFrom(byte[] starting_key) throws Exception {
        return getRIterFrom(defaultCf(), starting_key);
    }
}
