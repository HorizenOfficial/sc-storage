package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;
import com.horizen.common.DBIterator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Reader {
    Optional<byte[]> get(ColumnFamily cf, byte[] key);
    Map<byte[], Optional<byte[]>> get(ColumnFamily cf,  Set<byte[]> keys);
    byte[] getOrElse(ColumnFamily cf, byte[] key, byte[] defaultValue);
    boolean isEmpty(ColumnFamily cf);

    DBIterator getIter(ColumnFamily cf) throws Exception;
    DBIterator getRIter(ColumnFamily cf) throws Exception;
    DBIterator getIterFrom(ColumnFamily cf, byte[] starting_key) throws Exception;
    DBIterator getRIterFrom(ColumnFamily cf, byte[] starting_key) throws Exception;
}
