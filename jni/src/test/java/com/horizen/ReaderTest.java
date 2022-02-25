package com.horizen;

import com.horizen.common.ColumnFamily;
import com.horizen.common.interfaces.Reader;

import java.util.*;

import static org.junit.Assert.*;

public class ReaderTest {

    private static boolean contains(Set<byte[]> set, byte[] value){
       for(byte[] v : set){
           if (Arrays.equals(v, value))
               return true;
       }
       return false;
    }

    private static byte[] get(Map<byte[], byte[]> map, byte[] key){
        for(Map.Entry<byte[], byte[]> e : map.entrySet()){
            if (Arrays.equals(e.getKey(), key))
                return e.getValue();
        }
        return null;
    }

    public static void test(Reader reader,
                            ColumnFamily cf,
                            HashMap<byte[], byte[]> existing,
                            Set<byte[]> absent
    ){
        existing.forEach( (key, value) -> {
                Optional<byte[]> retrievedValue = reader.get(cf, key);
                assertTrue(retrievedValue.isPresent());
                assertArrayEquals(retrievedValue.get(), value);
            }
        );

        absent.forEach(key ->
                assertFalse(reader.get(cf, key).isPresent())
        );

        Set<byte[]> allKeys = new HashSet<>(existing.keySet());
        allKeys.addAll(absent);

        Map<byte[], Optional<byte[]>> kvs = reader.get(cf, allKeys);
        assertEquals(kvs.keySet().size(), allKeys.size());

        kvs.forEach((key, valueOpt) -> {
            if(contains(existing.keySet(), key)){
                assertTrue(
                        valueOpt.isPresent() &&
                        Arrays.equals(valueOpt.get(), get(existing, key))
                );
            } else {
                assertTrue(
                        contains(absent, key) &&
                        !valueOpt.isPresent()
                );
            }
        });
    }
}
