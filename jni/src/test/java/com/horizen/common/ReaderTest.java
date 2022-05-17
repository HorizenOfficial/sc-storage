package com.horizen.common;

import com.horizen.common.interfaces.DefaultReader;
import com.horizen.common.interfaces.Reader;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class ReaderTest {

    private static final byte[] defaultValue = "defaultValue".getBytes();

    private static boolean contains(List<byte[]> set, byte[] value){
       for(byte[] v : set){
           if (Arrays.equals(v, value))
               return true;
       }
       return false;
    }

    private static byte[] get(ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> entrySet, byte[] key){
        for(Map.Entry<byte[], byte[]> e : entrySet){
            if (Arrays.equals(e.getKey(), key))
                return e.getValue();
        }
        return null;
    }

    private static void testIter(DBIterator iter,
                                 ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> existing) {
        for(AbstractMap.SimpleEntry<byte[], byte[]> existingKV : existing){
            Optional<AbstractMap.SimpleEntry<byte[], byte[]>> kv = iter.next();
            assertTrue(kv.isPresent());
            assertArrayEquals(existingKV.getKey(), kv.get().getKey());
            assertArrayEquals(existingKV.getValue(), kv.get().getValue());
        }
        assertFalse(iter.next().isPresent());
    }

    private static void testRIter(DBIterator riter,
                                  ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> existing) {
        ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> reversedEntrySet = new ArrayList<>(0);
        existing.forEach(e -> reversedEntrySet.add(0, e));

        for(AbstractMap.SimpleEntry<byte[], byte[]> existingKV : reversedEntrySet){
            Optional<AbstractMap.SimpleEntry<byte[], byte[]>> kv = riter.next();
            assertTrue(kv.isPresent());
            assertArrayEquals(existingKV.getKey(), kv.get().getKey());
            assertArrayEquals(existingKV.getValue(), kv.get().getValue());
        }
        assertFalse(riter.next().isPresent());
    }

    public static boolean run(Reader reader,
                              ColumnFamily cf,
                              ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> existing,
                              List<byte[]> absent) {
        try{
            existing.forEach( kv -> {
                        Optional<byte[]> retrievedValue = reader.get(cf, kv.getKey());
                        assertTrue(retrievedValue.isPresent());
                        assertArrayEquals(retrievedValue.get(), kv.getValue());
                    }
            );

            absent.forEach(key -> {
                        assertFalse(reader.get(cf, key).isPresent());
                        assertArrayEquals(reader.getOrElse(cf, key, defaultValue), defaultValue);
                    }
            );

            List<byte[]> existingKeys = existing.stream().map(AbstractMap.SimpleEntry::getKey).collect(Collectors.toList());
            List<byte[]> allKeys = new ArrayList<>(existingKeys);
            allKeys.addAll(absent);

            List<byte[]> allValues = reader.get(cf, allKeys);
            assertEquals(allValues.size(), allKeys.size());

            for(int i = 0; i < allValues.size(); i++){
                byte[] key = allKeys.get(i);
                byte[] value = allValues.get(i);

                if(contains(existingKeys, key)){
                    assertTrue(
                            value != null &&
                                    Arrays.equals((value), get(existing, key))
                    );
                } else {
                    assertTrue(
                            contains(absent, key) &&
                                    value == null
                    );
                }
            }

            testIter(reader.getIter(cf), existing);
            testRIter(reader.getRIter(cf), existing);
            testIter(reader.getIterFrom(cf, existing.get(0).getKey()), existing);
            testRIter(reader.getRIterFrom(cf, existing.get(existing.size() - 1).getKey()), existing);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean runDefault(DefaultReader defaultReader,
                                     ArrayList<AbstractMap.SimpleEntry<byte[], byte[]>> existing,
                                     List<byte[]> absent) {
        try{
            existing.forEach( kv -> {
                        Optional<byte[]> retrievedValue = defaultReader.get(kv.getKey());
                        assertTrue(retrievedValue.isPresent());
                        assertArrayEquals(retrievedValue.get(), kv.getValue());
                    }
            );

            absent.forEach(key -> {
                        assertFalse(defaultReader.get(key).isPresent());
                        assertArrayEquals(defaultReader.getOrElse(key, defaultValue), defaultValue);
                    }
            );

            List<byte[]> existingKeys = existing.stream().map(AbstractMap.SimpleEntry::getKey).collect(Collectors.toList());
            List<byte[]> allKeys = new ArrayList<>(existingKeys);
            allKeys.addAll(absent);

            List<byte[]> allValues = defaultReader.get(allKeys);
            assertEquals(allValues.size(), allKeys.size());

            for(int i = 0; i < allValues.size(); i++){
                byte[] key = allKeys.get(i);
                byte[] value = allValues.get(i);

                if(contains(existingKeys, key)){
                    assertTrue(
                            value != null &&
                                    Arrays.equals((value), get(existing, key))
                    );
                } else {
                    assertTrue(
                            contains(absent, key) &&
                                    value == null
                    );
                }
            }

            testIter(defaultReader.getIter(), existing);
            testRIter(defaultReader.getRIter(), existing);
            testIter(defaultReader.getIterFrom(existing.get(0).getKey()), existing);
            testRIter(defaultReader.getRIterFrom(existing.get(existing.size() - 1).getKey()), existing);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
