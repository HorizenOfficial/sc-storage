package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

import java.util.Optional;

// Interface for managing column families of Storage/StorageVersioned
public interface ColumnFamilyManager {

    // Name of the default column family which exists in every instance of underlying RocksDB
    String DEFAULT_CF_NAME = "default";

    // Returns a handle for a specified column family name
    // Returns Optional.empty() if column family with a specified name is absent in storage
    Optional<ColumnFamily> getColumnFamily(String cf_name);

    // Creates column family with a specified name
    // Successfully returns if column family was created successfully or already exists
    // Throws Exception with describing message if any error occurred during column family creation
    void setColumnFamily(String cf_name) throws Exception;
}
