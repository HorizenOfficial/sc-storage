package com.horizen.common.interfaces;

import com.horizen.common.ColumnFamily;

// Interface for retrieving a handle of a default column family
public interface DefaultColumnFamily {

    // Returns the default column family (its wrapped handle)
    ColumnFamily defaultCf();
}
