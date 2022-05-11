package com.horizen.common;

import com.horizen.librust.Library;

// NOTE: The ColumnFamily should be retrieved again (with 'ColumnFamilyManager::getColumnFamily' method)
//       each time when re-initialization of a Storage or StorageVersioned occurs such as re-opening or rollback
public class ColumnFamily {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private final long columnFamilyPointer;

    // Constructor is intended to be called from inside the Rust environment for setting a raw pointer to a Rust-instance of ColumnFamily
    public ColumnFamily(long columnFamilyPointer) {
        this.columnFamilyPointer = columnFamilyPointer;
    }
    public boolean equals(ColumnFamily that){
        return (this.columnFamilyPointer == that.columnFamilyPointer);
    }
}
