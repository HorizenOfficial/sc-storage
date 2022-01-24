package com.horizen.common;

import com.horizen.librust.Library;

public class ColumnFamily {
    // Loading the Rust library which contains all the underlying logic
    static {
        Library.load();
    }

    private final long columnFamilyPointer;

    // Constructor is intended to be called from inside of the Rust environment for setting a raw pointer to a Rust-instance of ColumnFamily
    private ColumnFamily(long columnFamilyPointer) {
        this.columnFamilyPointer = columnFamilyPointer;
    }
}
