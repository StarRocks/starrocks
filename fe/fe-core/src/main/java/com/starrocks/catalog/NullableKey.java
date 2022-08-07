package com.starrocks.catalog;

public interface NullableKey {

    default String nullPartitionValue() {
        return "";
    }
}
