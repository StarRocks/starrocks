// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public enum RefreshType {
    SYNC("SYNC"),
    ASYNC("ASYNC"),
    MANUAL("MANUAL");

    public String typeString;

    RefreshType(String typeString) {
        this.typeString = typeString;
    }
    
}
