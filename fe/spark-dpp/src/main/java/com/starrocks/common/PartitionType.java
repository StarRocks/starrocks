// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.common;


public enum PartitionType {
    UNPARTITIONED("UNPARTITIONED"),
    RANGE("RANGE"),
    LIST("LIST");

    public String typeString;

    PartitionType(String typeString) {
        this.typeString = typeString;
    }

    public static PartitionType getByType(String type) {
        for (PartitionType typeEnum : values()) {
            if (typeEnum.typeString.equalsIgnoreCase(type)) {
                return typeEnum;
            }
        }
        return null;
    }
}
