// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.thrift.TTopNType;

public enum TopNType {
    ROW_NUMBER,
    RANK,
    DENSE_RANK;

    public TTopNType toThrift() {
        if (ROW_NUMBER.equals(this)) {
            return TTopNType.ROW_NUMBER;
        } else if (RANK.equals(this)) {
            return TTopNType.RANK;
        } else {
            return TTopNType.DENSE_RANK;
        }
    }

    public static TopNType parse(String name) {
        for (TopNType value : values()) {
            if (value.name().equalsIgnoreCase(name)) {
                return value;
            }
        }
        return null;
    }
}
