// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.thrift.TVarType;

// Set statement type.
public enum SetType {
    DEFAULT("DEFAULT"),
    GLOBAL("GLOBAL"),
    SESSION("SESSION"),
    USER("USER");

    private final String desc;

    SetType(String desc) {
        this.desc = desc;
    }

    public TVarType toThrift() {
        if (this == SetType.GLOBAL) {
            return TVarType.GLOBAL;
        }
        return TVarType.SESSION;
    }

    public static SetType fromThrift(TVarType tType) {
        if (tType == TVarType.GLOBAL) {
            return SetType.GLOBAL;
        }
        return SetType.SESSION;
    }
}
