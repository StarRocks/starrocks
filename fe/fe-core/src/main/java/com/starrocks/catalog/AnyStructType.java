// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public class AnyStructType extends PseudoType {
    @Override
    public boolean equals(Object obj) {
        return obj instanceof AnyStructType;
    }

    @Override
    public boolean matchesType(Type t) {
        return t instanceof AnyStructType || t instanceof AnyElementType || t.isStructType();
    }

    @Override
    public String toString() {
        return "PseudoType.AnyStructType";
    }
}
