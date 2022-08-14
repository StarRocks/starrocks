// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public class AnyArrayType extends PseudoType {
    @Override
    public boolean matchesType(Type t) {
        return t instanceof AnyArrayType || t instanceof AnyElementType || t.isArrayType();
    }

    @Override
    public String toString() {
        return "PseudoType.AnyArrayType";
    }
}
