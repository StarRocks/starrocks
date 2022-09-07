// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public class SubqueryType extends PseudoType {
    @Override
    public boolean matchesType(Type t) {
        return true;
    }
}
