// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.example.udf;

public class UDFAdd {
    public Integer evaluate(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
