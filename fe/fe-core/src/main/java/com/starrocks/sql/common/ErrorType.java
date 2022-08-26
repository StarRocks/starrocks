// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

public enum ErrorType {
    USER_ERROR(0),
    INTERNAL_ERROR(1),
    UNSUPPORTED(2);

    private final int code;

    ErrorType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}