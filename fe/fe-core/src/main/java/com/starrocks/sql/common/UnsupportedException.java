// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

public class UnsupportedException extends StarRocksPlannerException {
    private UnsupportedException(String formatString) {
        super(formatString, ErrorType.UNSUPPORTED);
    }

    public static UnsupportedException unsupportedException(String formatString) {
        throw new UnsupportedException(formatString);
    }
}
