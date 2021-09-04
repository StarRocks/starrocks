// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.common;

import static java.lang.String.format;

public class UnsupportedException extends StarRocksPlannerException {
    private UnsupportedException(String formatString, Object... args) {
        super(format(formatString, args), ErrorType.UNSUPPORTED);
    }

    public static UnsupportedException unsupportedException(String formatString, Object... args) {
        throw new UnsupportedException(format(formatString, args));
    }
}
