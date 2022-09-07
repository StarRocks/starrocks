// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.parser;

import static java.lang.String.format;

public class OperationNotAllowedException extends ParsingException {

    public OperationNotAllowedException(String message) {
        super(message);
    }

    public OperationNotAllowedException(String formatString, Object... args) {
        super(format(formatString, args));
    }
}
