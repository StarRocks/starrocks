// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.parser;

import static java.lang.String.format;

public class ParsingException extends RuntimeException {
    //TODO: we shourd add more message in parsing exception
    //eg. line, offset
    public ParsingException(String message) {
        super(message, null);
    }

    public ParsingException(String formatString, Object... args) {
        super(format(formatString, args));
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return getErrorMessage();
    }
}
