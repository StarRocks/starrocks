// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

public class ParsingException extends RuntimeException {
    //TODO: we shourd add more message in parsing exception
    //eg. line, offset
    public ParsingException(String message) {
        super(message, null);
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return getErrorMessage();
    }
}
