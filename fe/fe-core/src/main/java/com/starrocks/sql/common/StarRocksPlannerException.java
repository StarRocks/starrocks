// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

public class StarRocksPlannerException extends RuntimeException {
    private final ErrorType type;

    public ErrorType getType() {
        return type;
    }

    public StarRocksPlannerException(String message, ErrorType type) {
        super(message);
        this.type = type;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        return message;
    }
}
