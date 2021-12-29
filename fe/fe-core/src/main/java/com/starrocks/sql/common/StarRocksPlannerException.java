// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.common;

public class StarRocksPlannerException extends RuntimeException {
    private final ErrorType type;

    public ErrorType getType() {
        return type;
    }

    public StarRocksPlannerException(String message, ErrorType type, Exception e) {
        super(message, e);
        this.type = type;
    }

    public StarRocksPlannerException(String message, ErrorType type) {
        super(message);
        this.type = type;
    }

    public static StarRocksPlannerException nest(String message, ErrorType type, Exception e) {
        if (e instanceof StarRocksPlannerException) {
            return (StarRocksPlannerException) e;
        } else {
            return new StarRocksPlannerException(message, type, e);
        }
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
