// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

import com.google.common.base.Strings;

public class StarRocksPlannerException extends RuntimeException {
    private final ErrorType type;

    public ErrorType getType() {
        return type;
    }

    public StarRocksPlannerException(String message, ErrorType type) {
        super(message);
        this.type = type;
    }

    public StarRocksPlannerException(ErrorType type, String messageTemplate,  Object... errorMessageArgs) {
        super(Strings.lenientFormat(messageTemplate, errorMessageArgs));
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
