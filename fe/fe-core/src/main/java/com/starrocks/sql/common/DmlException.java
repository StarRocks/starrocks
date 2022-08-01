// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.common;

public class DmlException extends RuntimeException {
    public DmlException(String message) {
        super(message);
    }

    public DmlException(String message, Throwable cause) {
        super(message, cause);
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
