// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

import static java.lang.String.format;

public class DmlException extends RuntimeException {
    public DmlException(String message) {
        super(message);
    }

    public DmlException(String formatString, Object... args) {
        super(format(formatString, args));
    }

    public DmlException(String formatString, Throwable cause, Object... args) {
        super(format(formatString, args), cause);
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
