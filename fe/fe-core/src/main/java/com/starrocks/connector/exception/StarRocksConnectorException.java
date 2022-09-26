// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.exception;

import static java.lang.String.format;

public class StarRocksConnectorException extends RuntimeException {
    public StarRocksConnectorException(String message) {
        super(message);
    }

    public StarRocksConnectorException(String formatString, Object... args) {
        super(format(formatString, args));
    }

    public StarRocksConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return getErrorMessage();
    }

}
