// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

public class NoAliveBackendException extends UserException {
    public NoAliveBackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public NoAliveBackendException(String msg) {
        super(msg);
    }
}
