// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.common;

/**
 * Exception for timeout, like Util.executeCommand
 */
public class TimeoutException extends UserException {
    public TimeoutException(String msg) {
        super(msg);
    }
}