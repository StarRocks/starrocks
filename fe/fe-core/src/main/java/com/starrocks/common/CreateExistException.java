// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.common;

/**
 * Thrown for SQL statements that create while exists.
 */
public class CreateExistException extends UserException {
    public CreateExistException(String msg) {
        super(msg);
    }
}
