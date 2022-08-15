// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

/**
 * Thrown for SQL statements that Create while Exist.
 */
public class AlreadyExistsException extends UserException {
    public AlreadyExistsException(String msg) {
        super(msg);
    }
}
