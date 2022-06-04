// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

public class LockException extends RuntimeException {
    public LockException(String msg) {
        super(msg);
    }
}
