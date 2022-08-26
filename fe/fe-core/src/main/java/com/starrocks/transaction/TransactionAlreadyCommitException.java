// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.transaction;

public class TransactionAlreadyCommitException extends TransactionException {

    public TransactionAlreadyCommitException(String msg) {
        super(msg);
    }
}
