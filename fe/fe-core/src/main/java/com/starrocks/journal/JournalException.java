// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

/**
 * exception on journals
 */
public class JournalException extends Exception {
    public JournalException(String errMsg) {
        super(errMsg);
    }
}
