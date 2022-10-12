// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

/**
 * if inconsistent issue occurs when handling journal, this exception is called, indicating it's time to quit the process
 */
public class JournalInconsistentException extends Exception {
    public JournalInconsistentException(String errMsg)  {
        super(errMsg);
    }
}
