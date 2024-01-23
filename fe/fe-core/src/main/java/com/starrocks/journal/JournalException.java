// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

import com.starrocks.persist.OperationType;

/**
 * exception on journals
 */
public class JournalException extends Exception {
    private short opCode = OperationType.OP_INVALID;

    public JournalException(String errMsg) {
        super(errMsg);
    }

    public JournalException(short opCode, String errMsg) {
        super(errMsg);
        this.opCode = opCode;
    }

    public short getOpCode() {
        return opCode;
    }
}
