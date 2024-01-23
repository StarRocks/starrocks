// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

import com.starrocks.persist.OperationType;

/**
 * if inconsistent issue occurs when handling journal, this exception is called, indicating it's time to quit the process
 */
public class JournalInconsistentException extends Exception {
    private short opCode = OperationType.OP_INVALID;

    public JournalInconsistentException(String errMsg)  {
        super(errMsg);
    }

    public JournalInconsistentException(short opCode, String errMsg)  {
        super(errMsg);
        this.opCode = opCode;
    }

    public short getOpCode() {
        return opCode;
    }
}
