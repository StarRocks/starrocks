// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InsertTxnCommitAttachment extends TxnCommitAttachment {
    private long loadedRows;

    public InsertTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
    }

    public InsertTxnCommitAttachment(long loadedRows) {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        this.loadedRows = loadedRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(loadedRows);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        loadedRows = in.readLong();
    }
}
