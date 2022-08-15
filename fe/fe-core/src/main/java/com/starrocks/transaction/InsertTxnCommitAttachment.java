// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InsertTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("loadedRows")
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
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String s = Text.readString(in);
        InsertTxnCommitAttachment insertTxnCommitAttachment =
                GsonUtils.GSON.fromJson(s, InsertTxnCommitAttachment.class);
        this.loadedRows = insertTxnCommitAttachment.getLoadedRows();
    }
}
