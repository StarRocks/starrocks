// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.loadv2;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteAttachment extends TxnCommitAttachment {
    // insert overwrite need this partition list to process locks
    @SerializedName(value = "targetPartitionList")
    private List<Long> targetPartitionList;

    public InsertOverwriteAttachment() {
        super(TransactionState.LoadJobSourceType.INSERT_OVERWRITE);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write sourceType
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InsertOverwriteAttachment read(DataInput in) throws IOException {
        String json = Text.readString(in);
        InsertOverwriteAttachment insertOverwriteAttachment = GsonUtils.GSON.fromJson(json, InsertOverwriteAttachment.class);
        return insertOverwriteAttachment;
    }
}
