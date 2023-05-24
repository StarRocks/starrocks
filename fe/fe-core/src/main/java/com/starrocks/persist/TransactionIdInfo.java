package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

public class TransactionIdInfo implements Writable {
    @SerializedName("txnId")
    private long txnId;

    public TransactionIdInfo(long txnId){
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }
}
