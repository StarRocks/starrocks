package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;

public class TransactionIdInfo {
    @SerializedName("txnId")
    private long txnId;

    public TransactionIdInfo(){}

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }
}
