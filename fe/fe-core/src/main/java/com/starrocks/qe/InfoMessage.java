// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.qe;

import com.google.gson.annotations.SerializedName;
import com.starrocks.transaction.TransactionStatus;

public class InfoMessage {
    @SerializedName("label")
    private String label;
    @SerializedName("status")
    private TransactionStatus status;
    @SerializedName("txnId")
    private long txnId;
    @SerializedName("err")
    private String err;
    public InfoMessage(String label, TransactionStatus status, long txnId, String err) {
        this.label = label;
        this.status = status;
        this.txnId = txnId;
        this.err = err;
    }
}
