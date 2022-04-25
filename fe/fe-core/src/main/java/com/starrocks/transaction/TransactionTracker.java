// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.transaction;

public class TransactionTracker {
    private long transactionId = -1;
    private long loadedRows = 0;
    private int filteredRows = 0;
    TransactionStatus txnStatus = TransactionStatus.UNKNOWN;

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public void setLoadedRows(long loadedRows) {
        this.loadedRows = loadedRows;
    }

    public int getFilteredRows() {
        return filteredRows;
    }

    public void setFilteredRows(int filteredRows) {
        this.filteredRows = filteredRows;
    }

    public TransactionStatus getTxnStatus() {
        return txnStatus;
    }

    public void setTxnStatus(TransactionStatus txnStatus) {
        this.txnStatus = txnStatus;
    }
}
