// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.load.loadv2;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.thrift.TManualLoadTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataOutput;
import java.io.IOException;

public class ManualLoadTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("ls")
    private long loadedRows;
    @SerializedName("fr")
    private long filteredRows;

    private long unselectedRows;
    private long receivedBytes;
    private long loadedBytes;
    // optional
    @SerializedName("eu")
    private String errorLogUrl;
    private long beginTxnTime;
    private long planTime;
    private long receiveDataTime;

    public ManualLoadTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
    }

    public ManualLoadTxnCommitAttachment(TManualLoadTxnCommitAttachment tManualLoadTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        this.loadedRows = tManualLoadTxnCommitAttachment.getLoadedRows();
        this.loadedBytes = tManualLoadTxnCommitAttachment.getLoadedBytes();
        this.receivedBytes = tManualLoadTxnCommitAttachment.getReceivedBytes();
        this.filteredRows = tManualLoadTxnCommitAttachment.getFilteredRows();
        this.unselectedRows = tManualLoadTxnCommitAttachment.getUnselectedRows();
        if (tManualLoadTxnCommitAttachment.isSetErrorLogUrl()) {
            this.errorLogUrl = tManualLoadTxnCommitAttachment.getErrorLogUrl();
        }
        this.beginTxnTime = tManualLoadTxnCommitAttachment.getBeginTxnTime();
        this.planTime = tManualLoadTxnCommitAttachment.getPlanTime();
        this.receiveDataTime = tManualLoadTxnCommitAttachment.getReceiveDataTime();
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public long getReceivedBytes() {
        return receivedBytes;
    }

    public long getLoadedBytes() {
        return loadedBytes;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getUnselectedRows() {
        return unselectedRows;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    public long getBeginTxnTime() {
        return beginTxnTime;
    }

    public long getPlanTime() {
        return planTime;
    }

    public long getReceiveDataTime() {
        return receiveDataTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(filteredRows);
        out.writeLong(loadedRows);
        if (errorLogUrl == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, errorLogUrl);
        }
        // TODO: Persist `receivedBytes` && `loadedBytes`
        // out.writeLong(receivedBytes);
        // out.writeLong(loadedBytes);
    }
}
