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

import com.starrocks.common.io.Text;
import com.starrocks.thrift.TManualLoadTxnCommitAttachment;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ManualLoadTxnCommitAttachment extends TxnCommitAttachment {
    private long loadedRows;
    private long filteredRows;
    private long receivedBytes;
    private long loadedBytes;
    // optional
    private String errorLogUrl;

    public ManualLoadTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
    }

    public ManualLoadTxnCommitAttachment(TManualLoadTxnCommitAttachment tManualLoadTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        this.loadedRows = tManualLoadTxnCommitAttachment.getLoadedRows();
        this.loadedBytes = tManualLoadTxnCommitAttachment.getLoadedBytes();
        this.receivedBytes = tManualLoadTxnCommitAttachment.getReceivedBytes();
        this.filteredRows = tManualLoadTxnCommitAttachment.getFilteredRows();
        if (tManualLoadTxnCommitAttachment.isSetErrorLogUrl()) {
            this.errorLogUrl = tManualLoadTxnCommitAttachment.getErrorLogUrl();
        }
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

    public String getErrorLogUrl() {
        return errorLogUrl;
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

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        filteredRows = in.readLong();
        loadedRows = in.readLong();
        if (in.readBoolean()) {
            errorLogUrl = Text.readString(in);
        }
        // TODO: Persist `receivedBytes` && `loadedBytes`
        // if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_93) {
        //     receivedBytes = in.readLong();
        //     loadedBytes = in.readLong();
        // }
    }
}
