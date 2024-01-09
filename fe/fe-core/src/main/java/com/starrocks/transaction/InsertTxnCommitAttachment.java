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

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class InsertTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("loadedRows")
    private long loadedRows;

    private Map<String, String> loadCounters;

    public InsertTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
    }

    public InsertTxnCommitAttachment(long loadedRows) {
        super(TransactionState.LoadJobSourceType.INSERT_STREAMING);
        this.loadedRows = loadedRows;
    }

    public InsertTxnCommitAttachment(long loadedRows, Map<String, String> loadCounters) {
        this(loadedRows);
        this.loadCounters = loadCounters;
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

    @Override
    public String toString() {
        return "InsertTxnCommitAttachment " + loadCounters.toString();
    }
}
