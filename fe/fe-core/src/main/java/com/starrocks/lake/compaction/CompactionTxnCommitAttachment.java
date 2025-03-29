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

package com.starrocks.lake.compaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataOutput;
import java.io.IOException;

public class CompactionTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("fc")
    private boolean forceCommit = false;

    public CompactionTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.LAKE_COMPACTION);
        this.forceCommit = false;
    }

    public CompactionTxnCommitAttachment(boolean forceCommit) {
        super(TransactionState.LoadJobSourceType.LAKE_COMPACTION);
        this.forceCommit = forceCommit;
    }

    public boolean getForceCommit() {
        return forceCommit;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }
}
