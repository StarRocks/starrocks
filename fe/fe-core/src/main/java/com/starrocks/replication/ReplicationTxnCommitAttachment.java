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

package com.starrocks.replication;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/*
 * The new versions in a replication transaction depend on the versions in source cluster
 * So we use this class to save the version in source cluster when committing a replication transaction
 */
public class ReplicationTxnCommitAttachment extends TxnCommitAttachment {
    @SerializedName("partitionVersions")
    private Map<Long, Long> partitionVersions; // The data version of partitions, not the visible version

    @SerializedName("partitionVersionEpochs")
    private Map<Long, Long> partitionVersionEpochs; // The version epoch of partitions

    public ReplicationTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.REPLICATION);
    }

    public ReplicationTxnCommitAttachment(Map<Long, Long> partitionVersions, Map<Long, Long> partitionVersionEpochs) {
        super(TransactionState.LoadJobSourceType.REPLICATION);
        this.partitionVersions = partitionVersions;
        this.partitionVersionEpochs = partitionVersionEpochs;
    }

    public Map<Long, Long> getPartitionVersions() {
        return partitionVersions;
    }

    public Map<Long, Long> getPartitionVersionEpochs() {
        return partitionVersionEpochs;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }
}