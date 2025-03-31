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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/TxnCommitAttachment.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.thrift.TTxnCommitAttachment;

import java.io.DataOutput;
import java.io.IOException;

public abstract class TxnCommitAttachment implements Writable {

    @SerializedName("s")
    protected TransactionState.LoadJobSourceType sourceType;

    public TxnCommitAttachment(TransactionState.LoadJobSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public static TxnCommitAttachment fromThrift(TTxnCommitAttachment txnCommitAttachment) {
        if (txnCommitAttachment != null) {
            switch (txnCommitAttachment.getLoadType()) {
                case ROUTINE_LOAD:
                    return new RLTaskTxnCommitAttachment(txnCommitAttachment.getRlTaskTxnCommitAttachment());
                case MINI_LOAD:
                    return new MiniLoadTxnCommitAttachment(txnCommitAttachment.getMlTxnCommitAttachment());
                case MANUAL_LOAD:
                    if (!txnCommitAttachment.isSetManualLoadTxnCommitAttachment()) {
                        return null;
                    }
                    return new ManualLoadTxnCommitAttachment(txnCommitAttachment.getManualLoadTxnCommitAttachment());
                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, sourceType.name());
    }
}
