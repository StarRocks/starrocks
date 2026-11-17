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
import com.starrocks.common.io.Writable;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoKinds;
import com.starrocks.persist.proto.LoadJobSourceTypePB;
import com.starrocks.persist.proto.TxnCommitAttachmentCorePB;
import com.starrocks.persist.proto.TxnCommitAttachmentPB;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.thrift.TTxnCommitAttachment;

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

    protected TxnCommitAttachmentCorePB toTxnCommitAttachmentCoreProto() {
        TxnCommitAttachmentCorePB.Builder builder = TxnCommitAttachmentCorePB.newBuilder();
        if (sourceType != null) {
            builder.setSourceType(ProtoEnums.toProto(sourceType, LoadJobSourceTypePB.class, "LOAD_JOB_SOURCE_TYPE_"));
        }
        return builder.build();
    }

    protected void fillFromTxnCommitAttachmentCoreProto(TxnCommitAttachmentCorePB pb) {
        if (pb.hasSourceType()) {
            sourceType = ProtoEnums.fromProto(pb.getSourceType(),
                    TransactionState.LoadJobSourceType.class, "LOAD_JOB_SOURCE_TYPE_");
        }
    }

    public TxnCommitAttachmentPB toProto() {
        TxnCommitAttachmentPB.Builder builder = TxnCommitAttachmentPB.newBuilder();
        builder.setCore(toTxnCommitAttachmentCoreProto());
        fillProtoVariant(builder);
        return builder.build();
    }

    protected void fillProtoVariant(TxnCommitAttachmentPB.Builder builder) {
        throw new IllegalStateException("cannot persist " + getClass().getName());
    }

    protected void fillFromVariantProto(TxnCommitAttachmentPB pb) {
        throw new IllegalStateException("cannot load " + getClass().getName());
    }

    public static TxnCommitAttachment fromProto(TxnCommitAttachmentPB pb) {
        ProtoKinds.checkKnown(pb.getKind(), pb.getKindValue());
        TxnCommitAttachment value;
        switch (pb.getKind()) {
            case INSERT:
                if (!pb.hasInsert()) {
                    throw new IllegalArgumentException("missing insert variant");
                }
                value = new InsertTxnCommitAttachment();
                break;
            case LOAD_JOB_FINAL_OPERATION:
                if (!pb.hasLoadJobFinalOperation()) {
                    throw new IllegalArgumentException("missing loadJobFinalOperation variant");
                }
                value = new LoadJobFinalOperation();
                break;
            case MANUAL_LOAD:
                if (!pb.hasManualLoad()) {
                    throw new IllegalArgumentException("missing manualLoad variant");
                }
                value = new ManualLoadTxnCommitAttachment();
                break;
            case MINI_LOAD:
                if (!pb.hasMiniLoad()) {
                    throw new IllegalArgumentException("missing miniLoad variant");
                }
                value = new MiniLoadTxnCommitAttachment();
                break;
            case ROUTINE_LOAD:
                if (!pb.hasRoutineLoad()) {
                    throw new IllegalArgumentException("missing routineLoad variant");
                }
                value = new RLTaskTxnCommitAttachment();
                break;
            case STREAM_LOAD:
                if (!pb.hasStreamLoad()) {
                    throw new IllegalArgumentException("missing streamLoad variant");
                }
                value = new StreamLoadTxnCommitAttachment();
                break;
            case REPLICATION:
                if (!pb.hasReplication()) {
                    throw new IllegalArgumentException("missing replication variant");
                }
                value = new ReplicationTxnCommitAttachment();
                break;
            case COMPACTION:
                if (!pb.hasCompaction()) {
                    throw new IllegalArgumentException("missing compaction variant");
                }
                value = new CompactionTxnCommitAttachment();
                break;
            default:
                throw new IllegalArgumentException("unknown TxnCommitAttachment kind " + pb.getKind());
        }
        value.fillFromTxnCommitAttachmentCoreProto(pb.getCore());
        value.fillFromVariantProto(pb);
        return value;
    }
}
