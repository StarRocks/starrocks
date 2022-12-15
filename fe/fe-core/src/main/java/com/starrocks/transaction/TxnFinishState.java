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

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.proto.TxnFinishStatePB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record all the replicas and their versions involved in a transaction.
 * this state is integrated into {@link TransactionState} and is persisted into edit log
 * <p>
 * the replicas are separated into two groups:
 * 1. normal replicas: the replicas' version == commit version, since the version is already stored in
 * commit info, don't need to record the version, to save space
 * 2. abnormal replicas: the replicas' version != commit version, so version is stored with replica
 */
public class TxnFinishState {
    private static Codec<TxnFinishStatePB> finishStatePBCodec = ProtobufProxy.create(TxnFinishStatePB.class);

    // store involved replicas with version that is same as version in commitInfo
    @SerializedName("normal")
    public Set<Long> normalReplicas = new HashSet<>();
    // store replica,version pairs that version != commit version
    @SerializedName("abnormal")
    public Map<Long, Long> abnormalReplicasWithVersion = new HashMap<>();

    public TxnFinishStatePB toPB() {
        TxnFinishStatePB pb = new TxnFinishStatePB();
        pb.normalReplicas = normalReplicas.stream().collect(Collectors.toList());
        pb.abnormalReplicasWithVersion = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : abnormalReplicasWithVersion.entrySet()) {
            pb.abnormalReplicasWithVersion.add(entry.getKey());
            pb.abnormalReplicasWithVersion.add(entry.getValue());
        }
        return pb;
    }

    public byte[] toBytes() throws IOException {
        return finishStatePBCodec.encode(toPB());
    }

    public void fromBytes(byte[] bytes) throws IOException {
        TxnFinishStatePB pb = finishStatePBCodec.decode(bytes);
        normalReplicas.addAll(pb.normalReplicas);
        Preconditions.checkState(pb.abnormalReplicasWithVersion.size() % 2 == 0);
        for (int i = 0; i < pb.abnormalReplicasWithVersion.size(); i += 2) {
            abnormalReplicasWithVersion.put(pb.abnormalReplicasWithVersion.get(i), pb.abnormalReplicasWithVersion.get(i + 1));
        }
    }
}
