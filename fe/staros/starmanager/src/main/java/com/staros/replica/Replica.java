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

package com.staros.replica;

import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaInfoLite;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ReplicaState;
import com.staros.proto.WorkerInfo;

/**
 * ReplicaState affects the selection of replica lists to the client under different scenarios
 * 1. When number of replicas less than expected replicas
 *    - criteria: count(REPLICA_OK) + count(REPLICA_SCALE_OUT) < expected replica number
 *       REPLICA_SCALE_IN will not be taken into consideration, however the worker the REPLICA_SCALE_IN will be blocked
 *       so new created REPLICA_SCALE_OUT replica will not be assigned to the worker where the REPLICA_SCALE_IN sits
 *    - final state: count(REPLICA_OK) + count(REPLICA_SCALE_OUT) == expected replica number
 * 2. When number of replicas more than expected replicas, existing replicas needs to be eliminated.
 *    - criteria: count(REPLICA_OK) + count(REPLICA_SCALE_IN) >= expected replica number
 *       first choose one of the replica from replicas with REPLICA_SCALE_IN, if still exceeds the criteria,
 *       continue remove replicas with REPLICA_OK status. REPLICA_SCALE_OUT will not be taken into account.
 *    - final state: count(REPLICA_OK) + count(REPLICA_SCALE_IN) == expected replica number
 * 3. When the replicas returned to the client to get shard locations.
 *    - criteria: return all replicas with REPLICA_OK and REPLICA_SCALE_IN state. If still empty, return one of
 *      the replicas with REPLICA_SCALE_OUT state. If still empty, on-demand scheduling triggered, where there should
 *      be at least one replica created with REPLICA_SCALE_OUT state.
 */

// protected by shard manager's lock
public class Replica {
    private final long workerId;
    private ReplicaRole role;
    // After upgraded from older version, the replicaState will be REPLICA_OK by default.
    // 1. In the new version, the replica state will be in REPLICA_SCALE_OUT,
    // the state will be changed to REPLICA_STATE_OK either by starlet report status OK or
    // by ShardChecker turns to REPLICA_OK when the replica is in REPLICA_SCALE_OUT for a long time.
    // 2. Redundant replica will be removed by ShardChecker, when total replicas of a SHARD in a workergroup
    // exceeds the expected number of replicas.
    // 3. When doing balance, the TARGET replica will be marked as REPLICA_SCALE_OUT and the source replica
    // will be marked as REPLICA_SCALE_IN, and when the target replica turns to REPLICA_OK, the source replica
    // will be removed from the system.
    private ReplicaState replicaState;
    private long replicaStateTimestamp;
    private  boolean isTemp;

    public Replica(long workerId) {
        this.workerId = workerId;
        this.role = ReplicaRole.PRIMARY;
        this.replicaState = ReplicaState.REPLICA_OK;
        this.replicaStateTimestamp = System.currentTimeMillis();
        this.isTemp = false;
    }

    public Replica(long workerId, ReplicaState state, long replicaStateTimestamp, boolean isTemp) {
        this.workerId = workerId;
        this.role = ReplicaRole.PRIMARY;
        this.replicaState = state;
        this.replicaStateTimestamp = replicaStateTimestamp;
        this.isTemp = isTemp;
    }

    public ReplicaRole getRole() {
        return role;
    }

    public void setRole(ReplicaRole role) {
        this.role = role;
    }

    public long getWorkerId() {
        return workerId;
    }

    public ReplicaState getState() {
        return replicaState;
    }

    public void setReplicaState(ReplicaState state) {
        if (this.replicaState == state) {
            return;
        }
        this.replicaState = state;
        replicaStateTimestamp = System.currentTimeMillis();
    }

    public boolean stateTimedOut(long timeoutMs) {
        return replicaStateTimestamp + timeoutMs < System.currentTimeMillis();
    }

    public boolean getTempFlag() {
        return isTemp;
    }

    public void clearTempFlag() {
        isTemp = false;
    }

    public ReplicaInfo toProtobuf() {
        ReplicaInfo.Builder builder = ReplicaInfo.newBuilder()
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(workerId).build())
                .setReplicaRole(role)
                .setReplicaState(replicaState)
                .setReplicaStateTimestamp(replicaStateTimestamp)
                .setIsTemp(isTemp);
        return builder.build();
    }

    public ReplicaInfoLite toReplicaInfoLite() {
        return ReplicaInfoLite.newBuilder()
                .setWorkerId(workerId)
                .setReplicaState(replicaState)
                .build();
    }

    public static Replica fromProtobuf(ReplicaInfo info) {
        Replica replica = new Replica(info.getWorkerInfo().getWorkerId(), info.getReplicaState(),
                info.getReplicaStateTimestamp(), info.getIsTemp());
        replica.setRole(info.getReplicaRole());
        return replica;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(workerId ^ replicaState.ordinal());
    }
}
