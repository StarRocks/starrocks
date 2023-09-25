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

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Objects;

public class ReplicationJobState {
    @SerializedName("id")
    protected final int id;

    protected ReplicationJobState(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public String name() {
        String name = ID_TO_NAME.get(id);
        if (name != null) {
            return name;
        }
        return "UNKNOWN";
    }

    public static final ReplicationJobState INITIALIZING = new ReplicationJobState(1);
    public static final ReplicationJobState SNAPSHOTING = new ReplicationJobState(2);
    public static final ReplicationJobState REPLICATING = new ReplicationJobState(3);
    public static final ReplicationJobState COMMITTED = new ReplicationJobState(4);
    public static final ReplicationJobState ABORTED = new ReplicationJobState(5);

    public static final Map<String, ReplicationJobState> NAME_TO_STATE = new ImmutableMap.Builder<String, ReplicationJobState>()
            .put("INITIALIZING", INITIALIZING)
            .put("SNAPSHOTING", SNAPSHOTING)
            .put("REPLICATING", REPLICATING)
            .put("COMMITTED", COMMITTED)
            .put("ABORTED", ABORTED)
            .build();

    public static final Map<Integer, String> ID_TO_NAME = new ImmutableMap.Builder<Integer, String>()
            .put(INITIALIZING.getId(), "INITIALIZING")
            .put(SNAPSHOTING.getId(), "SNAPSHOTING")
            .put(REPLICATING.getId(), "REPLICATING")
            .put(COMMITTED.getId(), "COMMITTED")
            .put(ABORTED.getId(), "ABORTED")
            .build();

    public static final Map<Integer, ReplicationJobState> ID_TO_STATE = new ImmutableMap.Builder<Integer, ReplicationJobState>()
            .put(INITIALIZING.getId(), INITIALIZING)
            .put(SNAPSHOTING.getId(), SNAPSHOTING)
            .put(REPLICATING.getId(), REPLICATING)
            .put(COMMITTED.getId(), COMMITTED)
            .put(ABORTED.getId(), ABORTED)
            .build();

    @Override
    public String toString() {
        return name();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ReplicationJobState)) {
            return false;
        }
        ReplicationJobState that = (ReplicationJobState) other;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}