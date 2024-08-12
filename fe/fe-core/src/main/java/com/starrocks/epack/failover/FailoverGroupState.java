// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.util.Map;
import java.util.Objects;

public class FailoverGroupState {
    @SerializedName("id")
    protected final int id;

    protected FailoverGroupState(int id) {
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

    // Initial state
    public static final FailoverGroupState INITIALIZING = new FailoverGroupState(0);

    // Failover group is running after a successful handshake
    public static final FailoverGroupState RUNNING = new FailoverGroupState(1);

    // Only for secondary, secondary is replicating data from primary
    public static final FailoverGroupState REPLICATING = new FailoverGroupState(2);

    // Only for secondary, secondary is updating some meta from primary
    public static final FailoverGroupState UPDATING = new FailoverGroupState(3);

    public static final Map<String, FailoverGroupState> NAME_TO_STATE = new ImmutableMap.Builder<String, FailoverGroupState>()
            .put("INITIALIZING", INITIALIZING)
            .put("RUNNING", RUNNING)
            .put("REPLICATING", REPLICATING)
            .put("UPDATING", UPDATING)
            .build();

    public static final Map<Integer, String> ID_TO_NAME = new ImmutableMap.Builder<Integer, String>()
            .put(INITIALIZING.getId(), "INITIALIZING")
            .put(RUNNING.getId(), "RUNNING")
            .put(REPLICATING.getId(), "REPLICATING")
            .put(UPDATING.getId(), "UPDATING")
            .build();

    public static final Map<Integer, FailoverGroupState> ID_TO_STATE = new ImmutableMap.Builder<Integer, FailoverGroupState>()
            .put(INITIALIZING.getId(), INITIALIZING)
            .put(RUNNING.getId(), RUNNING)
            .put(REPLICATING.getId(), REPLICATING)
            .put(UPDATING.getId(), UPDATING)
            .build();

    @Override
    public String toString() {
        return name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FailoverGroupState)) {
            return false;
        }
        FailoverGroupState that = (FailoverGroupState) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
