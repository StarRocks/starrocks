// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.thrift.TFailoverGroupState;

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

    public TFailoverGroupState toThrift() {
        return TFailoverGroupState.values()[id];
    }

    public static FailoverGroupState fromThrift(TFailoverGroupState thriftState) {
        return FailoverGroupState.ID_TO_STATE.get(thriftState.ordinal());
    }

    // Initial state
    public static final FailoverGroupState INITIALIZING = new FailoverGroupState(
            TFailoverGroupState.INITIALIZING.ordinal());

    // Failover group is running after a successful handshake
    public static final FailoverGroupState RUNNING = new FailoverGroupState(TFailoverGroupState.RUNNING.ordinal());

    // Only for secondary, secondary is replicating data from primary
    public static final FailoverGroupState REPLICATING = new FailoverGroupState(
            TFailoverGroupState.REPLICATING.ordinal());

    // Failover group has error
    public static final FailoverGroupState ERROR = new FailoverGroupState(TFailoverGroupState.ERROR.ordinal());

    public static final Map<String, FailoverGroupState> NAME_TO_STATE = new ImmutableMap.Builder<String, FailoverGroupState>()
            .put("INITIALIZING", INITIALIZING)
            .put("RUNNING", RUNNING)
            .put("REPLICATING", REPLICATING)
            .put("ERROR", ERROR)
            .build();

    public static final Map<Integer, String> ID_TO_NAME = new ImmutableMap.Builder<Integer, String>()
            .put(INITIALIZING.getId(), "INITIALIZING")
            .put(RUNNING.getId(), "RUNNING")
            .put(REPLICATING.getId(), "REPLICATING")
            .put(ERROR.getId(), "ERROR")
            .build();

    public static final Map<Integer, FailoverGroupState> ID_TO_STATE = new ImmutableMap.Builder<Integer, FailoverGroupState>()
            .put(INITIALIZING.getId(), INITIALIZING)
            .put(RUNNING.getId(), RUNNING)
            .put(REPLICATING.getId(), REPLICATING)
            .put(ERROR.getId(), ERROR)
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
