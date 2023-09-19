// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.thrift.TFailoverGroupRole;

import java.util.Map;
import java.util.Objects;

public class FailoverGroupRole {
    @SerializedName("id")
    protected final int id;

    protected FailoverGroupRole(int id) {
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

    public TFailoverGroupRole toThrift() {
        return TFailoverGroupRole.values()[id];
    }

    public static FailoverGroupRole fromThrift(TFailoverGroupRole thriftRole) {
        return FailoverGroupRole.ID_TO_ROLE.get(thriftRole.ordinal());
    }

    public static final FailoverGroupRole NONE = new FailoverGroupRole(TFailoverGroupRole.NONE.ordinal());
    public static final FailoverGroupRole PRIMARY = new FailoverGroupRole(TFailoverGroupRole.PRIMARY.ordinal());
    public static final FailoverGroupRole SECONDARY = new FailoverGroupRole(TFailoverGroupRole.SECONDARY.ordinal());

    public static final Map<String, FailoverGroupRole> NAME_TO_ROLE = new ImmutableMap.Builder<String, FailoverGroupRole>()
            .put("NONE", NONE)
            .put("PRIMARY", PRIMARY)
            .put("SECONDARY", SECONDARY)
            .build();

    public static final Map<Integer, String> ID_TO_NAME = new ImmutableMap.Builder<Integer, String>()
            .put(NONE.getId(), "NONE")
            .put(PRIMARY.getId(), "PRIMARY")
            .put(SECONDARY.getId(), "SECONDARY")
            .build();

    public static final Map<Integer, FailoverGroupRole> ID_TO_ROLE = new ImmutableMap.Builder<Integer, FailoverGroupRole>()
            .put(NONE.getId(), NONE)
            .put(PRIMARY.getId(), PRIMARY)
            .put(SECONDARY.getId(), SECONDARY)
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
        if (!(o instanceof FailoverGroupRole)) {
            return false;
        }
        FailoverGroupRole that = (FailoverGroupRole) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
