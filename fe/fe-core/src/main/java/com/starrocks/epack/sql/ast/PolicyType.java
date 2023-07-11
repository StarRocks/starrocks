// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class PolicyType {
    @SerializedName("id")
    private final int id;

    PolicyType(int id) {
        this.id = id;
    }

    public static final PolicyType MASKING = new PolicyType(1);
    public static final PolicyType ROW_ACCESS = new PolicyType(2);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PolicyType that = (PolicyType) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
