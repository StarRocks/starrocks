// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.gson.annotations.SerializedName;

public class PolicyType {
    @SerializedName("id")
    private final int id;

    PolicyType(int id) {
        this.id = id;
    }

    public static final PolicyType MASKING = new PolicyType(1);
    public static final PolicyType ROW_ACCESS = new PolicyType(2);
}
