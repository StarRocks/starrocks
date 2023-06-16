// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.gson.annotations.SerializedName;

public enum PolicyType {
    COLUMN_MASKING(1),
    ROW_ACCESS(2);

    @SerializedName("id")
    private final int id;

    PolicyType(int id) {
        this.id = id;
    }
}
