// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColumnId;

import java.util.List;

public class MaskingPolicyContext {
    @SerializedName(value = "i")
    Long policyId;

    @SerializedName(value = "c")
    List<ColumnId> usingColumns;

    public MaskingPolicyContext(Long policyId, List<ColumnId> usingColumns) {
        this.policyId = policyId;
        this.usingColumns = usingColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public List<ColumnId> getUsingColumns() {
        return usingColumns;
    }
}
