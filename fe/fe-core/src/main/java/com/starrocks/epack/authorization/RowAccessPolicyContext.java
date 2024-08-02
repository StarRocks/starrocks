// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColumnId;

import java.util.List;

public class RowAccessPolicyContext {
    @SerializedName(value = "i")
    Long policyId;

    @SerializedName(value = "c")
    List<ColumnId> onColumns;

    public RowAccessPolicyContext(Long policyId, List<ColumnId> onColumns) {
        this.policyId = policyId;
        this.onColumns = onColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public List<ColumnId> getOnColumns() {
        return onColumns;
    }
}
