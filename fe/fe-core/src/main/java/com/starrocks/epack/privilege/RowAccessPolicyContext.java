// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class RowAccessPolicyContext {
    @SerializedName(value = "i")
    Long policyId;

    @SerializedName(value = "c")
    List<String> onColumns;

    public RowAccessPolicyContext(Long policyId, List<String> onColumns) {
        this.policyId = policyId;
        this.onColumns = onColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public List<String> getOnColumns() {
        return onColumns;
    }
}
