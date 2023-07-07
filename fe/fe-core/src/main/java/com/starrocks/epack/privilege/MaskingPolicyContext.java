// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class MaskingPolicyContext {
    @SerializedName(value = "i")
    Long policyId;

    @SerializedName(value = "c")
    List<String> usingColumns;

    public MaskingPolicyContext(Long policyId, List<String> usingColumns) {
        this.policyId = policyId;
        this.usingColumns = usingColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public List<String> getUsingColumns() {
        return usingColumns;
    }
}
