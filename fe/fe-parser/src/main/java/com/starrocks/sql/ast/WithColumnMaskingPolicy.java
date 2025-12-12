// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class WithColumnMaskingPolicy implements ParseNode {
    private final PolicyName policyName;
    private List<String> usingColumns;
    private final NodePosition pos;

    //Resolved by Analyzer
    private Long policyId;

    public WithColumnMaskingPolicy(PolicyName policyName, List<String> usingColumns, NodePosition pos) {
        this.policyName = policyName;
        this.usingColumns = usingColumns;
        this.pos = pos;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getUsingColumns() {
        return usingColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public void setUsingColumns(List<String> usingColumns) {
        this.usingColumns = usingColumns;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
