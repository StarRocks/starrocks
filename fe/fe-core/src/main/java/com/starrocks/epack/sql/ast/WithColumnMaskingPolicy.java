// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.analyzer.AnalyzerUtilsEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class WithColumnMaskingPolicy implements ParseNode {
    private final PolicyName policyName;
    private final List<String> usingColumns;
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

    public void analyze(ConnectContext context) {
        AnalyzerUtilsEPack.normalizationPolicyName(context, policyName);
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyMgr.getPolicyByName(PolicyType.MASKING, policyName, false);
        policyId = policy.getPolicyId();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
