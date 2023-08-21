// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.analyzer.AnalyzerUtilsEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class WithRowAccessPolicy implements ParseNode {
    private final PolicyName policyName;
    private final List<String> onColumns;
    private final NodePosition pos;

    //Resolved by Analyzer
    private Long policyId;

    public WithRowAccessPolicy(PolicyName policyName, List<String> onColumns, NodePosition pos) {
        this.policyName = policyName;
        this.onColumns = onColumns;
        this.pos = pos;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getOnColumns() {
        return onColumns;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public void analyze(ConnectContext context) {
        AnalyzerUtilsEPack.normalizationPolicyName(context, policyName);
        SecurityPolicyMgr securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyManager.getPolicyByName(PolicyType.ROW_ACCESS, policyName);
        if (policy == null) {
            throw new SemanticException("Can't find policy " + policyName);
        }
        policyId = policy.getPolicyId();

        if (policy.getArgNames().size() != onColumns.size()) {
            throw new SemanticException("The number of on columns does not match " +
                    "the number of parameters required by the policy");
        }
    }
}
