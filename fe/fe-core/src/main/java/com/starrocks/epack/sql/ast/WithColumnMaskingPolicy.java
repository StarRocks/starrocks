// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ParseNode;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.analyzer.AnalyzerUtilsEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
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

    public void analyze(ConnectContext context, String maskingColumnName) {
        AnalyzerUtilsEPack.normalizationPolicyName(context, policyName);
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyMgr.getPolicyByName(PolicyType.MASKING, policyName);
        if (policy == null) {
            throw new SemanticException("Can't find policy " + policyName);
        }
        policyId = policy.getPolicyId();

        if (usingColumns == null || usingColumns.isEmpty()) {
            if (policy.getArgNames().size() > 1) {
                throw new SemanticException("Multi-parameter policies need to use `using` to specify input parameters");
            } else if (policy.getArgNames().size() == 1) {
                usingColumns = Lists.newArrayList(maskingColumnName);
            }
        }

        if (policy.getArgNames().size() != usingColumns.size()) {
            throw new SemanticException("The number of using columns does not match " +
                    "the number of parameters required by the policy");
        }
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
