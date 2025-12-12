// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;

public class WithColumnMaskingPolicyAnalyzer {
    private WithColumnMaskingPolicyAnalyzer() {
    }

    public static void analyze(WithColumnMaskingPolicy maskingPolicy, ConnectContext context,
                               String maskingColumnName) {
        AnalyzerUtilsEPack.normalizationPolicyName(context, maskingPolicy.getPolicyName());
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        Policy policy = securityPolicyMgr.getPolicyByName(PolicyType.MASKING, maskingPolicy.getPolicyName());
        if (policy == null) {
            throw new SemanticException("Can't find policy " + maskingPolicy.getPolicyName());
        }
        maskingPolicy.setPolicyId(policy.getPolicyId());

        if (maskingPolicy.getUsingColumns() == null || maskingPolicy.getUsingColumns().isEmpty()) {
            if (policy.getArgNames().size() > 1) {
                throw new SemanticException("Multi-parameter policies need to use `using` to specify input parameters");
            } else if (policy.getArgNames().size() == 1) {
                maskingPolicy.setUsingColumns(Lists.newArrayList(maskingColumnName));
            }
        }

        if (policy.getArgNames().size() != maskingPolicy.getUsingColumns().size()) {
            throw new SemanticException("The number of using columns does not match "
                    + "the number of parameters required by the policy");
        }
    }
}
