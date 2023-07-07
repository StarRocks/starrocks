// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;

public class AnalyzerUtilsEPack {
    public static void normalizationPolicyName(ConnectContext connectContext, PolicyName policyName) {
        if (Strings.isNullOrEmpty(policyName.getCatalog())) {
            if (Strings.isNullOrEmpty(connectContext.getCurrentCatalog())) {
                throw new SemanticException("No catalog selected");
            }
            policyName.setCatalog(connectContext.getCurrentCatalog());
        }
        if (Strings.isNullOrEmpty(policyName.getDbName())) {
            if (Strings.isNullOrEmpty(connectContext.getDatabase())) {
                throw new SemanticException("No database selected");
            }
            policyName.setDbName(connectContext.getDatabase());
        }

        if (Strings.isNullOrEmpty(policyName.getName())) {
            throw new SemanticException("Table name is null");
        }
    }
}
