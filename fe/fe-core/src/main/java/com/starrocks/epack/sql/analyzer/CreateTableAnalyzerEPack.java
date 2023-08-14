// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.CreateTableAnalyzer;
import com.starrocks.sql.ast.CreateTableStmt;

import java.util.Map;

public class CreateTableAnalyzerEPack {
    public static void analyze(CreateTableStmt statement, ConnectContext context) {
        CreateTableAnalyzer.analyze(statement, context);

        if (statement.getMaskingPolicyContextMap() != null) {
            Map<String, WithColumnMaskingPolicy> maskingPolicyMap = statement.getMaskingPolicyContextMap();
            for (Map.Entry<String, WithColumnMaskingPolicy> entry : maskingPolicyMap.entrySet()) {
                entry.getValue().analyze(context, entry.getKey());
            }
        }

        if (statement.getWithRowAccessPolicies() != null) {
            for (WithRowAccessPolicy withRowAccessPolicy : statement.getWithRowAccessPolicies()) {
                withRowAccessPolicy.analyze(context);
            }
        }
    }
}
