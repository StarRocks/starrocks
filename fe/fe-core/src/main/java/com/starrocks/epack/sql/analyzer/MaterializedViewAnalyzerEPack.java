// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;

import java.util.Map;

public class MaterializedViewAnalyzerEPack {
    public static void analyze(CreateMaterializedViewStatement statement, ConnectContext context) {
        MaterializedViewAnalyzer.analyze(statement, context);
        new MaterializedViewAnalyzerVisitor().visit(statement, context);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            Map<String, WithColumnMaskingPolicy> maskingPolicyMap = statement.getMaskingPolicyContextMap();
            for (Map.Entry<String, WithColumnMaskingPolicy> entry : maskingPolicyMap.entrySet()) {
                entry.getValue().analyze(context);
            }

            for (WithRowAccessPolicy withRowAccessPolicy : statement.getWithRowAccessPolicies()) {
                withRowAccessPolicy.analyze(context);
            }
            return null;
        }
    }
}
