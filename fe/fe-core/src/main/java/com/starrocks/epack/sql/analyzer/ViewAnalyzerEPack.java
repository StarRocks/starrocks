// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.ViewAnalyzer;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.Map;

public class ViewAnalyzerEPack {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new ViewAnalyzerVisitor().visit(statement, context);
    }

    static class ViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext context) {
            ViewAnalyzer.analyze(statement, context);
            Map<String, WithColumnMaskingPolicy> maskingPolicyMap = statement.getMaskingPolicyContextMap();
            for (Map.Entry<String, WithColumnMaskingPolicy> entry : maskingPolicyMap.entrySet()) {
                entry.getValue().analyze(context, entry.getKey());
            }

            for (WithRowAccessPolicy withRowAccessPolicy : statement.getWithRowAccessPolicies()) {
                withRowAccessPolicy.analyze(context);
            }
            return null;
        }
    }
}
