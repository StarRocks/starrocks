// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.epack.privilege.PolicyAppliedContext;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.privilege.TableUID;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;

import java.util.List;
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
            for (Map.Entry<String, WithColumnMaskingPolicy> entry : statement.getMaskingPolicyContextMap().entrySet()) {
                entry.getValue().analyze(context, entry.getKey());
            }

            for (WithRowAccessPolicy withRowAccessPolicy : statement.getWithRowAccessPolicies()) {
                withRowAccessPolicy.analyze(context);
            }

            List<BaseTableInfo> baseTableInfoList = statement.getBaseTableInfos();
            for (BaseTableInfo baseTableInfo : baseTableInfoList) {
                TableUID tableUID = TableUID.generate(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), baseTableInfo.getTableName());

                SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
                PolicyAppliedContext policyAppliedContext = securityPolicyMgr.getTableAppliedPolicyInfo(tableUID);
                if (policyAppliedContext != null && !policyAppliedContext.isEmpty()) {
                    throw new SemanticException("Can't create materialized view with table which has security policy");
                }
            }
            return null;
        }
    }
}
