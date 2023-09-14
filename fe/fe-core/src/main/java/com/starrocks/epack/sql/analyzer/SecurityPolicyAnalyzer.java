// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.TypeManager;

import java.util.ArrayList;
import java.util.List;

public class SecurityPolicyAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new SecurityPolicyAnalyzerVisitor().analyze(statement, session);
    }

    static class SecurityPolicyAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            FeNameFormat.checkColumnName(policyName.getName());
            normalizationPolicyName(session, policyName);
            analyzePolicyBody(session, stmt.getPolicyType(), stmt.getExpression(),
                    stmt.getArgTypeDefs(), stmt.getReturnType().getType(), stmt.getArgNames());
            return null;
        }

        @Override
        public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationPolicyName(session, policyName);
            return null;
        }

        @Override
        public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationPolicyName(session, policyName);

            if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicyRename) {
                AlterPolicyStmt.PolicyRename policyRename = (AlterPolicyStmt.PolicyRename) stmt.getAlterPolicyClause();
                FeNameFormat.checkColumnName(policyRename.getNewPolicyName());
            } else if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicySetBody) {
                AlterPolicyStmt.PolicySetBody policySetBody = (AlterPolicyStmt.PolicySetBody) stmt.getAlterPolicyClause();

                SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
                Policy policy = securityPolicyMgr.getPolicyByName(stmt.getPolicyType(), stmt.getPolicyName(), stmt.isIfExists());
                if (policy == null) {
                    return null;
                }
                analyzePolicyBody(session, stmt.getPolicyType(), policySetBody.getPolicyBody(),
                        policy.getArgTypes(), policy.getRetType(), policy.getArgNames());

                stmt.setPolicyId(policy.getPolicyId());
            }
            return null;
        }

        private void analyzePolicyBody(ConnectContext context, PolicyType policyType, Expr policyBody,
                                       List<Type> argTypeDefs, Type returnType, List<String> argNames) {
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < argTypeDefs.size(); ++i) {
                fields.add(new Field(argNames.get(i), argTypeDefs.get(i), new TableName(null, "__policy"), policyBody));
            }

            ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(context);
            expressionAnalyzer.analyze(policyBody, new AnalyzeState(),
                    new Scope(RelationId.anonymous(), new RelationFields(fields)));

            //Policy Analyzer does not set the TableName of slotRef because the analysis here is just a temporary policy table.
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitSlot(SlotRef slotRef, Void context) {
                    TableName tableName = slotRef.getTblNameWithoutAnalyzed();
                    if (tableName != null) {
                        if (slotRef.getTblNameWithoutAnalyzed().equals(new TableName(null, "__policy"))) {
                            slotRef.setTblName(null);
                        }
                    }
                    return null;
                }
            }.visit(policyBody);

            //Check compatible between expr result type and return type
            TypeManager.addCastExpr(policyBody, returnType);
        }

        @Override
        public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalog())) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException("No catalog selected");
                }
                statement.setCatalog(context.getCurrentCatalog());
            }
            if (Strings.isNullOrEmpty(statement.getDbName())) {
                if (Strings.isNullOrEmpty(context.getDatabase())) {
                    throw new SemanticException("No database selected");
                }
                statement.setDbName(context.getDatabase());
            }
            return null;
        }

        @Override
        public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationPolicyName(session, policyName);
            return null;
        }
    }

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
            throw new SemanticException("Policy name is null");
        }
    }
}