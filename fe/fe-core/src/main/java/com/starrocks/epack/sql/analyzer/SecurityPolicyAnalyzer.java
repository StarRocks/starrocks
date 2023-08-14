// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.TypeManager;

import java.util.Collections;
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
            analyzePolicyBody(session, stmt.getPolicyType(), stmt.getExpression().clone(),
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
            SelectList selectList;
            Expr predicate = null;
            if (policyType.equals(PolicyType.MASKING)) {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(policyBody.clone(), null)), false);
            } else {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(null)), false);
                predicate = policyBody.clone();
            }

            List<Expr> row = Lists.newArrayList();
            ValuesRelation valuesRelation;
            if (!argNames.isEmpty()) {
                for (Type argType : argTypeDefs) {
                    row.add(NullLiteral.create(argType));
                }
                List<List<Expr>> rows = Collections.singletonList(row);
                valuesRelation = new ValuesRelation(rows, argNames);
                valuesRelation.setAlias(new TableName(null, "__policy"));
            } else {
                valuesRelation = ValuesRelation.newDualRelation();
            }

            SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, predicate, null, null);
            QueryStatement queryStatement = new QueryStatement(selectRelation);
            Analyzer.analyze(queryStatement, context);
            if (policyType.equals(PolicyType.MASKING)) {
                Expr result = queryStatement.getQueryRelation().getOutputExpression().get(0);
                //Check compatible between expr result type and return type
                TypeManager.addCastExpr(result, returnType);
            }
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