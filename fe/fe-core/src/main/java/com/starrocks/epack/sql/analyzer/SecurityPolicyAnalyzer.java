// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.TypeDef;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
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
        public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext session) {
            PolicyName policyName = statement.getPolicyName();
            normalizationPolicyName(session, policyName);

            SelectList selectList;
            if (statement.getPolicyType().equals(PolicyType.MASKING)) {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(statement.getExpression(), null)), false);
            } else {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(null)), false);
            }
            List<Expr> row = Lists.newArrayList();
            for (TypeDef typeDef : statement.getArgTypeDefs()) {
                row.add(NullLiteral.create(typeDef.getType()));
            }
            List<List<Expr>> rows = Collections.singletonList(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, statement.getArgNames());

            Expr predicate = null;
            if (statement.getPolicyType().equals(PolicyType.ROW_ACCESS)) {
                predicate = statement.getExpression();
            }

            SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, predicate, null, null);
            QueryStatement queryStatement = new QueryStatement(selectRelation);
            Analyzer.analyze(queryStatement, session);

            if (statement.getPolicyType().equals(PolicyType.MASKING)) {
                Expr result = queryStatement.getQueryRelation().getOutputExpression().get(0);
                //Check compatible between expr result type and return type
                TypeManager.addCastExpr(result, statement.getReturnType().getType());
            }

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
            return null;
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