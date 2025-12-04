// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.catalog.TableName;
import com.starrocks.epack.authorization.PasswordPolicy;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DropPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.SetPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt;
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
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SecurityPolicyAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new SecurityPolicyAnalyzerVisitor().analyze(statement, session);
    }

    static class SecurityPolicyAnalyzerVisitor implements AstVisitorEPack<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            FeNameFormat.checkTableName(policyName.getName());
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
                FeNameFormat.checkTableName(policyRename.getNewPolicyName());
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

        @Override
        public Void visitCreatePasswordPolicyStatement(CreatePasswordPolicyStmt statement, ConnectContext context) {
            FeNameFormat.checkColumnName(statement.getPolicyName());

            SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
            if (securityPolicyMgr.getPasswordPolicy(statement.getPolicyName()) != null) {
                throw new SemanticException("Password policy " + statement.getPolicyName() + " already exists.");
            }

            Map<String, String> properties = statement.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (!PasswordPolicy.validPasswordProperties.contains(entry.getKey())) {
                    throw new SemanticException("Can't support property " + entry.getKey());
                }

                try {
                    int propertyValue = Integer.parseInt(entry.getValue());
                    if (propertyValue < 0) {
                        throw new SemanticException("Password Policy property " + entry.getValue() + " can not less than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new SemanticException("Password Policy property " + entry.getValue() + " value must be integer");
                }
            }

            return null;
        }

        @Override
        public Void visitDropPasswordPolicyStatement(DropPasswordPolicyStmt statement, ConnectContext context) {
            SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();

            PasswordPolicy passwordPolicy = securityPolicyMgr.getPasswordPolicy(statement.getPolicyName());
            if (passwordPolicy == null) {
                throw new SemanticException("Password Policy " + statement.getPolicyName() + " is not exist");
            }

            PasswordPolicy globalPasswordPolicy = securityPolicyMgr.getGlobalPasswordPolicy();
            if (globalPasswordPolicy != null) {
                if (Objects.equals(passwordPolicy.getPolicyId(), globalPasswordPolicy.getPolicyId())) {
                    throw new SemanticException("Cannot delete a password policy that is in use. " +
                            "You can use unset first and then delete it");
                }
            }
            return null;
        }

        @Override
        public Void visitShowPasswordPolicyStatement(ShowPasswordPolicyStmt statement, ConnectContext context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public Void visitShowCreatePasswordPolicyStatement(ShowCreatePasswordPolicyStmt statement, ConnectContext context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public Void visitSetPasswordPolicyStatement(SetPasswordPolicyStmt statement, ConnectContext context) {
            SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
            PasswordPolicy passwordPolicy = securityPolicyMgr.getPasswordPolicy(statement.getPolicyName());
            if (passwordPolicy == null) {
                throw new SemanticException("Password Policy " + statement.getPolicyName() + " is not exist");
            }
            return null;
        }

        @Override
        public Void visitUnsetPasswordPolicyStatement(UnsetPasswordPolicyStmt statement, ConnectContext context) {
            return visitStatement(statement, context);
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