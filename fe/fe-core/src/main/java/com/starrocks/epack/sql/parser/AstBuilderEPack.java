// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.parser;

import com.google.common.base.Joiner;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Type;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.CancelDecommissionDiskClause;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.epack.sql.ast.DecommissionDiskClause;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DisableDiskClause;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.RefreshRoleMappingStatement;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.SetPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.epack.sql.ast.ShowSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.UnsetPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.StarRocksParser;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.util.stream.Collectors.toList;

public class AstBuilderEPack extends AstBuilder {
    public AstBuilderEPack(long sqlMode, IdentityHashMap<ParserRuleContext, List<HintNode>> hintMap) {
        super(sqlMode, hintMap);
    }

    private static final AstBuilderEPack.AstBuilderFactory INSTANCE = new AstBuilderEPack.AstBuilderFactory();

    public static AstBuilderEPack.AstBuilderFactory getInstance() {
        return INSTANCE;
    }

    public static class AstBuilderFactory extends AstBuilder.AstBuilderFactory {
        private AstBuilderFactory() {
            super();
        }

        public AstBuilder create(long sqlMode) {
            return new AstBuilderEPack(sqlMode, new IdentityHashMap<>());
        }

        public AstBuilder create(long sqlMode, IdentityHashMap<ParserRuleContext, List<HintNode>> hintMap) {
            return new AstBuilderEPack(sqlMode, hintMap);
        }
    }

    // ------------------------------------------- Table Statement -----------------------------------------------------
    @Override
    public ParseNode visitCreateTableStatement(StarRocksParser.CreateTableStatementContext context) {
        CreateTableStmt createTableStmt = (CreateTableStmt) super.visitCreateTableStatement(context);

        if (context.columnDesc() != null) {
            int columnSize = context.columnDesc().size();
            for (int i = 0; i < columnSize; ++i) {
                StarRocksParser.ColumnDescContext columnDescContext = context.columnDesc(i);
                if (columnDescContext.withMaskingPolicy() != null) {
                    WithColumnMaskingPolicy withColumnMaskingPolicy
                            = (WithColumnMaskingPolicy) visit(columnDescContext.withMaskingPolicy());

                    createTableStmt.getColumnDefs().get(i).setWithColumnMaskingPolicy(withColumnMaskingPolicy);
                }
            }
        }

        List<WithRowAccessPolicy> withRowAccessPolicies =
                visit(context.withRowAccessPolicy(), WithRowAccessPolicy.class);
        createTableStmt.setWithRowAccessPolicies(withRowAccessPolicies);

        return createTableStmt;
    }

    // ------------------------------------------- View Statement ------------------------------------------------------

    @Override
    public ParseNode visitCreateViewStatement(StarRocksParser.CreateViewStatementContext context) {
        CreateViewStmt createViewStmt = (CreateViewStmt) super.visitCreateViewStatement(context);

        if (context.columnNameWithComment() != null) {
            int columnSize = context.columnNameWithComment().size();
            for (int i = 0; i < columnSize; ++i) {
                StarRocksParser.ColumnNameWithCommentContext columnNameWithComment = context.columnNameWithComment(i);
                if (columnNameWithComment.withMaskingPolicy() != null) {
                    WithColumnMaskingPolicy withColumnMaskingPolicy
                            = (WithColumnMaskingPolicy) visit(columnNameWithComment.withMaskingPolicy());

                    createViewStmt.getColWithComments().get(i).setWithColumnMaskingPolicy(withColumnMaskingPolicy);
                }
            }
        }

        List<WithRowAccessPolicy> withRowAccessPolicies =
                visit(context.withRowAccessPolicy(), WithRowAccessPolicy.class);
        createViewStmt.setWithRowAccessPolicies(withRowAccessPolicies);
        return createViewStmt;
    }

    // ------------------------------------------- Materialized View Statement -----------------------------------------
    @Override
    public ParseNode visitCreateMaterializedViewStatement(StarRocksParser.CreateMaterializedViewStatementContext context) {
        StatementBase stmt = (StatementBase) super.visitCreateMaterializedViewStatement(context);
        if (stmt instanceof CreateMaterializedViewStmt) {
            return stmt;
        }

        CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement) stmt;
        if (context.columnNameWithComment() != null) {
            int columnSize = context.columnNameWithComment().size();
            for (int i = 0; i < columnSize; ++i) {
                StarRocksParser.ColumnNameWithCommentContext columnNameWithComment = context.columnNameWithComment(i);
                if (columnNameWithComment.withMaskingPolicy() != null) {
                    WithColumnMaskingPolicy withColumnMaskingPolicy
                            = (WithColumnMaskingPolicy) visit(columnNameWithComment.withMaskingPolicy());

                    createMaterializedViewStatement.getColWithComments().get(i)
                            .setWithColumnMaskingPolicy(withColumnMaskingPolicy);
                }
            }
        }

        List<WithRowAccessPolicy> withRowAccessPolicies =
                visit(context.withRowAccessPolicy(), WithRowAccessPolicy.class);
        createMaterializedViewStatement.setWithRowAccessPolicies(withRowAccessPolicies);
        return createMaterializedViewStatement;
    }

    @Override
    public ParseNode visitAlterMaterializedViewStatement(
            StarRocksParser.AlterMaterializedViewStatementContext context) {
        AlterMaterializedViewStmt alterMaterializedViewStmt =
                (AlterMaterializedViewStmt) super.visitAlterMaterializedViewStatement(context);

        if (context.applyMaskingPolicyClause() != null) {
            AlterTableClause alterClause = (AlterTableClause) visit(context.applyMaskingPolicyClause());
            alterMaterializedViewStmt.setAlterTableClause(alterClause);
        } else if (context.applyRowAccessPolicyClause() != null) {
            AlterTableClause alterClause = (AlterTableClause) visit(context.applyRowAccessPolicyClause());
            alterMaterializedViewStmt.setAlterTableClause(alterClause);
        }

        return alterMaterializedViewStmt;
    }

    // ---------------------------------------- Alter Policy Clause ---------------------------------------------------

    @Override
    public ParseNode visitApplyMaskingPolicyClause(StarRocksParser.ApplyMaskingPolicyClauseContext context) {
        String columName = ((Identifier) visit(context.identifier())).getValue();
        if (context.SET() != null) {
            List<String> usingColumns = new ArrayList<>();
            if (context.identifierList() != null) {
                final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
                usingColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
            }

            QualifiedName qualifiedName = getQualifiedName(context.policyName);
            PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

            WithColumnMaskingPolicy withColumnMaskingPolicy =
                    new WithColumnMaskingPolicy(policyName, usingColumns, createPos(context));

            return new ApplyMaskingPolicyClause(columName, withColumnMaskingPolicy, createPos(context));
        } else {
            return new RevokeMaskingPolicyClause(columName, createPos(context));
        }
    }

    @Override
    public ParseNode visitApplyRowAccessPolicyClause(StarRocksParser.ApplyRowAccessPolicyClauseContext context) {
        if (context.ADD() != null) {
            List<String> onColumns = new ArrayList<>();

            if (context.identifierList() != null) {
                final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
                onColumns.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
            }

            QualifiedName qualifiedName = getQualifiedName(context.policyName);
            PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

            WithRowAccessPolicy withRowAccessPolicy =
                    new WithRowAccessPolicy(policyName, onColumns, createPos(context));

            return new ApplyRowAccessPolicyClause(withRowAccessPolicy, createPos(context));
        } else {
            if (context.ALL() != null) {
                return new RevokeRowAccessPolicyClause(createPos(context));
            } else {
                QualifiedName qualifiedName = getQualifiedName(context.policyName);
                PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
                return new RevokeRowAccessPolicyClause(policyName, createPos(context));
            }
        }
    }

    @Override
    public ParseNode visitWithMaskingPolicy(StarRocksParser.WithMaskingPolicyContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        List<String> columnList = new ArrayList<>();
        if (context.identifierList() != null) {
            List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            columnList.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
        }

        return new WithColumnMaskingPolicy(policyName, columnList, createPos(context));
    }

    @Override
    public ParseNode visitWithRowAccessPolicy(StarRocksParser.WithRowAccessPolicyContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        List<String> columnList = new ArrayList<>();
        if (context.identifierList() != null) {
            List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            columnList.addAll(identifierList.stream().map(Identifier::getValue).collect(toList()));
        }

        return new WithRowAccessPolicy(policyName, columnList, createPos(context));
    }

    // ---------------------------------------- Security Integration Statement --------------------------------------

    @Override
    public ParseNode visitCreateSecurityIntegrationStatement(
            StarRocksParser.CreateSecurityIntegrationStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        Map<String, String> propertyMap = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                propertyMap.put(property.getKey(), property.getValue());
            }
        }
        return new CreateSecurityIntegrationStatement(name, propertyMap, createPos(context));
    }

    @Override
    public ParseNode visitAlterSecurityIntegrationStatement(
            StarRocksParser.AlterSecurityIntegrationStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.propertyList().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        return new AlterSecurityIntegrationStatement(name, properties, createPos(context));
    }

    @Override
    public ParseNode visitDropSecurityIntegrationStatement(
            StarRocksParser.DropSecurityIntegrationStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        return new DropSecurityIntegrationStatement(name, createPos(context));
    }

    @Override
    public ParseNode visitShowCreateSecurityIntegrationStatement(
            StarRocksParser.ShowCreateSecurityIntegrationStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        return new ShowCreateSecurityIntegrationStatement(name, createPos(context));
    }

    @Override
    public ParseNode visitShowSecurityIntegrationStatement(
            StarRocksParser.ShowSecurityIntegrationStatementContext context) {
        return new ShowSecurityIntegrationStatement();
    }

    @Override
    public ParseNode visitCreateRoleMappingStatement(
            StarRocksParser.CreateRoleMappingStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        Map<String, String> propertyMap = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                propertyMap.put(property.getKey(), property.getValue());
            }
        }
        return new CreateRoleMappingStatement(name, propertyMap, createPos(context));
    }

    @Override
    public ParseNode visitAlterRoleMappingStatement(
            StarRocksParser.AlterRoleMappingStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.propertyList().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        return new AlterRoleMappingStatement(name, properties, createPos(context));
    }

    @Override
    public ParseNode visitDropRoleMappingStatement(
            StarRocksParser.DropRoleMappingStatementContext context) {
        String name = ((Identifier) visit(context.identifier())).getValue();
        return new DropRoleMappingStatement(name, createPos(context));
    }

    @Override
    public ParseNode visitShowRoleMappingStatement(
            StarRocksParser.ShowRoleMappingStatementContext context) {
        return new ShowRoleMappingStatement();
    }

    @Override
    public ParseNode visitRefreshRoleMappingStatement(
            StarRocksParser.RefreshRoleMappingStatementContext context) {
        return new RefreshRoleMappingStatement();
    }

    // ---------------------------------------- Security Policy Statement -------------------------------------------

    @Override
    public ParseNode visitCreateMaskingPolicyStatement(StarRocksParser.CreateMaskingPolicyStatementContext context) {
        List<String> argNames = new ArrayList<>();
        List<TypeDef> argTypes = new ArrayList<>();
        if (context.policySignature() != null) {
            for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
                argNames.add(((Identifier) visit(arg.identifier())).getValue());
                argTypes.add(new TypeDef(getType(arg.type())));
            }
        }

        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        String comment = context.comment() == null ? "" : ((StringLiteral) visit(context.comment())).getStringValue();

        return new CreatePolicyStmt(context.IF() != null,
                PolicyType.MASKING, policyName, argNames, argTypes, new TypeDef(getType(context.type())),
                (Expr) visit(context.expression()), comment, createPos(context));
    }

    @Override
    public ParseNode visitDropMaskingPolicyStatement(StarRocksParser.DropMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new DropPolicyStmt(PolicyType.MASKING, policyName, context.IF() != null, context.FORCE() != null,
                createPos(context));
    }

    @Override
    public ParseNode visitAlterMaskingPolicyStatement(StarRocksParser.AlterMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        if (context.BODY() != null) {
            return new AlterPolicyStmt(PolicyType.MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetBody((Expr) visit(context.expression())), createPos(context));
        } else if (context.COMMENT() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new AlterPolicyStmt(PolicyType.MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetComment(stringLiteral.getValue()), createPos(context));
        } else {
            String newPolicyName = ((Identifier) visit(context.newPolicyName)).getValue();
            return new AlterPolicyStmt(PolicyType.MASKING, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicyRename(newPolicyName), createPos(context));
        }
    }

    @Override
    public ParseNode visitShowMaskingPolicyStatement(StarRocksParser.ShowMaskingPolicyStatementContext context) {
        String database = null;
        String catalog = null;
        // catalog.db
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            List<String> parts = qualifiedName.getParts();
            if (parts.size() == 2) {
                catalog = qualifiedName.getParts().get(0);
                database = qualifiedName.getParts().get(1);
            } else if (parts.size() == 1) {
                database = qualifiedName.getParts().get(0);
            }
        }

        return new ShowPolicyStmt(catalog, database, PolicyType.MASKING, createPos(context));
    }

    @Override
    public ParseNode visitShowCreateMaskingPolicyStatement(StarRocksParser.ShowCreateMaskingPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new ShowCreatePolicyStmt(PolicyType.MASKING, policyName, createPos(context));
    }

    @Override
    public ParseNode visitCreateRowAccessPolicyStatement(StarRocksParser.CreateRowAccessPolicyStatementContext context) {
        List<String> argNames = new ArrayList<>();
        List<TypeDef> argTypes = new ArrayList<>();
        if (context.policySignature() != null) {
            for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
                argNames.add(((Identifier) visit(arg.identifier())).getValue());
                argTypes.add(new TypeDef(getType(arg.type())));
            }
        }

        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        String comment = context.comment() == null ? "" : ((StringLiteral) visit(context.comment())).getStringValue();

        return new CreatePolicyStmt(context.IF() != null,
                PolicyType.ROW_ACCESS, policyName, argNames, argTypes, new TypeDef(Type.BOOLEAN),
                (Expr) visit(context.expression()), comment, createPos(context));
    }

    @Override
    public ParseNode visitAlterRowAccessPolicyStatement(StarRocksParser.AlterRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        if (context.BODY() != null) {
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetBody((Expr) visit(context.expression())), createPos(context));
        } else if (context.COMMENT() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicySetComment(stringLiteral.getValue()), createPos(context));
        } else {
            String newPolicyName = ((Identifier) visit(context.newPolicyName)).getValue();
            return new AlterPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null,
                    new AlterPolicyStmt.PolicyRename(newPolicyName), createPos(context));
        }
    }

    @Override
    public ParseNode visitDropRowAccessPolicyStatement(StarRocksParser.DropRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new DropPolicyStmt(PolicyType.ROW_ACCESS, policyName, context.IF() != null, context.FORCE() != null,
                createPos(context));
    }

    @Override
    public ParseNode visitShowRowAccessPolicyStatement(StarRocksParser.ShowRowAccessPolicyStatementContext context) {
        String database = null;
        String catalog = null;
        // catalog.db
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            List<String> parts = qualifiedName.getParts();
            if (parts.size() == 2) {
                catalog = qualifiedName.getParts().get(0);
                database = qualifiedName.getParts().get(1);
            } else if (parts.size() == 1) {
                database = qualifiedName.getParts().get(0);
            }
        }

        return new ShowPolicyStmt(catalog, database, PolicyType.ROW_ACCESS, createPos(context));
    }

    @Override
    public ParseNode visitShowCreateRowAccessPolicyStatement(
            StarRocksParser.ShowCreateRowAccessPolicyStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);
        return new ShowCreatePolicyStmt(PolicyType.ROW_ACCESS, policyName, createPos(context));
    }

    private PolicyName qualifiedNameToPolicyName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.policy_name
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 3) {
            return new PolicyName(parts.get(0), parts.get(1), parts.get(2), qualifiedName.getPos());
        } else if (parts.size() == 2) {
            return new PolicyName(null, qualifiedName.getParts().get(0), qualifiedName.getParts().get(1),
                    qualifiedName.getPos());
        } else if (parts.size() == 1) {
            return new PolicyName(null, null, qualifiedName.getParts().get(0), qualifiedName.getPos());
        } else {
            throw new ParsingException(PARSER_ERROR_MSG.invalidTableFormat(qualifiedName.toString()));
        }
    }

    @Override
    public ParseNode visitCreatePasswordPolicy(StarRocksParser.CreatePasswordPolicyContext ctx) {
        String policyName = getIdentifierName(ctx.identifier());
        String comment = ctx.comment() == null ? "" : ((StringLiteral) visit(ctx.comment())).getStringValue();
        Map<String, String> properties = getProperties(ctx.properties());
        return new CreatePasswordPolicyStmt(policyName, comment, properties, createPos(ctx));
    }

    @Override
    public ParseNode visitDropPasswordPolicy(StarRocksParser.DropPasswordPolicyContext ctx) {
        String policyName = getIdentifierName(ctx.identifier());
        return new DropPasswordPolicyStmt(policyName, createPos(ctx));
    }

    @Override
    public ParseNode visitShowPasswordPolicy(StarRocksParser.ShowPasswordPolicyContext ctx) {
        return new ShowPasswordPolicyStmt(createPos(ctx));
    }

    @Override
    public ParseNode visitShowCreatePasswordPolicy(StarRocksParser.ShowCreatePasswordPolicyContext ctx) {
        String policyName = getIdentifierName(ctx.identifier());
        return new ShowCreatePasswordPolicyStmt(policyName, createPos(ctx));
    }

    @Override
    public ParseNode visitSetPasswordPolicy(StarRocksParser.SetPasswordPolicyContext ctx) {
        String policyName = getIdentifierName(ctx.identifier());
        return new SetPasswordPolicyStmt(policyName, createPos(ctx));
    }

    @Override
    public ParseNode visitUnsetPasswordPolicy(StarRocksParser.UnsetPasswordPolicyContext ctx) {
        return new UnsetPasswordPolicyStmt(createPos(ctx));
    }

    // ---------------------------------------- Warehouse Statement ---------------------------------------------------
    @Override
    public ParseNode visitCreateWarehouseStatement(StarRocksParser.CreateWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String whName = identifier.getValue();
        Map<String, String> properties = null;
        if (context.properties() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }
        return new CreateWarehouseStmt(context.IF() != null, whName, properties, comment, createPos(context));
    }

    @Override
    public ParseNode visitSuspendWarehouseStatement(StarRocksParser.SuspendWarehouseStatementContext context) {
        String warehouseName = ((Identifier) visit(context.identifier())).getValue();
        return new SuspendWarehouseStmt(warehouseName, createPos(context));
    }

    @Override
    public ParseNode visitResumeWarehouseStatement(StarRocksParser.ResumeWarehouseStatementContext context) {
        String warehouseName = ((Identifier) visit(context.identifier())).getValue();
        return new ResumeWarehouseStmt(warehouseName, createPos(context));
    }

    @Override
    public ParseNode visitDropWarehouseStatement(StarRocksParser.DropWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String warehouseName = identifier.getValue();
        return new DropWarehouseStmt(context.IF() != null, warehouseName, createPos(context));
    }

    @Override
    public ParseNode visitSetWarehouseStatement(StarRocksParser.SetWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String warehouseName = identifier.getValue();
        return new SetWarehouseStmt(warehouseName, createPos(context));
    }

    @Override
    public ParseNode visitShowWarehousesStatement(StarRocksParser.ShowWarehousesStatementContext context) {
        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        return new ShowWarehousesStmt(pattern, createPos(context));
    }

    @Override
    public ParseNode visitShowClustersStatement(StarRocksParser.ShowClustersStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        return new ShowClustersStmt(whName, createPos(context));
    }

    @Override
    public ParseNode visitShowNodesStatement(StarRocksParser.ShowNodesStatementContext context) {
        String pattern = null;
        String warehouseName = null;
        if (context.WAREHOUSE() != null) {
            warehouseName = ((Identifier) visit(context.identifier())).getValue();
        } else if (context.WAREHOUSES() != null) {
            if (context.pattern != null) {
                StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
                pattern = stringLiteral.getValue();
            }
        }
        return new ShowNodesStmt(warehouseName, pattern, createPos(context));
    }

    @Override
    public ParseNode visitAlterWarehouseStatement(StarRocksParser.AlterWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String whName = identifier.getValue();
        Map<String, String> properties = new HashMap<>();
        if (context.modifyPropertiesClause() != null) {
            ModifyTablePropertiesClause clause = (ModifyTablePropertiesClause) visit(context.modifyPropertiesClause());
            properties = clause.getProperties();
        }
        return new AlterWarehouseStmt(whName, properties, createPos(context));
    }

    // ---------------------------------------- Failover Group Statement ---------------------------------------------------

    @Override
    public ParseNode visitCreatePrimaryFailoverGroupStatement(
            StarRocksParser.CreatePrimaryFailoverGroupStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        List<String> includeCatalogs = new ArrayList<>();
        List<DatabaseName> includeDatabases = new ArrayList<>();
        List<TableName> includeTables = new ArrayList<>();
        convertTableNames(parseIncludeTablesDescStatement(context.includeTablesDesc()),
                includeCatalogs, includeDatabases, includeTables);
        List<String> excludeCatalogs = new ArrayList<>();
        List<DatabaseName> excludeDatabases = new ArrayList<>();
        List<TableName> excludeTables = new ArrayList<>();
        convertTableNames(parseExcludeTablesDescStatement(context.excludeTablesDesc()),
                excludeCatalogs, excludeDatabases, excludeTables);
        List<String> members = parseMembersDescStatement(context.membersDesc());
        String schedule = ((StringLiteral) visit(context.scheduleDesc().string())).getStringValue();
        Map<String, String> properties = getProperties(context.properties());
        String comment = context.comment() == null ? null
                : ((StringLiteral) visit(context.comment().string())).getStringValue();

        return new CreatePrimaryFailoverGroupStmt(ifNotExist, failoverGroupName,
                includeCatalogs, includeDatabases, includeTables,
                excludeCatalogs, excludeDatabases, excludeTables,
                members, schedule, properties, comment, createPos(context));
    }

    @Override
    public ParseNode visitCreateSecondaryFailoverGroupStatement(
            StarRocksParser.CreateSecondaryFailoverGroupStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        String primaryMember = ((StringLiteral) visit(context.string())).getStringValue();

        return new CreateSecondaryFailoverGroupStmt(ifNotExist, failoverGroupName,
                primaryMember, createPos(context));
    }

    @Override
    public ParseNode visitDropFailoverGroupStatement(StarRocksParser.DropFailoverGroupStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new DropFailoverGroupStmt(ifNotExist, failoverGroupName, createPos(context));
    }

    @Override
    public ParseNode visitShowFailoverGroupsStatement(StarRocksParser.ShowFailoverGroupsStatementContext context) {
        String pattern = null;
        if (context.string() != null) {
            pattern = ((StringLiteral) visit(context.string())).getStringValue();
        }

        return new ShowFailoverGroupsStmt(pattern, createPos(context));
    }

    @Override
    public ParseNode visitDescribeFailoverGroupStatement(
            StarRocksParser.DescribeFailoverGroupStatementContext context) {
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new DescribeFailoverGroupStmt(failoverGroupName, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupSetStatement(
            StarRocksParser.AlterFailoverGroupSetStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        List<String> includeCatalogs = new ArrayList<>();
        List<DatabaseName> includeDatabases = new ArrayList<>();
        List<TableName> includeTables = new ArrayList<>();
        convertTableNames(parseIncludeTablesDescStatement(context.includeTablesDesc()),
                includeCatalogs, includeDatabases, includeTables);
        List<String> excludeCatalogs = new ArrayList<>();
        List<DatabaseName> excludeDatabases = new ArrayList<>();
        List<TableName> excludeTables = new ArrayList<>();
        convertTableNames(parseExcludeTablesDescStatement(context.excludeTablesDesc()),
                excludeCatalogs, excludeDatabases, excludeTables);
        List<String> members = parseMembersDescStatement(context.membersDesc());
        String schedule = context.scheduleDesc() == null ? null
                : ((StringLiteral) visit(context.scheduleDesc().string())).getStringValue();
        Map<String, String> properties = getProperties(context.properties());
        String comment = context.comment() == null ? null
                : ((StringLiteral) visit(context.comment().string())).getStringValue();

        return new AlterFailoverGroupSetStmt(ifNotExist, failoverGroupName,
                includeCatalogs, includeDatabases, includeTables,
                excludeCatalogs, excludeDatabases, excludeTables,
                members, schedule, properties, comment, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupAddStatement(
            StarRocksParser.AlterFailoverGroupAddStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        List<String> includeCatalogs = new ArrayList<>();
        List<DatabaseName> includeDatabases = new ArrayList<>();
        List<TableName> includeTables = new ArrayList<>();
        convertTableNames(parseIncludeTablesAddDescStatement(context.includeTablesAddDesc()),
                includeCatalogs, includeDatabases, includeTables);
        List<String> excludeCatalogs = new ArrayList<>();
        List<DatabaseName> excludeDatabases = new ArrayList<>();
        List<TableName> excludeTables = new ArrayList<>();
        convertTableNames(parseExcludeTablesAddDescStatement(context.excludeTablesAddDesc()),
                excludeCatalogs, excludeDatabases, excludeTables);
        List<String> members = parseMembersAddDescStatement(context.membersAddDesc());
        Map<String, String> properties = getProperties(context.properties());

        return new AlterFailoverGroupAddStmt(ifNotExist, failoverGroupName,
                includeCatalogs, includeDatabases, includeTables,
                excludeCatalogs, excludeDatabases, excludeTables,
                members, properties, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupRemoveStatement(
            StarRocksParser.AlterFailoverGroupRemoveStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        List<String> includeCatalogs = new ArrayList<>();
        List<DatabaseName> includeDatabases = new ArrayList<>();
        List<TableName> includeTables = new ArrayList<>();
        convertTableNames(parseIncludeTablesRemoveDescStatement(context.includeTablesRemoveDesc()),
                includeCatalogs, includeDatabases, includeTables);
        List<String> excludeCatalogs = new ArrayList<>();
        List<DatabaseName> excludeDatabases = new ArrayList<>();
        List<TableName> excludeTables = new ArrayList<>();
        convertTableNames(parseExcludeTablesRemoveDescStatement(context.excludeTablesRemoveDesc()),
                excludeCatalogs, excludeDatabases, excludeTables);
        List<String> members = parseMembersRemoveDescStatement(context.membersRemoveDesc());

        return new AlterFailoverGroupRemoveStmt(ifNotExist, failoverGroupName,
                includeCatalogs, includeDatabases, includeTables,
                excludeCatalogs, excludeDatabases, excludeTables,
                members, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupRefreshStatement(
            StarRocksParser.AlterFailoverGroupRefreshStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new AlterFailoverGroupRefreshStmt(ifNotExist, failoverGroupName, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupPrimaryStatement(
            StarRocksParser.AlterFailoverGroupPrimaryStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new AlterFailoverGroupPrimaryStmt(ifNotExist, failoverGroupName, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupSuspendStatement(
            StarRocksParser.AlterFailoverGroupSuspendStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new AlterFailoverGroupSuspendStmt(ifNotExist, failoverGroupName, createPos(context));
    }

    @Override
    public ParseNode visitAlterFailoverGroupResumeStatement(
            StarRocksParser.AlterFailoverGroupResumeStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();

        return new AlterFailoverGroupResumeStmt(ifNotExist, failoverGroupName, createPos(context));
    }

    private List<TableName> parseIncludeTablesDescStatement(
            StarRocksParser.IncludeTablesDescContext includeTablesDesc) {
        if (includeTablesDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(includeTablesDesc.tableNamesDesc());
    }

    private List<TableName> parseExcludeTablesDescStatement(
            StarRocksParser.ExcludeTablesDescContext excludeTablesDesc) {
        if (excludeTablesDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(excludeTablesDesc.tableNamesDesc());
    }

    private List<String> parseMembersDescStatement(StarRocksParser.MembersDescContext membersDesc) {
        if (membersDesc == null) {
            return null;
        }

        List<String> members = new ArrayList<>();
        List<StarRocksParser.StringContext> memberList = membersDesc.string();
        for (StarRocksParser.StringContext memberContext : memberList) {
            String member = ((StringLiteral) visit(memberContext)).getStringValue();
            members.add(member);
        }
        return members;
    }

    private List<TableName> parseIncludeTablesAddDescStatement(
            StarRocksParser.IncludeTablesAddDescContext includeTablesAddDesc) {
        if (includeTablesAddDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(includeTablesAddDesc.tableNamesDesc());
    }

    private List<TableName> parseExcludeTablesAddDescStatement(
            StarRocksParser.ExcludeTablesAddDescContext excludeTablesAddDesc) {
        if (excludeTablesAddDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(excludeTablesAddDesc.tableNamesDesc());
    }

    private List<String> parseMembersAddDescStatement(StarRocksParser.MembersAddDescContext membersAddDesc) {
        if (membersAddDesc == null) {
            return null;
        }

        List<String> members = new ArrayList<>();
        List<StarRocksParser.StringContext> memberList = membersAddDesc.string();
        for (StarRocksParser.StringContext memberContext : memberList) {
            String member = ((StringLiteral) visit(memberContext)).getStringValue();
            members.add(member);
        }
        return members;
    }

    private List<TableName> parseIncludeTablesRemoveDescStatement(
            StarRocksParser.IncludeTablesRemoveDescContext includeTablesRemoveDesc) {
        if (includeTablesRemoveDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(includeTablesRemoveDesc.tableNamesDesc());
    }

    private List<TableName> parseExcludeTablesRemoveDescStatement(
            StarRocksParser.ExcludeTablesRemoveDescContext excludeTablesRemoveDesc) {
        if (excludeTablesRemoveDesc == null) {
            return null;
        }

        return parseTableNamesDescStatement(excludeTablesRemoveDesc.tableNamesDesc());
    }

    private List<String> parseMembersRemoveDescStatement(StarRocksParser.MembersRemoveDescContext membersRemoveDesc) {
        if (membersRemoveDesc == null) {
            return null;
        }

        List<String> members = new ArrayList<>();
        List<StarRocksParser.IdentifierOrStringContext> memberList = membersRemoveDesc.identifierOrString();
        for (StarRocksParser.IdentifierOrStringContext memberContext : memberList) {
            String member = ((Identifier) visit(memberContext)).getValue();
            members.add(member);
        }
        return members;
    }

    private List<TableName> parseTableNamesDescStatement(
            List<StarRocksParser.TableNamesDescContext> tableNameList) {
        List<TableName> tableNames = new ArrayList<>();
        for (StarRocksParser.TableNamesDescContext tableName : tableNameList) {
            List<StarRocksParser.IdentifierOrStringOrStarContext> partList = tableName.identifierOrStringOrStar();
            List<String> parts = partList.stream().map(c -> ((Identifier) visit(c)).getValue()).collect(toList());
            switch (parts.size()) {
                case 1:
                    tableNames.add(new TableName(null, null, parts.get(0)));
                    break;
                case 2:
                    tableNames.add(new TableName(null, parts.get(0), parts.get(1)));
                    break;
                case 3:
                    tableNames.add(new TableName(parts.get(0), parts.get(1), parts.get(2)));
                    break;
                default:
                    throw new ParsingException(PARSER_ERROR_MSG.invalidTableFormat(Joiner.on('.').join(parts)));
            }
        }
        return tableNames;
    }

    private static void convertTableNames(List<TableName> originTableNames,
                                          List<String> catalogNames,
                                          List<DatabaseName> databaseNames,
                                          List<TableName> tableNames) {
        if (originTableNames == null) {
            return;
        }

        for (TableName tableName : originTableNames) {
            if (tableName.getTbl() == null) {
                throw new ParsingException(PARSER_ERROR_MSG.invalidTableFormat(tableName.toString()));
            } else if (tableName.getTbl().equals("*")) {
                if (tableName.getDb() == null) {
                    catalogNames.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                } else if (tableName.getDb().equals("*")) {
                    if (tableName.getCatalog() == null) {
                        catalogNames.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                    } else if (tableName.getCatalog().equals("*")) {
                        catalogNames.add(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                    } else {
                        catalogNames.add(tableName.getCatalog());
                    }
                } else {
                    databaseNames.add(new DatabaseName(tableName.getCatalog(), tableName.getDb()));
                }
            } else {
                tableNames.add(tableName);
            }
        }
    }

    @Override
    public ParseNode visitDecommissionDiskClause(StarRocksParser.DecommissionDiskClauseContext context) {
        List<String> strings = context
                .string()
                .stream()
                .map(c -> ((StringLiteral) visit(c)).getStringValue())
                .collect(toList());
        return new DecommissionDiskClause(strings.get(strings.size() - 1), strings.subList(0, strings.size() - 1));
    }

    @Override
    public ParseNode visitCancelDecommissionDiskClause(StarRocksParser.CancelDecommissionDiskClauseContext context) {
        List<String> strings = context
                .string()
                .stream()
                .map(c -> ((StringLiteral) visit(c)).getStringValue())
                .collect(toList());
        return new CancelDecommissionDiskClause(strings.get(strings.size() - 1), strings.subList(0, strings.size() - 1));
    }

    @Override
    public ParseNode visitDisableDiskClause(StarRocksParser.DisableDiskClauseContext context) {
        List<String> strings = context
                .string()
                .stream()
                .map(c -> ((StringLiteral) visit(c)).getStringValue())
                .collect(toList());
        return new DisableDiskClause(strings.get(strings.size() - 1), strings.subList(0, strings.size() - 1));
    }
}
