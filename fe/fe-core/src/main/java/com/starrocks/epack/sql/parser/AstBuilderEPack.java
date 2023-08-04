// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.parser;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Type;
import com.starrocks.epack.sql.ast.AlterPolicyStmt;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ApplyMaskingPolicyClause;
import com.starrocks.epack.sql.ast.ApplyRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DatabaseName;
import com.starrocks.epack.sql.ast.DropPolicyStmt;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.RevokeMaskingPolicyClause;
import com.starrocks.epack.sql.ast.RevokeRowAccessPolicyClause;
import com.starrocks.epack.sql.ast.SetWarehouseStmt;
import com.starrocks.epack.sql.ast.ShowClustersStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.epack.sql.ast.ShowSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.epack.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.epack.sql.ast.WithRowAccessPolicy;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RefreshRoleMappingStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.StarRocksParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.util.stream.Collectors.toList;

public class AstBuilderEPack extends AstBuilder {

    public AstBuilderEPack(long sqlMode) {
        super(sqlMode);
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
            AlterClause alterClause = (AlterClause) visit(context.applyMaskingPolicyClause());
            alterMaterializedViewStmt.setOps(Collections.singletonList(alterClause));
        } else if (context.applyRowAccessPolicyClause() != null) {
            AlterClause alterClause = (AlterClause) visit(context.applyRowAccessPolicyClause());
            alterMaterializedViewStmt.setOps(Collections.singletonList(alterClause));
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

        List<String> columnList = null;
        if (context.identifierList() != null) {
            List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            columnList = identifierList.stream().map(Identifier::getValue).collect(toList());
        }

        return new WithColumnMaskingPolicy(policyName, columnList, createPos(context));
    }

    @Override
    public ParseNode visitWithRowAccessPolicy(StarRocksParser.WithRowAccessPolicyContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.policyName);
        PolicyName policyName = qualifiedNameToPolicyName(qualifiedName);

        List<String> columnList = null;
        if (context.identifierList() != null) {
            List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            columnList = identifierList.stream().map(Identifier::getValue).collect(toList());
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
        for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
            argNames.add(((Identifier) visit(arg.identifier())).getValue());
            argTypes.add(new TypeDef(getType(arg.type())));
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
        for (StarRocksParser.PolicySignatureContext arg : context.policySignature()) {
            argNames.add(((Identifier) visit(arg.identifier())).getValue());
            argTypes.add(new TypeDef(getType(arg.type())));
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

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowWarehousesStmt(pattern, where, createPos(context));
    }

    @Override
    public ParseNode visitShowClustersStatement(StarRocksParser.ShowClustersStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        return new ShowClustersStmt(whName, createPos(context));
    }

    
    // ---------------------------------------- Failover Group Statement ---------------------------------------------------

    @Override
    public ParseNode visitCreatePrimaryFailoverGroupStatement(
            StarRocksParser.CreatePrimaryFailoverGroupStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        String failoverGroupName = ((Identifier) visit(context.identifierOrString())).getValue();
        List<String> catalogNames = parseCatalogsDescStatement(context.catalogsDesc());
        List<DatabaseName> databaseNames = parseDatabasesDescStatement(context.databasesDesc());
        List<TableName> tableNames = parseTablesDescStatement(context.tablesDesc());
        List<String> members = parseMembersDescStatement(context.membersDesc());
        String schedule = ((StringLiteral) visit(context.scheduleDesc().string())).getStringValue();
        Map<String, String> properties = getProperties(context.properties());
        String comment = context.comment() == null ? null : 
                ((StringLiteral) visit(context.comment().string())).getStringValue();

        return new CreatePrimaryFailoverGroupStmt(ifNotExist, failoverGroupName, catalogNames, 
                databaseNames, tableNames, members, schedule, properties, comment, createPos(context));
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

    private List<String> parseCatalogsDescStatement(StarRocksParser.CatalogsDescContext catalogsDesc) {
        if (catalogsDesc == null) {
            return null;
        }

        List<String> catalogNames = new ArrayList<>();
        List<StarRocksParser.IdentifierContext> catalogList = catalogsDesc.identifier();
        for (StarRocksParser.IdentifierContext catalog : catalogList) {
            String catalogName = ((Identifier) visit(catalog)).getValue();
            catalogNames.add(catalogName);
        }
        return catalogNames;
    }

    private List<DatabaseName> parseDatabasesDescStatement(StarRocksParser.DatabasesDescContext databasesDesc) {
        if (databasesDesc == null) {
            return null;
        }

        List<DatabaseName> databaseNames = new ArrayList<>();
        List<StarRocksParser.QualifiedNameContext> databaseList = databasesDesc.qualifiedName();
        for (StarRocksParser.QualifiedNameContext database : databaseList) {
            DatabaseName databaseName = qualifiedNameToDatabaseName(getQualifiedName(database));
            databaseNames.add(databaseName);
        }
        return databaseNames;
    }

    private List<TableName> parseTablesDescStatement(StarRocksParser.TablesDescContext tablesDesc) {
        if (tablesDesc == null) {
            return null;
        }

        List<TableName> tableNames = new ArrayList<>();
        List<StarRocksParser.QualifiedNameContext> tableList = tablesDesc.qualifiedName();
        for (StarRocksParser.QualifiedNameContext table : tableList) {
            TableName tableName = qualifiedNameToTableName(getQualifiedName(table));
            tableNames.add(tableName);
        }
        return tableNames;
    }

    private List<String> parseMembersDescStatement(StarRocksParser.MembersDescContext membersDesc) {
        if (membersDesc == null) {
            throw new ParsingException(PARSER_ERROR_MSG.noViableStatement("MEMBERS"));
        }

        List<String> members = new ArrayList<>();
        List<StarRocksParser.StringContext> memberList = membersDesc.string();
        for (StarRocksParser.StringContext memberContext : memberList) {
            String member = ((StringLiteral) visit(memberContext)).getStringValue();
            members.add(member);
        }
        return members;
    }

    private DatabaseName qualifiedNameToDatabaseName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 2) {
            return new DatabaseName(parts.get(0), parts.get(1), qualifiedName.getPos());
        } else if (parts.size() == 1) {
            return new DatabaseName(null, parts.get(0), qualifiedName.getPos());
        } else {
            throw new ParsingException(PARSER_ERROR_MSG.invalidDbFormat(qualifiedName.toString()));
        }
    }

    private TableName qualifiedNameToTableName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.table
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 3) {
            return new TableName(parts.get(0), parts.get(1), parts.get(2), qualifiedName.getPos());
        } else if (parts.size() == 2) {
            return new TableName(null, parts.get(0), parts.get(1), qualifiedName.getPos());
        } else if (parts.size() == 1) {
            return new TableName(null, null, parts.get(0), qualifiedName.getPos());
        } else {
            throw new ParsingException(PARSER_ERROR_MSG.invalidTableFormat(qualifiedName.toString()));
        }
    }
    // add/drop be or cn with warehouse
    @Override
    public ParseNode visitAddBackendClause(StarRocksParser.AddBackendClauseContext context) {
        List<String> backends =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        String whName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (context.warehouseName != null) {
            Identifier identifier = (Identifier) visit(context.identifierOrString());
            whName = identifier.getValue();
        }
        return new AddBackendClause(backends, createPos(context), whName);
    }

    @Override
    public ParseNode visitDropBackendClause(StarRocksParser.DropBackendClauseContext context) {
        List<String> backends =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        String whName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (context.warehouseName != null) {
            Identifier identifier = (Identifier) visit(context.identifierOrString());
            whName = identifier.getValue();
        }
        return new DropBackendClause(backends, context.FORCE() != null, createPos(context), whName);
    }

    @Override
    public ParseNode visitAddComputeNodeClause(StarRocksParser.AddComputeNodeClauseContext context) {
        List<String> hostPorts =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        String whName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (context.warehouseName != null) {
            Identifier identifier = (Identifier) visit(context.identifierOrString());
            whName = identifier.getValue();
        }
        return new AddComputeNodeClause(hostPorts, whName);
    }

    @Override
    public ParseNode visitDropComputeNodeClause(StarRocksParser.DropComputeNodeClauseContext context) {
        List<String> hostPorts =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        String whName = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        if (context.warehouseName != null) {
            Identifier identifier = (Identifier) visit(context.identifierOrString());
            whName = identifier.getValue();
        }
        return new DropComputeNodeClause(hostPorts, createPos(context), whName);
    }

}
