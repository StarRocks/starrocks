// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AddBackendClause;
import com.starrocks.analysis.AddColumnClause;
import com.starrocks.analysis.AddColumnsClause;
import com.starrocks.analysis.AddComputeNodeClause;
import com.starrocks.analysis.AddFollowerClause;
import com.starrocks.analysis.AddObserverClause;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.analysis.AdminShowReplicaStatusStmt;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterLoadStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelLoadStmt;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.ColWithComment;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.ColumnSeparator;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateIndexClause;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DataDescription;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DescribeStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropBackendClause;
import com.starrocks.analysis.DropColumnClause;
import com.starrocks.analysis.DropComputeNodeClause;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFollowerClause;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropIndexClause;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropObserverClause;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionArgsDef;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.InsertTarget;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.KillStmt;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.ListPartitionDesc;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyColumnClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.analysis.ModifyPartitionClause;
import com.starrocks.analysis.ModifyTablePropertiesClause;
import com.starrocks.analysis.MultiItemListPartitionDesc;
import com.starrocks.analysis.MultiRangePartitionDesc;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OdbcScalarFunctionCall;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.PartitionKeyDesc;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.PartitionValue;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.ReorderColumnsClause;
import com.starrocks.analysis.ResourceDesc;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SelectList;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SetNamesVar;
import com.starrocks.analysis.SetPassVar;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.SetUserPropertyVar;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
import com.starrocks.analysis.ShowBrokerStmt;
import com.starrocks.analysis.ShowCharsetStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.ShowCreateTableStmt;
import com.starrocks.analysis.ShowDataStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowDeleteStmt;
import com.starrocks.analysis.ShowDynamicPartitionStmt;
import com.starrocks.analysis.ShowFunctionsStmt;
import com.starrocks.analysis.ShowIndexStmt;
import com.starrocks.analysis.ShowLoadStmt;
import com.starrocks.analysis.ShowLoadWarningsStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowOpenTableStmt;
import com.starrocks.analysis.ShowPartitionsStmt;
import com.starrocks.analysis.ShowProcStmt;
import com.starrocks.analysis.ShowProcesslistStmt;
import com.starrocks.analysis.ShowRoutineLoadStmt;
import com.starrocks.analysis.ShowStatusStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowTabletStmt;
import com.starrocks.analysis.ShowUserPropertyStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.SingleItemListPartitionDesc;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SwapTableClause;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TruncatePartitionClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.TypeDef;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.analysis.ValueList;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStatement;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AnalyzeBasicDesc;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStatement;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.ManualRefreshSchemeDesc;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowProcedureStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.SyncRefreshSchemeDesc;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentifier;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.SetQualifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class AstBuilder extends StarRocksBaseVisitor<ParseNode> {
    private final long sqlMode;

    public AstBuilder(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    @Override
    public ParseNode visitSingleStatement(StarRocksParser.SingleStatementContext context) {
        return visit(context.statement());
    }


    // ---------------------------------------- Database Statement -----------------------------------------------------
    @Override
    public ParseNode visitAlterDbQuotaStmt(StarRocksParser.AlterDbQuotaStmtContext context) {
        String dbName = ((Identifier) visit(context.identifier(0))).getValue();
        if (context.DATA() != null) {
            String quotaValue = ((Identifier) visit(context.identifier(1))).getValue();
            return new AlterDatabaseQuotaStmt(dbName,
                    AlterDatabaseQuotaStmt.QuotaType.DATA,
                    quotaValue);
        } else {
            String quotaValue = context.INTEGER_VALUE().getText();
            return new AlterDatabaseQuotaStmt(dbName,
                    AlterDatabaseQuotaStmt.QuotaType.REPLICA,
                    quotaValue);
        }
    }

    @Override
    public ParseNode visitCreateDbStatement(StarRocksParser.CreateDbStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new CreateDbStmt(
                context.IF() != null,
                dbName);
    }

    @Override
    public ParseNode visitDropDbStatement(StarRocksParser.DropDbStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new DropDbStmt(
                context.IF() != null,
                dbName,
                context.FORCE() != null);
    }

    @Override
    public ParseNode visitShowCreateDbStatement(StarRocksParser.ShowCreateDbStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new ShowCreateDbStmt(dbName);
    }

    @Override
    public ParseNode visitAlterDatabaseRename(StarRocksParser.AlterDatabaseRenameContext context) {
        String dbName = ((Identifier) visit(context.identifier(0))).getValue();
        String newName = ((Identifier) visit(context.identifier(1))).getValue();
        return new AlterDatabaseRename(dbName, newName);
    }

    @Override
    public ParseNode visitRecoverDbStmt(StarRocksParser.RecoverDbStmtContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new RecoverDbStmt(dbName);
    }

    @Override
    public ParseNode visitShowDataStmt(StarRocksParser.ShowDataStmtContext context) {
        if (context.FROM() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            TableName targetTableName = qualifiedNameToTableName(qualifiedName);
            return new ShowDataStmt(targetTableName.getDb(), targetTableName.getTbl());
        } else {
            return new ShowDataStmt(null, null);
        }
    }

    @Override
    public ParseNode visitShowDynamicPartitionStatement(StarRocksParser.ShowDynamicPartitionStatementContext context) {

        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        return new ShowDynamicPartitionStmt(dbName == null ? null : dbName.toString());
    }

    // ------------------------------------------- Table Statement -----------------------------------------------------

    @Override
    public ParseNode visitCreateTableStatement(StarRocksParser.CreateTableStatementContext context) {
        Map<String, String> properties = null;
        if (context.properties() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        Map<String, String> extProperties = null;
        if (context.extProperties() != null) {
            extProperties = new HashMap<>();
            List<Property> propertyList = visit(context.extProperties().properties().property(), Property.class);
            for (Property property : propertyList) {
                extProperties.put(property.getKey(), property.getValue());
            }
        }

        return new CreateTableStmt(
                context.IF() != null,
                context.EXTERNAL() != null,
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName())),
                context.columnDesc() == null ? null : getColumnDefs(context.columnDesc()),
                context.indexDesc() == null ? null : getIndexDefs(context.indexDesc()),
                context.engineDesc() == null ? "olap" :
                        ((Identifier) visit(context.engineDesc().identifier())).getValue(),
                context.charsetDesc() == null ? "utf8" :
                        ((Identifier) visit(context.charsetDesc().identifierOrString())).getValue(),
                context.keyDesc() == null ? null : getKeysDesc(context.keyDesc()),
                context.partitionDesc() == null ? null : getPartitionDesc(context.partitionDesc()),
                context.distributionDesc() == null ? null : (DistributionDesc) visit(context.distributionDesc()),
                properties,
                extProperties,
                context.comment() == null ? null : ((StringLiteral) visit(context.comment().string())).getStringValue(),
                context.rollupDesc() == null ? null : getRollupDesc(context.rollupDesc()));
    }

    @Override
    public ParseNode visitCreateTableLikeStatement(StarRocksParser.CreateTableLikeStatementContext context) {
        return new CreateTableLikeStmt(context.IF() != null,
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName(0))),
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName(1))));
    }

    private PartitionDesc getPartitionDesc(StarRocksParser.PartitionDescContext context) {
        final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
        final List<String> columnList = identifierList.stream().map(Identifier::getValue).collect(toList());
        List<PartitionDesc> partitionDescList = new ArrayList<>();
        PartitionDesc partitionDesc = null;
        if (context.RANGE() != null) {
            for (StarRocksParser.RangePartitionDescContext rangePartitionDescContext : context.rangePartitionDesc()) {
                final PartitionDesc rangePartitionDesc = (PartitionDesc) visit(rangePartitionDescContext);
                partitionDescList.add(rangePartitionDesc);
            }
            partitionDesc = new RangePartitionDesc(columnList, partitionDescList);
        } else if (context.LIST() != null) {
            for (StarRocksParser.ListPartitionDescContext listPartitionDescContext : context.listPartitionDesc()) {
                final PartitionDesc listPartitionDesc = (PartitionDesc) visit(listPartitionDescContext);
                partitionDescList.add(listPartitionDesc);
            }
            partitionDesc = new ListPartitionDesc(columnList, partitionDescList);
        }
        return partitionDesc;
    }

    private List<AlterClause> getRollupDesc(StarRocksParser.RollupDescContext context) {
        List<AlterClause> rollupList = new ArrayList<>();
        for (StarRocksParser.AddRollupClauseContext addRollupClauseContext : context.addRollupClause()) {
            String rollupName = ((Identifier) visit(addRollupClauseContext.identifier())).getValue();
            List<Identifier> columnList =
                    visit(addRollupClauseContext.identifierList().identifier(), Identifier.class);
            List<String> dupKeys = null;
            if (addRollupClauseContext.dupKeys() != null) {
                final List<Identifier> identifierList =
                        visit(addRollupClauseContext.dupKeys().identifierList().identifier(), Identifier.class);
                dupKeys = identifierList.stream().map(Identifier::getValue).collect(toList());
            }
            String baseRollupName = addRollupClauseContext.fromRollup() != null ?
                    ((Identifier) visit(addRollupClauseContext.fromRollup().identifier())).getValue() : null;
            Map<String, String> properties = null;
            if (addRollupClauseContext.properties() != null) {
                properties = new HashMap<>();
                List<Property> propertyList = visit(addRollupClauseContext.properties().property(), Property.class);
                for (Property property : propertyList) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            final AddRollupClause addRollupClause =
                    new AddRollupClause(rollupName, columnList.stream().map(Identifier::getValue).collect(toList()),
                            dupKeys, baseRollupName,
                            properties);
            rollupList.add(addRollupClause);
        }
        return rollupList;
    }

    private KeysDesc getKeysDesc(StarRocksParser.KeyDescContext context) {
        KeysType keysType = null;
        if (null != context.PRIMARY()) {
            keysType = KeysType.PRIMARY_KEYS;
        } else if (null != context.DUPLICATE()) {
            keysType = KeysType.DUP_KEYS;
        } else if (null != context.AGGREGATE()) {
            keysType = KeysType.AGG_KEYS;
        } else if (null != context.UNIQUE()) {
            keysType = KeysType.UNIQUE_KEYS;
        }
        List<Identifier> columnList = visit(context.identifierList().identifier(), Identifier.class);
        return new KeysDesc(keysType, columnList.stream().map(Identifier::getValue).collect(toList()));
    }

    private List<IndexDef> getIndexDefs(List<StarRocksParser.IndexDescContext> indexDesc) {
        List<IndexDef> indexDefList = new ArrayList<>();
        for (StarRocksParser.IndexDescContext context : indexDesc) {
            String indexName = ((Identifier) visit(context.identifier())).getValue();
            List<Identifier> columnList = visit(context.identifierList().identifier(), Identifier.class);
            String comment =
                    context.comment() != null ? ((StringLiteral) visit(context.comment())).getStringValue() : null;
            final IndexDef indexDef =
                    new IndexDef(indexName, columnList.stream().map(Identifier::getValue).collect(toList()),
                            IndexDef.IndexType.BITMAP, comment);
            indexDefList.add(indexDef);
        }
        return indexDefList;
    }

    private List<ColumnDef> getColumnDefs(List<StarRocksParser.ColumnDescContext> columnDesc) {
        return columnDesc.stream().map(context -> getColumnDef(context)).collect(toList());
    }

    private ColumnDef getColumnDef(StarRocksParser.ColumnDescContext context) {
        String columnName = ((Identifier) visit(context.identifier())).getValue();
        TypeDef typeDef = new TypeDef(getType(context.type()));
        String charsetName = context.charsetName() != null ?
                ((Identifier) visit(context.charsetName().identifier())).getValue() : null;
        boolean isKey = context.KEY() != null;
        AggregateType aggregateType =
                context.aggDesc() != null ? AggregateType.valueOf(context.aggDesc().getText().toUpperCase()) : null;
        Boolean isAllowNull = null;
        if (context.NOT() != null && context.NULL() != null) {
            isAllowNull = false;
        } else if (context.NULL() != null) {
            isAllowNull = true;
        }
        ColumnDef.DefaultValueDef defaultValueDef = ColumnDef.DefaultValueDef.NOT_SET;
        final StarRocksParser.DefaultDescContext defaultDescContext = context.defaultDesc();
        if (defaultDescContext != null) {
            if (defaultDescContext.string() != null) {
                String value = ((StringLiteral) visit(defaultDescContext.string())).getStringValue();
                defaultValueDef = new ColumnDef.DefaultValueDef(true, new StringLiteral(value));
            } else if (defaultDescContext.NULL() != null) {
                defaultValueDef = ColumnDef.DefaultValueDef.NULL_DEFAULT_VALUE;
            } else if (defaultDescContext.CURRENT_TIMESTAMP() != null) {
                defaultValueDef = ColumnDef.DefaultValueDef.CURRENT_TIMESTAMP_VALUE;
            }
        }
        String comment = context.comment() == null ? "" :
                ((StringLiteral) visit(context.comment().string())).getStringValue();
        return new ColumnDef(columnName, typeDef, charsetName, isKey, aggregateType, isAllowNull, defaultValueDef,
                comment);
    }

    @Override
    public ParseNode visitCreateTableAsSelectStatement(StarRocksParser.CreateTableAsSelectStatementContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        CreateTableStmt createTableStmt = new CreateTableStmt(
                context.IF() != null,
                false,
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName())),
                null,
                "olap",
                null,
                context.partitionDesc() == null ? null : (PartitionDesc) visit(context.partitionDesc()),
                context.distributionDesc() == null ? null : (DistributionDesc) visit(context.distributionDesc()),
                properties,
                null,
                context.comment() == null ? null :
                        ((StringLiteral) visit(context.comment().string())).getStringValue());

        List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
        return new CreateTableAsSelectStmt(
                createTableStmt,
                columns == null ? null : columns.stream().map(Identifier::getValue).collect(toList()),
                (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitAddComputeNodeClause(StarRocksParser.AddComputeNodeClauseContext context) {
        List<String> hostPorts =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new AddComputeNodeClause(hostPorts);
    }

    @Override
    public ParseNode visitDropComputeNodeClause(StarRocksParser.DropComputeNodeClauseContext context) {
        List<String> hostPorts =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new DropComputeNodeClause(hostPorts);
    }

    @Override
    public ParseNode visitSwapTableClause(StarRocksParser.SwapTableClauseContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        return new SwapTableClause(identifier.getValue());
    }

    @Override
    public ParseNode visitModifyTablePropertiesClause(StarRocksParser.ModifyTablePropertiesClauseContext context) {
        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.propertyList().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        return new ModifyTablePropertiesClause(properties);
    }

    @Override
    public ParseNode visitShowComputeNodes(StarRocksParser.ShowComputeNodesContext context) {
        return new ShowComputeNodesStmt();
    }

    @Override
    public ParseNode visitAddPartitionClause(StarRocksParser.AddPartitionClauseContext context) {
        boolean temporary = context.TEMPORARY() != null;
        PartitionDesc partitionDesc = null;
        if (context.singleRangePartition() != null) {
            partitionDesc = (PartitionDesc) visitSingleRangePartition(context.singleRangePartition());
        } else if (context.multiRangePartition() != null) {
            partitionDesc = (PartitionDesc) visitMultiRangePartition(context.multiRangePartition());
        }
        DistributionDesc distributionDesc = null;
        if (context.distributionDesc() != null) {
            distributionDesc = (DistributionDesc) visitDistributionDesc(context.distributionDesc());
        }
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new AddPartitionClause(partitionDesc, distributionDesc, properties, temporary);
    }

    @Override
    public ParseNode visitAlterTableStatement(StarRocksParser.AlterTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        List<AlterClause> alterClauses = visit(context.alterClause(), AlterClause.class);
        return new AlterTableStmt(targetTableName, alterClauses);
    }

    @Override
    public ParseNode visitCancelAlterTableStatement(StarRocksParser.CancelAlterTableStatementContext context) {
        ShowAlterStmt.AlterType alterType;
        if (context.ROLLUP() != null) {
            alterType = ShowAlterStmt.AlterType.ROLLUP;
        } else if (context.MATERIALIZED() != null && context.VIEW() != null) {
            alterType = ShowAlterStmt.AlterType.MATERIALIZED_VIEW;
        } else {
            alterType = ShowAlterStmt.AlterType.COLUMN;
        }
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName dbTableName = qualifiedNameToTableName(qualifiedName);

        List<Long> alterJobIdList = null;
        if (context.INTEGER_VALUE() != null) {
            alterJobIdList = context.INTEGER_VALUE()
                    .stream().map(ParseTree::getText).map(Long::parseLong).collect(toList());
        }
        return new CancelAlterTableStmt(alterType, dbTableName, alterJobIdList == null ? null : alterJobIdList);
    }

    @Override
    public ParseNode visitRecoverTableStatement(StarRocksParser.RecoverTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        return new RecoverTableStmt(tableName);
    }

    @Override
    public ParseNode visitDropTableStatement(StarRocksParser.DropTableStatementContext context) {
        boolean ifExists = context.IF() != null && context.EXISTS() != null;
        boolean force = context.FORCE() != null;
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new DropTableStmt(ifExists, targetTableName, force);
    }

    @Override
    public ParseNode visitShowTableStatement(StarRocksParser.ShowTableStatementContext context) {
        boolean isVerbose = context.FULL() != null;
        String database = null;
        if (context.qualifiedName() != null) {
            database = getQualifiedName(context.qualifiedName()).toString();
        }

        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowTableStmt(database, isVerbose, stringLiteral.getValue());
        } else if (context.expression() != null) {
            return new ShowTableStmt(database, isVerbose, null, (Expr) visit(context.expression()));
        } else {
            return new ShowTableStmt(database, isVerbose, null);
        }
    }

    @Override
    public ParseNode visitShowTabletStatement(StarRocksParser.ShowTabletStatementContext context) {
        if (context.INTEGER_VALUE() != null) {
            return new ShowTabletStmt(null, Long.parseLong(context.INTEGER_VALUE().getText()));
        } else {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            TableName dbTblName = qualifiedNameToTableName(qualifiedName);
            PartitionNames partitionNames = null;
            if (context.partitionNames() != null) {
                partitionNames = (PartitionNames) visit(context.partitionNames());
            }
            Expr where = null;
            if (context.expression() != null) {
                where = (Expr) visit(context.expression());
            }
            List<OrderByElement> orderByElements = null;
            if (context.ORDER() != null) {
                orderByElements = new ArrayList<>();
                orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
            }
            LimitElement limitElement = null;
            if (context.limitElement() != null) {
                limitElement = (LimitElement) visit(context.limitElement());
            }
            return new ShowTabletStmt(dbTblName, -1L, partitionNames, where, orderByElements, limitElement);
        }
    }

    @Override
    public ParseNode visitShowAlterStatement(StarRocksParser.ShowAlterStatementContext context) {
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }
        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }
        ShowAlterStmt.AlterType alterType;
        if (context.ROLLUP() != null) {
            alterType = ShowAlterStmt.AlterType.ROLLUP;
        } else if (context.MATERIALIZED() != null && context.VIEW() != null) {
            alterType = ShowAlterStmt.AlterType.MATERIALIZED_VIEW;
        } else {
            alterType = ShowAlterStmt.AlterType.COLUMN;
        }
        List<OrderByElement> orderByElements = null;
        if (context.ORDER() != null) {
            orderByElements = new ArrayList<>();
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }
        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }
        return new ShowAlterStmt(alterType, dbName == null ? null : dbName.toString(), where, orderByElements,
                limitElement);
    }

    @Override
    public ParseNode visitShowCreateTableStatement(StarRocksParser.ShowCreateTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        if (context.MATERIALIZED() != null && context.VIEW() != null) {
            return new ShowCreateTableStmt(targetTableName, ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW);
        }
        if (context.VIEW() != null) {
            return new ShowCreateTableStmt(targetTableName, ShowCreateTableStmt.CreateTableType.VIEW);
        }
        return new ShowCreateTableStmt(targetTableName, ShowCreateTableStmt.CreateTableType.TABLE);
    }

    @Override
    public ParseNode visitShowDeleteStatement(StarRocksParser.ShowDeleteStatementContext context) {
        QualifiedName dbName = null;
        if (context.qualifiedName() != null) {
            dbName = getQualifiedName(context.db);
        }
        return new ShowDeleteStmt(dbName == null ? null : dbName.toString());
    }

    @Override
    public ParseNode visitCreateIndexStatement(StarRocksParser.CreateIndexStatementContext context) {
        String indexName = ((Identifier) visit(context.identifier())).getValue();
        List<Identifier> columnList = visit(context.identifierList().identifier(), Identifier.class);
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }

        IndexDef indexDef = new IndexDef(indexName,
                columnList.stream().map(Identifier::getValue).collect(toList()),
                IndexDef.IndexType.BITMAP,
                comment);

        CreateIndexClause createIndexClause = new CreateIndexClause(null, indexDef, false);

        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new AlterTableStmt(targetTableName, Lists.newArrayList(createIndexClause));
    }

    @Override
    public ParseNode visitDropIndexStatement(StarRocksParser.DropIndexStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        DropIndexClause dropIndexClause = new DropIndexClause(identifier.getValue(), null, false);

        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new AlterTableStmt(targetTableName, Lists.newArrayList(dropIndexClause));
    }

    @Override
    public ParseNode visitShowColumnStatement(StarRocksParser.ShowColumnStatementContext context) {
        QualifiedName tableName = getQualifiedName(context.table);

        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowColumnStmt(qualifiedNameToTableName(tableName),
                dbName == null ? null : dbName.toString(),
                pattern,
                context.FULL() != null,
                where);
    }

    @Override
    public ParseNode visitShowIndexStatement(StarRocksParser.ShowIndexStatementContext context) {
        QualifiedName tableName = getQualifiedName(context.table);
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        return new ShowIndexStmt(dbName == null ? null : dbName.toString(),
                qualifiedNameToTableName(tableName));
    }

    @Override
    public ParseNode visitShowTableStatusStatement(StarRocksParser.ShowTableStatusStatementContext context) {
        QualifiedName dbName = null;
        if (context.qualifiedName() != null) {
            dbName = getQualifiedName(context.db);
        }

        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowTableStatusStmt(dbName == null ? null : dbName.toString(), pattern, where);
    }

    @Override
    public ParseNode visitRefreshTableStatement(StarRocksParser.RefreshTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        List<String> partitionNames = null;
        if (context.string() != null) {
            partitionNames = context.string().stream()
                    .map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        }
        return new RefreshTableStmt(targetTableName, partitionNames);
    }

    @Override
    public ParseNode visitDropPartitionClause(StarRocksParser.DropPartitionClauseContext context) {
        String partitionName = ((Identifier) visit(context.identifier())).getValue();
        boolean temp = context.TEMPORARY() != null;
        boolean force = context.FORCE() != null;
        boolean exists = context.EXISTS() != null;
        return new DropPartitionClause(exists, partitionName, temp, force);
    }

    @Override
    public ParseNode visitTruncatePartitionClause(StarRocksParser.TruncatePartitionClauseContext context) {
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new TruncatePartitionClause(partitionNames);
    }

    @Override
    public ParseNode visitModifyPartitionClause(StarRocksParser.ModifyPartitionClauseContext context) {
        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        if (context.identifier() != null) {
            final String partitionName = ((Identifier) visit(context.identifier())).getValue();
            return new ModifyPartitionClause(Collections.singletonList(partitionName), properties);
        } else if (context.identifierList() != null) {
            final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            return new ModifyPartitionClause(identifierList.stream().map(Identifier::getValue).collect(toList()),
                    properties);
        } else if (context.ASTERISK_SYMBOL() != null) {
            return ModifyPartitionClause.createStarClause(properties);
        }
        return null;
    }

    @Override
    public ParseNode visitShowPartitionsStatement(StarRocksParser.ShowPartitionsStatementContext context) {
        boolean temp = context.TEMPORARY() != null;
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }

        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }
        return new ShowPartitionsStmt(tableName, where, orderByElements, limitElement, temp);
    }

    @Override
    public ParseNode visitShowOpenTableStatement(StarRocksParser.ShowOpenTableStatementContext context) {
        return new ShowOpenTableStmt();
    }

    @Override
    public ParseNode visitAddColumnClause(StarRocksParser.AddColumnClauseContext context) {
        ColumnDef columnDef = getColumnDef(context.columnDesc());
        ColumnPosition columnPosition = null;
        if (context.FIRST() != null) {
            columnPosition = ColumnPosition.FIRST;
        } else if (context.AFTER() != null) {
            String afterColumnName = getIdentifierName(context.identifier(0));
            columnPosition = new ColumnPosition(afterColumnName);
        }
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new AddColumnClause(columnDef, columnPosition, rollupName, getProperties(context.properties()));
    }

    @Override
    public ParseNode visitAddColumnsClause(StarRocksParser.AddColumnsClauseContext context) {
        List<ColumnDef> columnDefs = getColumnDefs(context.columnDesc());
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new AddColumnsClause(columnDefs, rollupName, getProperties(context.properties()));
    }

    @Override
    public ParseNode visitDropColumnClause(StarRocksParser.DropColumnClauseContext context) {
        String columnName = getIdentifierName(context.identifier(0));
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new DropColumnClause(columnName, rollupName, getProperties(context.properties()));
    }

    @Override
    public ParseNode visitModifyColumnClause(StarRocksParser.ModifyColumnClauseContext context) {
        ColumnDef columnDef = getColumnDef(context.columnDesc());
        ColumnPosition columnPosition = null;
        if (context.FIRST() != null) {
            columnPosition = ColumnPosition.FIRST;
        } else if (context.AFTER() != null) {
            String afterColumnName = getIdentifierName(context.identifier(0));
            columnPosition = new ColumnPosition(afterColumnName);
        }
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new ModifyColumnClause(columnDef, columnPosition, rollupName, getProperties(context.properties()));
    }

    @Override
    public ParseNode visitColumnRenameClause(StarRocksParser.ColumnRenameClauseContext context) {
        String oldColumnName = getIdentifierName(context.oldColumn);
        String newColumnName = getIdentifierName(context.newColumn);
        return new ColumnRenameClause(oldColumnName, newColumnName);
    }

    @Override
    public ParseNode visitReorderColumnsClause(StarRocksParser.ReorderColumnsClauseContext context) {
        List<String> cols = context.identifierList().identifier().stream().map(this::getIdentifierName).collect(toList());
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new ReorderColumnsClause(cols, rollupName, getProperties(context.properties()));
    }

    private Map<String, String> getProperties(StarRocksParser.PropertiesContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context != null && context.property() != null) {
            List<Property> propertyList = visit(context.property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return properties;
    }
    // ------------------------------------------- View Statement ------------------------------------------------------

    @Override
    public ParseNode visitCreateViewStatement(StarRocksParser.CreateViewStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);

        List<ColWithComment> colWithComments = null;
        if (context.columnNameWithComment().size() > 0) {
            colWithComments = visit(context.columnNameWithComment(), ColWithComment.class);
        }
        return new CreateViewStmt(
                context.IF() != null,
                targetTableName,
                colWithComments,
                context.comment() == null ? null : ((StringLiteral) visit(context.comment())).getStringValue(),
                (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitAlterViewStatement(StarRocksParser.AlterViewStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);

        List<ColWithComment> colWithComments = null;
        if (context.columnNameWithComment().size() > 0) {
            colWithComments = visit(context.columnNameWithComment(), ColWithComment.class);
        }

        return new AlterViewStmt(targetTableName, colWithComments, (QueryStatement) visit(context.queryStatement()));
    }

    @Override
    public ParseNode visitDropViewStatement(StarRocksParser.DropViewStatementContext context) {
        boolean ifExists = context.IF() != null && context.EXISTS() != null;
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new DropTableStmt(ifExists, targetTableName, true, false);
    }

    // ------------------------------------------- Task Statement ------------------------------------------------------

    @Override
    public ParseNode visitSubmitTaskStatement(StarRocksParser.SubmitTaskStatementContext context) {
        QualifiedName qualifiedName = null;
        if (context.qualifiedName() != null) {
            qualifiedName = getQualifiedName(context.qualifiedName());
        }
        Map<String, String> properties = new HashMap<>();
        if (context.setVarHint() != null) {
            for (StarRocksParser.SetVarHintContext hintContext : context.setVarHint()) {
                for (StarRocksParser.HintMapContext hintMapContext : hintContext.hintMap()) {
                    properties.put(hintMapContext.k.getText(),
                            ((LiteralExpr) visit(hintMapContext.v)).getStringValue());
                }
            }
        }
        CreateTableAsSelectStmt createTableAsSelectStmt =
                (CreateTableAsSelectStmt) visit(context.createTableAsSelectStatement());
        int startIndex = context.createTableAsSelectStatement().start.getStartIndex();
        if (qualifiedName == null) {
            return new SubmitTaskStmt(null, null,
                    properties, startIndex, createTableAsSelectStmt);
        } else if (qualifiedName.getParts().size() == 1) {
            return new SubmitTaskStmt(null, qualifiedName.getParts().get(0),
                    properties, startIndex, createTableAsSelectStmt);
        } else if (qualifiedName.getParts().size() == 2) {
            return new SubmitTaskStmt(qualifiedName.getParts().get(0),
                    qualifiedName.getParts().get(1), properties, startIndex, createTableAsSelectStmt);
        } else {
            throw new ParsingException("error task name ");
        }
    }

    // ------------------------------------------- Materialized View Statement -----------------------------------------

    public static final ImmutableList<String> MATERIALIZEDVIEW_REFRESHSCHEME_SUPPORT_UNIT_IDENTIFIERS =
            new ImmutableList.Builder<String>()
                    .add("SECOND").add("MINUTE").add("HOUR").add("DAY")
                    .build();

    private boolean checkMaterializedViewAsyncRefreshSchemeUnitIdentifier(
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc) {
        if (asyncRefreshSchemeDesc.getIntervalLiteral() == null ||
                asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier() == null) {
            return true;
        }
        String description = asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription();
        if (StringUtils.isEmpty(description)) {
            return true;
        }
        return MATERIALIZEDVIEW_REFRESHSCHEME_SUPPORT_UNIT_IDENTIFIERS.contains(description);
    }

    @Override
    public ParseNode visitCreateMaterializedViewStatement(
            StarRocksParser.CreateMaterializedViewStatementContext context) {
        boolean ifNotExist = context.IF() != null;
        QualifiedName qualifiedName = getQualifiedName(context.mvName);
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        String comment =
                context.comment() == null ? null : ((StringLiteral) visit(context.comment().string())).getStringValue();
        QueryStatement queryStatement = (QueryStatement) visit(context.queryStatement());

        // process properties
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        // process refresh
        RefreshSchemeDesc refreshSchemeDesc;
        if (context.refreshSchemeDesc() == null) {
            if (context.distributionDesc() == null) {
                // use old materialized index
                refreshSchemeDesc = new SyncRefreshSchemeDesc();
            } else {
                // use new manual refresh
                refreshSchemeDesc = new ManualRefreshSchemeDesc();
            }
        } else {
            refreshSchemeDesc = ((RefreshSchemeDesc) visit(context.refreshSchemeDesc()));
        }
        if (refreshSchemeDesc instanceof SyncRefreshSchemeDesc) {
            if (context.primaryExpression() != null) {
                throw new IllegalArgumentException(
                        "Partition by is not supported by SYNC refresh type int materialized view");
            }
            if (context.distributionDesc() != null) {
                throw new IllegalArgumentException(
                        "Distribution by is not supported by SYNC refresh type in materialized view");
            }
            return new CreateMaterializedViewStmt(tableName.getTbl(), queryStatement, properties);
        }
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            if (!checkMaterializedViewAsyncRefreshSchemeUnitIdentifier(asyncRefreshSchemeDesc)) {
                throw new IllegalArgumentException(
                        "CreateMaterializedView UnitIdentifier only support 'SECOND','MINUTE','HOUR' or 'DAY'");
            }

        }
        if (!Config.enable_experimental_mv) {
            throw new IllegalArgumentException("The experimental mv is disabled, " +
                    "you can set FE config enable_experimental_mv=true to enable it.");
        }
        // process partition
        ExpressionPartitionDesc expressionPartitionDesc = null;
        if (context.primaryExpression() != null) {
            Expr expr = (Expr) visit(context.primaryExpression());
            if (expr instanceof SlotRef) {
                expressionPartitionDesc = new ExpressionPartitionDesc(expr);
            } else if (expr instanceof FunctionCallExpr) {
                for (Expr child : expr.getChildren()) {
                    if (child instanceof SlotRef) {
                        expressionPartitionDesc = new ExpressionPartitionDesc(expr);
                        break;
                    }
                }
                if (expressionPartitionDesc == null) {
                    throw new IllegalArgumentException(
                            "Partition exp not supports:" + expr.toSql());
                }
            } else {
                throw new IllegalArgumentException(
                        "Partition exp not supports:" + expr.toSql());
            }
        }
        // process distribution
        DistributionDesc distributionDesc =
                context.distributionDesc() == null ? null : (DistributionDesc) visit(context.distributionDesc());
        return new CreateMaterializedViewStatement(tableName, ifNotExist, comment,
                refreshSchemeDesc, expressionPartitionDesc, distributionDesc, properties, queryStatement);
    }

    @Override
    public ParseNode visitShowMaterializedViewStatement(StarRocksParser.ShowMaterializedViewStatementContext context) {
        String database = null;
        if (context.qualifiedName() != null) {
            database = getQualifiedName(context.qualifiedName()).toString();
        }
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowMaterializedViewStmt(database, stringLiteral.getValue());
        } else if (context.expression() != null) {
            return new ShowMaterializedViewStmt(database, (Expr) visit(context.expression()));
        } else {
            return new ShowMaterializedViewStmt(database);
        }
    }

    @Override
    public ParseNode visitDropMaterializedViewStatement(StarRocksParser.DropMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        return new DropMaterializedViewStmt(context.IF() != null, mvName);
    }

    @Override
    public ParseNode visitAlterMaterializedViewStatement(
            StarRocksParser.AlterMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        String newMvName = null;
        if (context.tableRenameClause() != null) {
            newMvName = ((Identifier) visit(context.tableRenameClause().identifier())).getValue();
        }
        //process refresh
        RefreshSchemeDesc refreshSchemeDesc = null;
        if (context.refreshSchemeDesc() != null) {
            refreshSchemeDesc = ((RefreshSchemeDesc) visit(context.refreshSchemeDesc()));
        }
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            if (!checkMaterializedViewAsyncRefreshSchemeUnitIdentifier(asyncRefreshSchemeDesc)) {
                throw new IllegalArgumentException(
                        "AlterMaterializedView UnitIdentifier only support 'SECOND','MINUTE','HOUR' or 'DAY'");
            }
        }
        return new AlterMaterializedViewStatement(mvName, newMvName, refreshSchemeDesc);
    }

    @Override
    public ParseNode visitRefreshMaterializedViewStatement(
            StarRocksParser.RefreshMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        return new RefreshMaterializedViewStatement(mvName);
    }

    @Override
    public ParseNode visitCancelRefreshMaterializedViewStatement(
            StarRocksParser.CancelRefreshMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        return new CancelRefreshMaterializedViewStatement(mvName);
    }

    // ------------------------------------------- Cluster Management Statement -----------------------------------------

    @Override
    public ParseNode visitAlterSystemStatement(StarRocksParser.AlterSystemStatementContext context) {
        return new AlterSystemStmt((AlterClause) visit(context.alterClause()));
    }

    // ------------------------------------------- Catalog Statement ---------------------------------------------------

    @Override
    public ParseNode visitCreateExternalCatalogStatement(
            StarRocksParser.CreateExternalCatalogStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String catalogName = identifier.getValue();
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new CreateCatalogStmt(catalogName, comment, properties);
    }

    @Override
    public ParseNode visitDropExternalCatalogStatement(StarRocksParser.DropExternalCatalogStatementContext context) {
        Identifier identifier = (Identifier) visit(context.catalogName);
        String catalogName = identifier.getValue();
        return new DropCatalogStmt(catalogName);
    }

    @Override
    public ParseNode visitShowCatalogsStatement(StarRocksParser.ShowCatalogsStatementContext context) {
        return new ShowCatalogsStmt();
    }

    // ------------------------------------------- Alter Clause --------------------------------------------------------

    @Override
    public ParseNode visitCreateIndexClause(StarRocksParser.CreateIndexClauseContext context) {
        String indexName = ((Identifier) visit(context.identifier())).getValue();
        List<Identifier> columnList = visit(context.identifierList().identifier(), Identifier.class);
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }

        IndexDef indexDef = new IndexDef(indexName,
                columnList.stream().map(Identifier::getValue).collect(toList()),
                IndexDef.IndexType.BITMAP,
                comment);

        return new CreateIndexClause(null, indexDef, true);
    }

    @Override
    public ParseNode visitDropIndexClause(StarRocksParser.DropIndexClauseContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        return new DropIndexClause(identifier.getValue(), null, true);
    }

    @Override
    public ParseNode visitTableRenameClause(StarRocksParser.TableRenameClauseContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        return new TableRenameClause(identifier.getValue());
    }

    @Override
    public ParseNode visitAdminSetReplicaStatus(StarRocksParser.AdminSetReplicaStatusContext context) {
        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.properties().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        return new AdminSetReplicaStatusStmt(properties);
    }

    @Override
    public ParseNode visitAddBackendClause(StarRocksParser.AddBackendClauseContext context) {
        List<String> backends =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new AddBackendClause(backends);
    }

    @Override
    public ParseNode visitDropBackendClause(StarRocksParser.DropBackendClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new DropBackendClause(clusters, context.FORCE() != null);
    }

    @Override
    public ParseNode visitModifyBackendHostClause(StarRocksParser.ModifyBackendHostClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new ModifyBackendAddressClause(clusters.get(0), clusters.get(1));
    }

    @Override
    public ParseNode visitAddFrontendClause(StarRocksParser.AddFrontendClauseContext context) {
        String cluster = ((StringLiteral) visit(context.string())).getStringValue();
        if (context.FOLLOWER() != null) {
            return new AddFollowerClause(cluster);
        } else if (context.OBSERVER() != null) {
            return new AddObserverClause(cluster);
        } else {
            Preconditions.checkState(false, "frontend clause error.");
            return null;
        }
    }

    @Override
    public ParseNode visitDropFrontendClause(StarRocksParser.DropFrontendClauseContext context) {
        String cluster = ((StringLiteral) visit(context.string())).getStringValue();
        if (context.FOLLOWER() != null) {
            return new DropFollowerClause(cluster);
        } else if (context.OBSERVER() != null) {
            return new DropObserverClause(cluster);
        } else {
            Preconditions.checkState(false, "frontend clause error.");
            return null;
        }
    }

    @Override
    public ParseNode visitModifyFrontendHostClause(StarRocksParser.ModifyFrontendHostClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new ModifyFrontendAddressClause(clusters.get(0), clusters.get(1));
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------
    @Override
    public ParseNode visitInsertStatement(StarRocksParser.InsertStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }

        QueryStatement queryStatement;
        if (context.VALUES() != null) {
            List<ValueList> rowValues = visit(context.expressionsWithDefault(), ValueList.class);
            List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getFirstRow).collect(toList());

            List<String> colNames = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); ++i) {
                colNames.add("column_" + i);
            }

            queryStatement = new QueryStatement(new ValuesRelation(rows, colNames));
        } else {
            queryStatement = (QueryStatement) visit(context.queryStatement());
        }

        List<String> targetColumnNames = null;
        if (context.columnAliases() != null) {
            // StarRocks tables are not case-sensitive, so targetColumnNames are converted
            // to lowercase characters to facilitate subsequent matching.
            List<Identifier> targetColumnNamesIdentifiers =
                    visitIfPresent(context.columnAliases().identifier(), Identifier.class);
            if (targetColumnNamesIdentifiers != null) {
                targetColumnNames = targetColumnNamesIdentifiers.stream()
                        .map(Identifier::getValue).map(String::toLowerCase).collect(toList());
            }
        }
        if (context.explainDesc() != null) {
            queryStatement.setIsExplain(true, getExplainType(context.explainDesc()));
        }

        return new InsertStmt(
                new InsertTarget(targetTableName, partitionNames),
                context.label == null ? null : ((Identifier) visit(context.label)).getValue(),
                targetColumnNames,
                queryStatement,
                Lists.newArrayList(),
                context.OVERWRITE() != null ? true : false);
    }

    @Override
    public ParseNode visitUpdateStatement(StarRocksParser.UpdateStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        List<ColumnAssignment> assignments = visit(context.assignmentList().assignment(), ColumnAssignment.class);
        Expr where = context.where != null ? (Expr) visit(context.where) : null;
        UpdateStmt ret = new UpdateStmt(targetTableName, assignments, where);
        if (context.explainDesc() != null) {
            ret.setIsExplain(true, getExplainType(context.explainDesc()));
        }
        return ret;
    }

    @Override
    public ParseNode visitDeleteStatement(StarRocksParser.DeleteStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        Expr where = context.where != null ? (Expr) visit(context.where) : null;
        DeleteStmt ret = new DeleteStmt(targetTableName, partitionNames, where);
        if (context.explainDesc() != null) {
            ret.setIsExplain(true, getExplainType(context.explainDesc()));
        }
        return ret;
    }

    @Override
    public ParseNode visitAlterLoadStatement(StarRocksParser.AlterLoadStatementContext context) {
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        String name = ((Identifier) visit(context.name)).getValue();
        Map<String, String> jobProperties = getJobProperties(context.jobProperties());

        return new AlterLoadStmt(new LabelName(dbName == null ? null : dbName.toString(), name),
                jobProperties);
    }

    // ------------------------------------------- Routine Statement ---------------------------------------------------
    @Override
    public ParseNode visitStopRoutineLoadStatement(StarRocksParser.StopRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = context.db.getText();
        }
        String name = null;
        if (context.name != null) {
            name = context.name.getText();
        }
        return new StopRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitResumeRoutineLoadStatement(StarRocksParser.ResumeRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = context.db.getText();
        }
        String name = null;
        if (context.name != null) {
            name = context.name.getText();
        }
        return new ResumeRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitPauseRoutineLoadStatement(StarRocksParser.PauseRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = context.db.getText();
        }
        String name = null;
        if (context.name != null) {
            name = context.name.getText();
        }
        return new PauseRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitShowRoutineLoadStatement(StarRocksParser.ShowRoutineLoadStatementContext context) {
        boolean isVerbose = context.ALL() != null;
        String database = null;
        if (context.db != null) {
            database = context.db.getText();
        }
        String name = null;
        if (context.name != null) {
            name = context.name.getText();
        }
        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }
        List<OrderByElement> orderByElements = null;
        if (context.ORDER() != null) {
            orderByElements = new ArrayList<>();
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }
        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }
        return new ShowRoutineLoadStmt(new LabelName(database, name), isVerbose, where, orderByElements, limitElement);
    }

    // ------------------------------------------- Analyze Statement ---------------------------------------------------

    @Override
    public ParseNode visitAnalyzeStatement(StarRocksParser.AnalyzeStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);

        List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
        List<String> columnNames = null;
        if (columns != null) {
            columnNames = columns.stream().map(Identifier::getValue).collect(toList());
        }

        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        return new AnalyzeStmt(tableName, columnNames, properties,
                context.SAMPLE() != null,
                context.ASYNC() != null,
                new AnalyzeBasicDesc());
    }

    @Override
    public ParseNode visitDropStatsStatement(StarRocksParser.DropStatsStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        return new DropStatsStmt(tableName);
    }

    @Override
    public ParseNode visitCreateAnalyzeStatement(StarRocksParser.CreateAnalyzeStatementContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        if (context.DATABASE() != null) {
            return new CreateAnalyzeJobStmt(((Identifier) visit(context.db)).getValue(), context.FULL() == null,
                    properties);
        } else if (context.TABLE() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            TableName tableName = qualifiedNameToTableName(qualifiedName);

            List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
            List<String> columnNames = null;
            if (columns != null) {
                columnNames = columns.stream().map(Identifier::getValue).collect(toList());
            }

            return new CreateAnalyzeJobStmt(tableName, columnNames, context.SAMPLE() != null, properties);
        } else {
            return new CreateAnalyzeJobStmt(context.FULL() == null, properties);
        }
    }

    @Override
    public ParseNode visitDropAnalyzeJobStatement(StarRocksParser.DropAnalyzeJobStatementContext context) {
        return new DropAnalyzeJobStmt(Long.parseLong(context.INTEGER_VALUE().getText()));
    }

    @Override
    public ParseNode visitShowAnalyzeStatement(StarRocksParser.ShowAnalyzeStatementContext context) {
        Predicate predicate = null;
        if (context.expression() != null) {
            predicate = (Predicate) visit(context.expression());
        }

        if (context.STATUS() != null) {
            return new ShowAnalyzeStatusStmt(predicate);
        } else if (context.JOB() != null) {
            return new ShowAnalyzeJobStmt(predicate);
        } else {
            return new ShowAnalyzeJobStmt(predicate);
        }
    }

    @Override
    public ParseNode visitShowStatsMetaStatement(StarRocksParser.ShowStatsMetaStatementContext context) {
        Predicate predicate = null;
        if (context.expression() != null) {
            predicate = (Predicate) visit(context.expression());
        }

        return new ShowBasicStatsMetaStmt(predicate);
    }

    @Override
    public ParseNode visitShowHistogramMetaStatement(StarRocksParser.ShowHistogramMetaStatementContext context) {
        Predicate predicate = null;
        if (context.expression() != null) {
            predicate = (Predicate) visit(context.expression());
        }

        return new ShowHistogramStatsMetaStmt(predicate);
    }

    @Override
    public ParseNode visitAnalyzeHistogramStatement(StarRocksParser.AnalyzeHistogramStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);

        List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
        List<String> columnNames = null;
        if (columns != null) {
            columnNames = columns.stream().map(Identifier::getValue).collect(toList());
        }

        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        long bucket;
        if (context.bucket != null) {
            bucket = Long.parseLong(context.bucket.getText());
        } else {
            bucket = Config.histogram_buckets_size;
        }

        return new AnalyzeStmt(tableName, columnNames, properties, true,
                context.ASYNC() != null, new AnalyzeHistogramDesc(bucket));
    }

    @Override
    public ParseNode visitDropHistogramStatement(StarRocksParser.DropHistogramStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);

        List<Identifier> columns = visitIfPresent(context.identifier(), Identifier.class);
        List<String> columnNames = null;
        if (columns != null) {
            columnNames = columns.stream().map(Identifier::getValue).collect(toList());
        }

        return new DropHistogramStmt(tableName, columnNames);
    }

    @Override
    public ParseNode visitKillAnalyzeStatement(StarRocksParser.KillAnalyzeStatementContext context) {
        return new KillAnalyzeStmt(Long.parseLong(context.INTEGER_VALUE().getText()));
    }

    // ------------------------------------------- Resource Group Statement -------------------------------------------------

    public ParseNode visitCreateResourceGroupStatement(StarRocksParser.CreateResourceGroupStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        String name = identifier.getValue();

        List<List<Predicate>> predicatesList = new ArrayList<>();
        for (StarRocksParser.ClassifierContext classifierContext : context.classifier()) {
            List<Predicate> p = visit(classifierContext.expression(), Predicate.class);
            predicatesList.add(p);
        }

        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }

        return new CreateResourceGroupStmt(name,
                context.EXISTS() != null,
                context.REPLACE() != null,
                predicatesList,
                properties);
    }

    @Override
    public ParseNode visitDropResourceGroupStatement(StarRocksParser.DropResourceGroupStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        return new DropResourceGroupStmt(identifier.getValue());
    }

    @Override
    public ParseNode visitAlterResourceGroupStatement(StarRocksParser.AlterResourceGroupStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        String name = identifier.getValue();
        if (context.ADD() != null) {
            List<List<Predicate>> predicatesList = new ArrayList<>();
            for (StarRocksParser.ClassifierContext classifierContext : context.classifier()) {
                List<Predicate> p = visit(classifierContext.expression(), Predicate.class);
                predicatesList.add(p);
            }

            return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.AddClassifiers(predicatesList));
        } else if (context.DROP() != null) {
            if (context.ALL() != null) {
                return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.DropAllClassifiers());
            } else {
                return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.DropClassifiers(context.INTEGER_VALUE()
                        .stream().map(ParseTree::getText).map(Long::parseLong).collect(toList())));
            }
        } else {
            Map<String, String> properties = new HashMap<>();
            List<Property> propertyList = visit(context.property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }

            return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.AlterProperties(properties));
        }
    }

    @Override
    public ParseNode visitShowResourceGroupStatement(StarRocksParser.ShowResourceGroupStatementContext context) {
        if (context.GROUPS() != null) {
            return new ShowResourceGroupStmt(null, context.ALL() != null);
        } else {
            Identifier identifier = (Identifier) visit(context.identifier());
            return new ShowResourceGroupStmt(identifier.getValue(), false);
        }
    }

    // ------------------------------------------- Query Statement -----------------------------------------------------

    @Override
    public ParseNode visitQueryStatement(StarRocksParser.QueryStatementContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.queryBody());
        QueryStatement queryStatement = new QueryStatement(queryRelation);
        if (context.outfile() != null) {
            queryStatement.setOutFileClause((OutFileClause) visit(context.outfile()));
        }

        if (context.explainDesc() != null) {
            queryStatement.setIsExplain(true, getExplainType(context.explainDesc()));
        }

        return queryStatement;
    }

    @Override
    public ParseNode visitQueryBody(StarRocksParser.QueryBodyContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.queryNoWith());

        List<CTERelation> withQuery = new ArrayList<>();
        if (context.withClause() != null) {
            withQuery = visit(context.withClause().commonTableExpression(), CTERelation.class);
        }
        withQuery.forEach(queryRelation::addCTERelation);

        return queryRelation;
    }

    @Override
    public ParseNode visitCommonTableExpression(StarRocksParser.CommonTableExpressionContext context) {
        List<Identifier> columns = null;
        if (context.columnAliases() != null) {
            columns = visit(context.columnAliases().identifier(), Identifier.class);
        }

        List<String> columnNames = null;
        if (columns != null) {
            columnNames = columns.stream().map(Identifier::getValue).collect(toList());
        }

        QueryRelation queryRelation = (QueryRelation) visit(context.queryBody());
        // Regenerate cteID when generating plan
        return new CTERelation(
                RelationId.of(queryRelation).hashCode(),
                ((Identifier) visit(context.name)).getValue(),
                columnNames,
                new QueryStatement(queryRelation));
    }

    @Override
    public ParseNode visitQueryNoWith(StarRocksParser.QueryNoWithContext context) {

        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }

        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }

        QueryRelation term = (QueryRelation) visit(context.queryTerm());
        term.setOrderBy(orderByElements);
        term.setLimit(limitElement);
        return term;
    }

    @Override
    public ParseNode visitSetOperation(StarRocksParser.SetOperationContext context) {
        QueryRelation left = (QueryRelation) visit(context.left);
        QueryRelation right = (QueryRelation) visit(context.right);

        boolean distinct = true;
        if (context.setQuantifier() != null) {
            if (context.setQuantifier().DISTINCT() != null) {
                distinct = true;
            } else if (context.setQuantifier().ALL() != null) {
                distinct = false;
            }
        }

        SetQualifier setQualifier = distinct ? SetQualifier.DISTINCT : SetQualifier.ALL;
        switch (context.operator.getType()) {
            case StarRocksLexer.UNION:
                if (left instanceof UnionRelation && ((UnionRelation) left).getQualifier().equals(setQualifier)) {
                    ((UnionRelation) left).addRelation(right);
                    return left;
                } else {
                    return new UnionRelation(Lists.newArrayList(left, right), setQualifier);
                }
            case StarRocksLexer.INTERSECT:
                if (left instanceof IntersectRelation &&
                        ((IntersectRelation) left).getQualifier().equals(setQualifier)) {
                    ((IntersectRelation) left).addRelation(right);
                    return left;
                } else {
                    return new IntersectRelation(Lists.newArrayList(left, right), setQualifier);
                }
            case StarRocksLexer.EXCEPT:
            case StarRocksLexer.MINUS:
                if (left instanceof ExceptRelation && ((ExceptRelation) left).getQualifier().equals(setQualifier)) {
                    ((ExceptRelation) left).addRelation(right);
                    return left;
                } else {
                    return new ExceptRelation(Lists.newArrayList(left, right), setQualifier);
                }
        }
        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public ParseNode visitQuerySpecification(StarRocksParser.QuerySpecificationContext context) {
        Relation from = null;
        List<SelectListItem> selectItems = visit(context.selectItem(), SelectListItem.class);

        if (context.fromClause() instanceof StarRocksParser.DualContext) {
            if (selectItems.stream().anyMatch(SelectListItem::isStar)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
        } else {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            if (fromContext.relations() != null) {
                List<Relation> relations = visit(fromContext.relations().relation(), Relation.class);
                Iterator<Relation> iterator = relations.iterator();
                Relation relation = iterator.next();
                while (iterator.hasNext()) {
                    relation = new JoinRelation(null, relation, iterator.next(), null, false);
                }
                from = relation;
            }
        }

        /*
          from == null means a statement without from or from dual, add a single row of null values here,
          so that the semantics are the same, and the processing of subsequent query logic can be simplified,
          such as select sum(1) or select sum(1) from dual, will be converted to select sum(1) from (values(null)) t.
          This can share the same logic as select sum(1) from table
         */
        if (from == null) {
            ArrayList<Expr> row = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            row.add(NullLiteral.create(Type.NULL));
            columnNames.add("");
            List<ArrayList<Expr>> rows = new ArrayList<>();
            rows.add(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, columnNames);
            valuesRelation.setNullValues(true);
            from = valuesRelation;
        }

        boolean isDistinct = context.setQuantifier() != null && context.setQuantifier().DISTINCT() != null;
        SelectList selectList = new SelectList(selectItems, isDistinct);
        if (context.setVarHint() != null) {
            Map<String, String> selectHints = new HashMap<>();
            for (StarRocksParser.SetVarHintContext hintContext : context.setVarHint()) {
                for (StarRocksParser.HintMapContext hintMapContext : hintContext.hintMap()) {
                    selectHints.put(hintMapContext.k.getText(),
                            ((LiteralExpr) visit(hintMapContext.v)).getStringValue());
                }
            }
            selectList.setOptHints(selectHints);
        }
        return new SelectRelation(
                selectList,
                from,
                (Expr) visitIfPresent(context.where),
                (GroupByClause) visitIfPresent(context.groupingElement()),
                (Expr) visitIfPresent(context.having));
    }

    @Override
    public ParseNode visitSelectSingle(StarRocksParser.SelectSingleContext context) {
        String alias = null;
        if (context.identifier() != null) {
            alias = ((Identifier) visit(context.identifier())).getValue();
        } else if (context.string() != null) {
            alias = ((StringLiteral) visit(context.string())).getStringValue();
        }

        return new SelectListItem((Expr) visit(context.expression()), alias);
    }

    @Override
    public ParseNode visitSelectAll(StarRocksParser.SelectAllContext context) {
        if (context.qualifiedName() != null) {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            return new SelectListItem(qualifiedNameToTableName(qualifiedName));
        }
        return new SelectListItem(null);
    }

    @Override
    public ParseNode visitSingleGroupingSet(StarRocksParser.SingleGroupingSetContext context) {
        return new GroupByClause(new ArrayList<>(visit(context.expression(), Expr.class)),
                GroupByClause.GroupingType.GROUP_BY);
    }

    @Override
    public ParseNode visitRollup(StarRocksParser.RollupContext context) {
        List<Expr> groupingExprs = visit(context.expression(), Expr.class);
        return new GroupByClause(new ArrayList<>(groupingExprs), GroupByClause.GroupingType.ROLLUP);
    }

    @Override
    public ParseNode visitCube(StarRocksParser.CubeContext context) {
        List<Expr> groupingExprs = visit(context.expression(), Expr.class);
        return new GroupByClause(new ArrayList<>(groupingExprs), GroupByClause.GroupingType.CUBE);
    }

    @Override
    public ParseNode visitMultipleGroupingSets(StarRocksParser.MultipleGroupingSetsContext context) {
        List<ArrayList<Expr>> groupingSets = new ArrayList<>();
        for (StarRocksParser.GroupingSetContext groupingSetContext : context.groupingSet()) {
            List<Expr> l = visit(groupingSetContext.expression(), Expr.class);
            groupingSets.add(new ArrayList<>(l));
        }

        return new GroupByClause(groupingSets, GroupByClause.GroupingType.GROUPING_SETS);
    }

    @Override
    public ParseNode visitGroupingOperation(StarRocksParser.GroupingOperationContext context) {
        List<Expr> arguments = visit(context.expression(), Expr.class);
        return new GroupingFunctionCallExpr("grouping", arguments);
    }

    @Override
    public ParseNode visitWindowFrame(StarRocksParser.WindowFrameContext context) {
        if (context.end != null) {
            return new AnalyticWindow(
                    getFrameType(context.frameType),
                    (AnalyticWindow.Boundary) visit(context.start),
                    (AnalyticWindow.Boundary) visit(context.end));
        } else {
            return new AnalyticWindow(
                    getFrameType(context.frameType),
                    (AnalyticWindow.Boundary) visit(context.start));
        }
    }

    private static AnalyticWindow.Type getFrameType(Token type) {
        switch (type.getType()) {
            case StarRocksLexer.RANGE:
                return AnalyticWindow.Type.RANGE;
            case StarRocksLexer.ROWS:
                return AnalyticWindow.Type.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    @Override
    public ParseNode visitUnboundedFrame(StarRocksParser.UnboundedFrameContext context) {
        return new AnalyticWindow.Boundary(getUnboundedFrameBoundType(context.boundType), null);
    }

    @Override
    public ParseNode visitBoundedFrame(StarRocksParser.BoundedFrameContext context) {
        return new AnalyticWindow.Boundary(getBoundedFrameBoundType(context.boundType),
                (Expr) visit(context.expression()));
    }

    @Override
    public ParseNode visitCurrentRowBound(StarRocksParser.CurrentRowBoundContext context) {
        return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
    }

    private static AnalyticWindow.BoundaryType getBoundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.PRECEDING:
                return AnalyticWindow.BoundaryType.PRECEDING;
            case StarRocksLexer.FOLLOWING:
                return AnalyticWindow.BoundaryType.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static AnalyticWindow.BoundaryType getUnboundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.PRECEDING:
                return AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING;
            case StarRocksLexer.FOLLOWING:
                return AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    @Override
    public ParseNode visitSortItem(StarRocksParser.SortItemContext context) {
        return new OrderByElement(
                (Expr) visit(context.expression()),
                getOrderingType(context.ordering),
                getNullOrderingType(getOrderingType(context.ordering), context.nullOrdering));
    }

    private boolean getNullOrderingType(boolean isAsc, Token token) {
        if (token == null) {
            return (!SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_SORT_NULLS_LAST)) == isAsc;
        }
        switch (token.getType()) {
            case StarRocksLexer.FIRST:
                return true;
            case StarRocksLexer.LAST:
                return false;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static boolean getOrderingType(Token token) {
        if (token == null) {
            return true;
        }
        switch (token.getType()) {
            case StarRocksLexer.ASC:
                return true;
            case StarRocksLexer.DESC:
                return false;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    @Override
    public ParseNode visitLimitElement(StarRocksParser.LimitElementContext context) {
        long limit = Long.parseLong(context.limit.getText());
        long offset = 0;
        if (context.offset != null) {
            offset = Long.parseLong(context.offset.getText());
        }
        return new LimitElement(offset, limit);
    }

    @Override
    public ParseNode visitRelation(StarRocksParser.RelationContext context) {
        Relation relation = (Relation) visit(context.relationPrimary());
        List<JoinRelation> joinRelations = visit(context.joinRelation(), JoinRelation.class);

        Relation leftChildRelation = relation;
        for (JoinRelation joinRelation : joinRelations) {
            joinRelation.setLeft(leftChildRelation);
            leftChildRelation = joinRelation;
        }
        return leftChildRelation;
    }

    @Override
    public ParseNode visitParenthesizedRelation(StarRocksParser.ParenthesizedRelationContext context) {
        if (context.relations().relation().size() == 1) {
            return visit(context.relations().relation().get(0));
        } else {
            List<Relation> relations = visit(context.relations().relation(), Relation.class);
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();
            while (iterator.hasNext()) {
                relation = new JoinRelation(null, relation, iterator.next(), null, false);
            }
            return relation;
        }
    }

    @Override
    public ParseNode visitTableAtom(StarRocksParser.TableAtomContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }

        List<Long> tabletIds = Lists.newArrayList();
        if (context.tabletList() != null) {
            tabletIds = context.tabletList().INTEGER_VALUE().stream().map(ParseTree::getText)
                    .map(Long::parseLong).collect(toList());
        }

        TableRelation tableRelation = new TableRelation(tableName, partitionNames, tabletIds);
        if (context.bracketHint() != null) {
            for (Identifier identifier : visit(context.bracketHint().identifier(), Identifier.class)) {
                if (identifier.getValue().equals("_META_")) {
                    tableRelation.setMetaQuery(true);
                }
            }
        }

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return tableRelation;
    }

    @Override
    public ParseNode visitJoinRelation(StarRocksParser.JoinRelationContext context) {
        // Because left recursion is required to parse the leftmost atom table first.
        // Therefore, the parsed result does not contain the information of the left table,
        // which is temporarily assigned to Null,
        // and the information of the left table will be filled in visitRelation
        Relation left = null;
        Relation right = (Relation) visit(context.rightRelation);

        JoinOperator joinType = JoinOperator.INNER_JOIN;
        if (context.crossOrInnerJoinType() != null) {
            if (context.crossOrInnerJoinType().CROSS() != null) {
                joinType = JoinOperator.CROSS_JOIN;
            } else {
                joinType = JoinOperator.INNER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().LEFT() != null) {
            if (context.outerAndSemiJoinType().OUTER() != null) {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            } else if (context.outerAndSemiJoinType().SEMI() != null) {
                joinType = JoinOperator.LEFT_SEMI_JOIN;
            } else if (context.outerAndSemiJoinType().ANTI() != null) {
                joinType = JoinOperator.LEFT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.LEFT_OUTER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().RIGHT() != null) {
            if (context.outerAndSemiJoinType().OUTER() != null) {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            } else if (context.outerAndSemiJoinType().SEMI() != null) {
                joinType = JoinOperator.RIGHT_SEMI_JOIN;
            } else if (context.outerAndSemiJoinType().ANTI() != null) {
                joinType = JoinOperator.RIGHT_ANTI_JOIN;
            } else {
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
            }
        } else if (context.outerAndSemiJoinType().FULL() != null) {
            joinType = JoinOperator.FULL_OUTER_JOIN;
        }

        Expr predicate = null;
        List<String> usingColNames = null;
        if (context.joinCriteria() != null) {
            if (context.joinCriteria().ON() != null) {
                predicate = (Expr) visit(context.joinCriteria().expression());
            } else if (context.joinCriteria().USING() != null) {
                List<Identifier> criteria = visit(context.joinCriteria().identifier(), Identifier.class);
                usingColNames = criteria.stream().map(Identifier::getValue).collect(Collectors.toList());
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        JoinRelation joinRelation = new JoinRelation(joinType, left, right, predicate, context.LATERAL() != null);
        joinRelation.setUsingColNames(usingColNames);
        if (context.bracketHint() != null) {
            joinRelation.setJoinHint(((Identifier) visit(context.bracketHint().identifier().get(0))).getValue());
        }

        return joinRelation;
    }

    @Override
    public ParseNode visitInlineTable(StarRocksParser.InlineTableContext context) {
        List<ValueList> rowValues = visit(context.rowConstructor(), ValueList.class);
        List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getFirstRow).collect(toList());

        List<String> colNames = new ArrayList<>();
        for (int i = 0; i < rows.get(0).size(); ++i) {
            colNames.add("column_" + i);
        }

        ValuesRelation valuesRelation = new ValuesRelation(rows, colNames);

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            valuesRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return valuesRelation;
    }

    @Override
    public ParseNode visitTableFunction(StarRocksParser.TableFunctionContext context) {
        TableFunctionRelation tableFunctionRelation = new TableFunctionRelation(
                getQualifiedName(context.qualifiedName()).toString(),
                new FunctionParams(false, visit(context.expression(), Expr.class)));

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableFunctionRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return tableFunctionRelation;
    }

    @Override
    public ParseNode visitRowConstructor(StarRocksParser.RowConstructorContext context) {
        ArrayList<Expr> row = new ArrayList<>(visit(context.expression(), Expr.class));
        return new ValueList(row);
    }

    @Override
    public ParseNode visitPartitionNames(StarRocksParser.PartitionNamesContext context) {
        List<Identifier> identifierList = visit(context.identifier(), Identifier.class);
        return new PartitionNames(context.TEMPORARY() != null,
                identifierList.stream().map(Identifier::getValue).collect(toList()));
    }

    @Override
    public ParseNode visitSubquery(StarRocksParser.SubqueryContext context) {
        return new SubqueryRelation(new QueryStatement((QueryRelation) visit(context.queryBody())));
    }

    @Override
    public ParseNode visitSubqueryPrimary(StarRocksParser.SubqueryPrimaryContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        return subqueryRelation.getQueryStatement().getQueryRelation();
    }

    @Override
    public ParseNode visitSubqueryRelation(StarRocksParser.SubqueryRelationContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            subqueryRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        return subqueryRelation;
    }

    @Override
    public ParseNode visitSubqueryExpression(StarRocksParser.SubqueryExpressionContext context) {
        SubqueryRelation subqueryRelation = (SubqueryRelation) visit(context.subquery());
        return new Subquery(subqueryRelation.getQueryStatement());
    }

    @Override
    public ParseNode visitInSubquery(StarRocksParser.InSubqueryContext context) {
        boolean isNotIn = context.NOT() != null;
        QueryRelation query = (QueryRelation) visit(context.queryBody());

        return new InPredicate((Expr) visit(context.value), new Subquery(new QueryStatement(query)), isNotIn);
    }

    @Override
    public ParseNode visitExists(StarRocksParser.ExistsContext context) {
        QueryRelation query = (QueryRelation) visit(context.queryBody());
        return new ExistsPredicate(new Subquery(new QueryStatement(query)), false);
    }

    @Override
    public ParseNode visitScalarSubquery(StarRocksParser.ScalarSubqueryContext context) {
        BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0))
                .getSymbol());
        Subquery subquery = new Subquery(new QueryStatement((QueryRelation) visit(context.queryBody())));
        return new BinaryPredicate(op, (Expr) visit(context.booleanExpression()), subquery);
    }

    @Override
    public ParseNode visitShowFunctionsStatement(StarRocksParser.ShowFunctionsStatementContext context) {
        boolean isBuiltIn = context.BUILTIN() != null;
        boolean isVerbose = context.FULL() != null;

        String dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db).toString();
        }

        String pattern = null;
        if (context.pattern != null) {
            pattern = ((StringLiteral) visit(context.pattern)).getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowFunctionsStmt(dbName, isBuiltIn, isVerbose, pattern, where);
    }

    @Override
    public ParseNode visitDropFunctionStatement(StarRocksParser.DropFunctionStatementContext context) {
        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();

        return new DropFunctionStmt(FunctionName.createFnName(functionName),
                getFunctionArgsDef(context.typeList()));
    }

    @Override
    public ParseNode visitCreateFunctionStatement(StarRocksParser.CreateFunctionStatementContext context) {
        String functionType = "SCALAR";
        if (context.functionType != null) {
            functionType = context.functionType.getText();
        }
        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();

        TypeDef returnTypeDef = new TypeDef(getType(context.returnType));
        TypeDef intermediateType = null;
        if (context.intermediateType != null) {
            intermediateType = new TypeDef(getType(context.intermediateType));
        }

        Map<String, String> properties = null;
        if (context.properties() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new CreateFunctionStmt(functionType, FunctionName.createFnName(functionName),
                getFunctionArgsDef(context.typeList()), returnTypeDef, intermediateType, properties);
    }

    // ------------------------------------------- Privilege Statement -------------------------------------------------

    @Override
    public ParseNode visitCreateUser(StarRocksParser.CreateUserContext context) {
        UserDesc userDesc;
        UserIdentifier user = (UserIdentifier) visit(context.user());
        UserAuthOption authOption = context.authOption() == null ? null : (UserAuthOption) visit(context.authOption());
        if (authOption == null) {
            userDesc = new UserDesc(user.getUserIdentity());
        } else if (authOption.getAuthPlugin() == null) {
            userDesc = new UserDesc(user.getUserIdentity(), authOption.getPassword(), authOption.isPasswordPlain());
        } else {
            userDesc = new UserDesc(user.getUserIdentity(), authOption.getAuthPlugin(), authOption.getAuthString(),
                    authOption.isPasswordPlain());
        }
        boolean ifNotExists = context.IF() != null;
        String userRole = context.string() == null ? null : ((StringLiteral) visit(context.string())).getStringValue();
        return new CreateUserStmt(ifNotExists, userDesc, userRole);
    }

    @Override
    public ParseNode visitAlterUser(StarRocksParser.AlterUserContext context) {
        UserDesc userDesc;
        UserIdentifier user = (UserIdentifier) visit(context.user());
        UserAuthOption authOption = (UserAuthOption) visit(context.authOption());
        if (authOption.getAuthPlugin() == null) {
            userDesc = new UserDesc(user.getUserIdentity(), authOption.getPassword(), authOption.isPasswordPlain());
        } else {
            userDesc = new UserDesc(user.getUserIdentity(), authOption.getAuthPlugin(), authOption.getAuthString(),
                    authOption.isPasswordPlain());
        }
        return new AlterUserStmt(userDesc);
    }

    @Override
    public ParseNode visitAuthWithoutPlugin(StarRocksParser.AuthWithoutPluginContext context) {
        String password = ((StringLiteral) visit(context.string())).getStringValue();
        boolean isPasswordPlain = context.PASSWORD() == null;
        return new UserAuthOption(password, null, null, isPasswordPlain);
    }

    @Override
    public ParseNode visitAuthWithPlugin(StarRocksParser.AuthWithPluginContext context) {
        Identifier authPlugin = (Identifier) visit(context.identifierOrString());
        String authString = context.string() == null ? null : ((StringLiteral) visit(context.string())).getStringValue();
        boolean isPasswordPlain = context.AS() == null;
        return new UserAuthOption(null, authPlugin.getValue().toUpperCase(), authString, isPasswordPlain);
    }

    // ------------------------------------------- Load Statement ------------------------------------------------------

    @Override
    public ParseNode visitLoadStatement(StarRocksParser.LoadStatementContext context) {
        LabelName label = getLabelName(context.labelName());
        List<DataDescription> dataDescriptions = null;
        if (context.data != null) {
            dataDescriptions = context.data.dataDesc().stream().map(this::getDataDescription)
                    .collect(toList());
        }
        Map<String, String> properties = null;
        if (context.props != null) {
            properties = Maps.newHashMap();
            List<Property> propertyList = visit(context.props.property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        if (context.resource != null) {
            ResourceDesc resourceDesc = getResourceDesc(context.resource);
            return new LoadStmt(label, dataDescriptions, resourceDesc, properties);
        }
        BrokerDesc brokerDesc = getBrokerDesc(context.broker);
        String cluster = null;
        if (context.system != null) {
            cluster = ((Identifier) visit(context.system)).getValue();
        }
        return new LoadStmt(label, dataDescriptions, brokerDesc, cluster, properties);
    }

    private LabelName getLabelName(StarRocksParser.LabelNameContext context) {
        String label = ((Identifier) visit(context.label)).getValue();
        String db = "";
        if (context.db != null) {
            db = ((Identifier) visit(context.db)).getValue();
        }
        return new LabelName(db, label);
    }

    private DataDescription getDataDescription(StarRocksParser.DataDescContext context) {
        String dstTableName = ((Identifier) visit(context.dstTableName)).getValue();
        PartitionNames partitionNames = (PartitionNames) visitIfPresent(context.partitions);
        Expr whereExpr = (Expr) visitIfPresent(context.where);
        List<Expr> colMappingList = null;
        if (context.colMappingList != null) {
            colMappingList = visit(context.colMappingList.expression(), Expr.class);
        }
        if (context.srcTableName != null) {
            String srcTableName = ((Identifier) visit(context.srcTableName)).getValue();
            return new DataDescription(dstTableName, partitionNames, srcTableName,
                    context.NEGATIVE() != null, colMappingList, whereExpr);
        }
        List<String> files = context.srcFiles.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue())
                .collect(toList());
        ColumnSeparator colSep = getColumnSeparator(context.colSep);
        String format = null;
        if (context.format != null) {
            if (context.format.identifier() != null) {
                format = ((Identifier) visit(context.format.identifier())).getValue();
            } else if (context.format.string() != null) {
                format = ((StringLiteral) visit(context.format.string())).getStringValue();
            }
        }
        List<String> colList = null;
        if (context.colList != null) {
            List<Identifier> identifiers = visit(context.colList.identifier(), Identifier.class);
            colList = identifiers.stream().map(Identifier::getValue).collect(toList());
        }
        List<String> colFromPath = null;
        if (context.colFromPath != null) {
            List<Identifier> identifiers = visit(context.colFromPath.identifier(), Identifier.class);
            colFromPath = identifiers.stream().map(Identifier::getValue).collect(toList());
        }
        return new DataDescription(dstTableName, partitionNames, files, colList, colSep, null, format,
                colFromPath, context.NEGATIVE() != null, colMappingList, whereExpr);
    }

    private ColumnSeparator getColumnSeparator(StarRocksParser.StringContext context) {
        if (context != null) {
            String sep = ((StringLiteral) visit(context)).getValue();
            return new ColumnSeparator(sep);
        }
        return null;
    }

    private BrokerDesc getBrokerDesc(StarRocksParser.BrokerDescContext context) {
        if (context != null) {
            Map<String, String> properties = null;
            if (context.props != null) {
                properties = Maps.newHashMap();
                List<Property> propertyList = visit(context.props.property(), Property.class);
                for (Property property : propertyList) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            if (context.identifierOrString() != null) {
                String brokerName = ((Identifier) visit(context.identifierOrString())).getValue();
                return new BrokerDesc(brokerName, properties);
            } else {
                return new BrokerDesc(properties);
            }

        }
        return null;
    }

    private ResourceDesc getResourceDesc(StarRocksParser.ResourceDescContext context) {
        if (context != null) {
            String brokerName = ((Identifier) visit(context.identifierOrString())).getValue();
            Map<String, String> properties = null;
            if (context.props != null) {
                properties = Maps.newHashMap();
                List<Property> propertyList = visit(context.props.property(), Property.class);
                for (Property property : propertyList) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            return new ResourceDesc(brokerName, properties);
        }
        return null;
    }

    @Override
    public ParseNode visitShowLoadStatement(StarRocksParser.ShowLoadStatementContext context) {
        String db = null;
        if (context.identifier() != null) {
            db = ((Identifier) visit(context.identifier())).getValue();
        }
        Expr labelExpr = null;
        if (context.expression() != null) {
            labelExpr = (Expr) visit(context.expression());
        }
        List<OrderByElement> orderByElements = null;
        if (context.ORDER() != null) {
            orderByElements = new ArrayList<>();
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }
        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }
        return new ShowLoadStmt(db, labelExpr, orderByElements, limitElement);
    }

    @Override
    public ParseNode visitShowLoadWarningsStatement(StarRocksParser.ShowLoadWarningsStatementContext context) {
        if (context.ON() != null) {
            String url = ((StringLiteral) visit(context.string())).getValue();
            return new ShowLoadWarningsStmt(null, url, null, null);
        }
        String db = null;
        if (context.identifier() != null) {
            db = ((Identifier) visit(context.identifier())).getValue();
        }
        Expr labelExpr = null;
        if (context.expression() != null) {
            labelExpr = (Expr) visit(context.expression());
        }
        LimitElement limitElement = null;
        if (context.limitElement() != null) {
            limitElement = (LimitElement) visit(context.limitElement());
        }
        return new ShowLoadWarningsStmt(db, null, labelExpr, limitElement);
    }

    @Override
    public ParseNode visitCancelLoadStatement(StarRocksParser.CancelLoadStatementContext context) {
        String db = null;
        if (context.identifier() != null) {
            db = ((Identifier) visit(context.identifier())).getValue();
        }
        Expr labelExpr = null;
        if (context.expression() != null) {
            labelExpr = (Expr) visit(context.expression());
        }
        return new CancelLoadStmt(db, labelExpr);
    }

    // ------------------------------------------- Other Statement -----------------------------------------------------

    @Override
    public ParseNode visitShowDatabasesStatement(StarRocksParser.ShowDatabasesStatementContext context) {
        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }

        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowDbStmt(stringLiteral.getValue(), catalog);
        } else if (context.expression() != null) {
            return new ShowDbStmt(null, (Expr) visit(context.expression()), catalog);
        } else {
            return new ShowDbStmt(null, null, catalog);
        }
    }

    @Override
    public ParseNode visitUseDb(StarRocksParser.UseDbContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 1) {
            return new UseDbStmt(null, parts.get(0));
        } else if (parts.size() == 2) {
            return new UseDbStmt(parts.get(0), parts.get(1));
        } else {
            throw new ParsingException("error catalog.database");
        }
    }

    public ParseNode visitUseCatalog(StarRocksParser.UseCatalogContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String catalogName = identifier.getValue();
        return new UseCatalogStmt(catalogName);
    }

    @Override
    public ParseNode visitShowUserPropertyStatement(StarRocksParser.ShowUserPropertyStatementContext context) {
        String user;
        String pattern;
        if (context.FOR() == null) {
            user = null;
            pattern = context.LIKE() == null ? null : ((StringLiteral) visit(context.string(0))).getValue();
        } else {
            user = ((StringLiteral) visit(context.string(0))).getValue();
            pattern = context.LIKE() == null ? null : ((StringLiteral) visit(context.string(1))).getValue();
        }
        return new ShowUserPropertyStmt(user, pattern);
    }

    @Override
    public ParseNode visitSetUserPropertyStatement(StarRocksParser.SetUserPropertyStatementContext context) {
        String user = context.FOR() == null ? null : ((StringLiteral) visit(context.string())).getValue();
        List<SetVar> list = new ArrayList<>();
        if (context.userPropertyList() != null) {
            List<Property> propertyList = visit(context.userPropertyList().property(), Property.class);
            for (Property property : propertyList) {
                SetVar setVar = new SetUserPropertyVar(property.getKey(), property.getValue());
                list.add(setVar);
            }
        }
        return new SetUserPropertyStmt(user, list);
    }

    @Override
    public ParseNode visitKillStatement(StarRocksParser.KillStatementContext context) {
        long id = Long.parseLong(context.INTEGER_VALUE().getText());
        if (context.QUERY() != null) {
            return new KillStmt(false, id);
        } else {
            return new KillStmt(true, id);
        }
    }

    @Override
    public ParseNode visitShowStatusStatement(StarRocksParser.ShowStatusStatementContext context) {
        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowStatusStmt(getVariableType(context.varType()), pattern, where);
    }

    @Override
    public ParseNode visitAdminSetConfig(StarRocksParser.AdminSetConfigContext context) {
        Map<String, String> configs = new HashMap<>();
        Property property = (Property) visitProperty(context.property());
        String configKey = property.getKey();
        String configValue = property.getValue();
        configs.put(configKey, configValue);
        return new AdminSetConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, configs);
    }

    @Override
    public ParseNode visitSetStatement(StarRocksParser.SetStatementContext context) {
        List<SetVar> propertyList = visit(context.setVar(), SetVar.class);
        return new SetStmt(propertyList);
    }

    @Override
    public ParseNode visitSetVariable(StarRocksParser.SetVariableContext context) {
        if (context.userVariable() != null) {
            VariableExpr variableDesc = (VariableExpr) visit(context.userVariable());
            Expr expr = (Expr) visit(context.expression());
            return new UserVariable(variableDesc.getName(), expr);
        } else if (context.systemVariable() != null) {
            VariableExpr variableDesc = (VariableExpr) visit(context.systemVariable());
            Expr expr = (Expr) visit(context.setExprOrDefault());
            return new SetVar(variableDesc.getSetType(), variableDesc.getName(), expr);
        } else {
            Expr expr = (Expr) visit(context.setExprOrDefault());
            String variable = ((Identifier) visit(context.identifier())).getValue();
            if (context.varType() != null) {
                return new SetVar(getVariableType(context.varType()), variable, expr);
            } else {
                return new SetVar(variable, expr);
            }
        }
    }

    @Override
    public ParseNode visitSetNames(StarRocksParser.SetNamesContext context) {
        if (context.CHAR() != null || context.CHARSET() != null) {
            if (context.identifierOrString().isEmpty()) {
                return new SetNamesVar(null);
            } else {
                return new SetNamesVar(((Identifier) visit(context.identifierOrString().get(0))).getValue());
            }
        } else {
            String charset = null;
            if (context.charset != null) {
                charset = ((Identifier) visit(context.charset)).getValue();
            }
            String collate = null;
            if (context.collate != null) {
                collate = ((Identifier) visit(context.collate)).getValue();
            }

            return new SetNamesVar(charset, collate);
        }
    }

    @Override
    public ParseNode visitSetPassword(StarRocksParser.SetPasswordContext context) {
        String passwordText;
        StringLiteral stringLiteral = (StringLiteral) visit(context.string());
        if (context.PASSWORD().size() > 1) {
            passwordText = new String(MysqlPassword.makeScrambledPassword(stringLiteral.getStringValue()));
        } else {
            passwordText = stringLiteral.getStringValue();
        }
        if (context.user() != null) {
            UserIdentifier userIdentifier = (UserIdentifier) visit(context.user());
            return new SetPassVar(userIdentifier.getUserIdentity(), passwordText);
        } else {
            return new SetPassVar(null, passwordText);
        }
    }

    @Override
    public ParseNode visitSetExprOrDefault(StarRocksParser.SetExprOrDefaultContext context) {
        if (context.DEFAULT() != null) {
            return null;
        } else if (context.ON() != null) {
            return new StringLiteral("ON");
        } else if (context.ALL() != null) {
            return new StringLiteral("ALL");
        } else {
            return visit(context.expression());
        }
    }

    @Override
    public ParseNode visitAdminShowConfig(StarRocksParser.AdminShowConfigContext context) {
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new AdminShowConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, stringLiteral.getValue());
        }
        return new AdminShowConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, null);
    }

    @Override
    public ParseNode visitAdminShowReplicaDistribution(StarRocksParser.AdminShowReplicaDistributionContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new AdminShowReplicaDistributionStmt(new TableRef(targetTableName, null, partitionNames));
    }

    @Override
    public ParseNode visitAdminShowReplicaStatus(StarRocksParser.AdminShowReplicaStatusContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        Expr where = context.where != null ? (Expr) visit(context.where) : null;
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new AdminShowReplicaStatusStmt(new TableRef(targetTableName, null, partitionNames), where);
    }

    @Override
    public ParseNode visitTruncateTableStatement(StarRocksParser.TruncateTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new TruncateTableStmt(new TableRef(targetTableName, null, partitionNames));
    }

    @Override
    public ParseNode visitGrantRole(StarRocksParser.GrantRoleContext context) {
        UserIdentifier user = (UserIdentifier) visit(context.user());
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        return new GrantRoleStmt(identifier.getValue(), user.getUserIdentity());
    }

    @Override
    public ParseNode visitRevokeRole(StarRocksParser.RevokeRoleContext context) {
        UserIdentifier user = (UserIdentifier) visit(context.user());
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        return new RevokeRoleStmt(identifier.getValue(), user.getUserIdentity());
    }

    @Override
    public ParseNode visitGrantImpersonate(StarRocksParser.GrantImpersonateContext context) {
        UserIdentity securedUser = ((UserIdentifier) visit(context.user(0))).getUserIdentity();
        if (context.user(1) != null) {
            UserIdentity authorizedUser = ((UserIdentifier) visit(context.user(1))).getUserIdentity();
            return new GrantImpersonateStmt(authorizedUser, securedUser);
        } else {
            String roleName = ((Identifier) visit(context.identifierOrString())).getValue();
            return new GrantImpersonateStmt(roleName, securedUser);
        }
    }

    @Override
    public ParseNode visitRevokeImpersonate(StarRocksParser.RevokeImpersonateContext context) {
        UserIdentity securedUser = ((UserIdentifier) visit(context.user(0))).getUserIdentity();
        if (context.user(1) != null) {
            UserIdentity authorizedUser = ((UserIdentifier) visit(context.user(1))).getUserIdentity();
            return new RevokeImpersonateStmt(authorizedUser, securedUser);
        } else {
            String roleName = ((Identifier) visit(context.identifierOrString())).getValue();
            return new RevokeImpersonateStmt(roleName, securedUser);
        }
    }

    @Override
    public ParseNode visitExecuteAs(StarRocksParser.ExecuteAsContext context) {
        UserIdentity toUser = ((UserIdentifier) visit(context.user())).getUserIdentity();
        boolean allowRevert = context.WITH() == null;
        // we only support WITH NO REVERT for now
        return new ExecuteAsStmt(toUser, allowRevert);
    }

    @Override
    public ParseNode visitShowAllAuthentication(StarRocksParser.ShowAllAuthenticationContext context) {
        return new ShowAuthenticationStmt(null, true);
    }

    @Override
    public ParseNode visitShowAuthenticationForUser(StarRocksParser.ShowAuthenticationForUserContext context) {
        if (context.user() != null) {
            UserIdentity user = ((UserIdentifier) visit(context.user())).getUserIdentity();
            return new ShowAuthenticationStmt(user, false);
        } else {
            return new ShowAuthenticationStmt(null, false);
        }
    }

    @Override
    public ParseNode visitShowVariablesStatement(StarRocksParser.ShowVariablesStatementContext context) {
        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowVariablesStmt(getVariableType(context.varType()), pattern, where);
    }

    @Override
    public ParseNode visitShowProcesslistStatement(StarRocksParser.ShowProcesslistStatementContext context) {
        boolean isShowFull = context.FULL() != null;
        return new ShowProcesslistStmt(isShowFull);
    }

    @Override
    public ParseNode visitShowBrokerStatement(StarRocksParser.ShowBrokerStatementContext context) {
        return new ShowBrokerStmt();
    }

    // ------------------------------------------- Expression ----------------------------------------------------------

    @Override
    public ParseNode visitExpressionOrDefault(StarRocksParser.ExpressionOrDefaultContext context) {
        if (context.DEFAULT() != null) {
            return new DefaultValueExpr();
        } else {
            return visit(context.expression());
        }
    }

    @Override
    public ParseNode visitExpressionsWithDefault(StarRocksParser.ExpressionsWithDefaultContext context) {
        ArrayList<Expr> row = Lists.newArrayList();
        for (int i = 0; i < context.expressionOrDefault().size(); ++i) {
            row.add((Expr) visit(context.expressionOrDefault(i)));
        }
        return new ValueList(row);
    }

    @Override
    public ParseNode visitLogicalNot(StarRocksParser.LogicalNotContext context) {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, (Expr) visit(context.expression()), null);
    }

    @Override
    public ParseNode visitLogicalBinary(StarRocksParser.LogicalBinaryContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);

        if (context.operator.getType() == StarRocksLexer.LOGICAL_OR) {
            return new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);
        } else {
            return new CompoundPredicate(getLogicalBinaryOperator(context.operator), left, right);
        }
    }

    private static CompoundPredicate.Operator getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.AND:
                return CompoundPredicate.Operator.AND;
            case StarRocksLexer.OR:
                return CompoundPredicate.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    @Override
    public ParseNode visitPredicate(StarRocksParser.PredicateContext context) {
        if (context.predicateOperations() != null) {
            return visit(context.predicateOperations());
        } else {
            return visit(context.valueExpression());
        }
    }

    @Override
    public ParseNode visitIsNull(StarRocksParser.IsNullContext context) {
        Expr child = (Expr) visit(context.booleanExpression());

        if (context.NOT() == null) {
            return new IsNullPredicate(child, false);
        } else {
            return new IsNullPredicate(child, true);
        }
    }

    @Override
    public ParseNode visitComparison(StarRocksParser.ComparisonContext context) {
        BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0))
                .getSymbol());
        return new BinaryPredicate(op, (Expr) visit(context.left), (Expr) visit(context.right));
    }

    private static BinaryPredicate.Operator getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case StarRocksParser.EQ:
                return BinaryPredicate.Operator.EQ;
            case StarRocksParser.NEQ:
                return BinaryPredicate.Operator.NE;
            case StarRocksParser.LT:
                return BinaryPredicate.Operator.LT;
            case StarRocksParser.LTE:
                return BinaryPredicate.Operator.LE;
            case StarRocksParser.GT:
                return BinaryPredicate.Operator.GT;
            case StarRocksParser.GTE:
                return BinaryPredicate.Operator.GE;
            case StarRocksParser.EQ_FOR_NULL:
                return BinaryPredicate.Operator.EQ_FOR_NULL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    @Override
    public ParseNode visitInList(StarRocksParser.InListContext context) {
        boolean isNotIn = context.NOT() != null;
        return new InPredicate(
                (Expr) visit(context.value),
                visit(context.expression(), Expr.class), isNotIn);
    }

    @Override
    public ParseNode visitBetween(StarRocksParser.BetweenContext context) {
        boolean isNotBetween = context.NOT() != null;

        return new BetweenPredicate(
                (Expr) visit(context.value),
                (Expr) visit(context.lower),
                (Expr) visit(context.upper),
                isNotBetween);
    }

    @Override
    public ParseNode visitLike(StarRocksParser.LikeContext context) {
        LikePredicate likePredicate;
        if (context.REGEXP() != null || context.RLIKE() != null) {
            likePredicate = new LikePredicate(LikePredicate.Operator.REGEXP,
                    (Expr) visit(context.value),
                    (Expr) visit(context.pattern));
        } else {
            likePredicate = new LikePredicate(
                    LikePredicate.Operator.LIKE,
                    (Expr) visit(context.value),
                    (Expr) visit(context.pattern));
        }
        if (context.NOT() != null) {
            return new CompoundPredicate(CompoundPredicate.Operator.NOT, likePredicate, null);
        } else {
            return likePredicate;
        }
    }

    @Override
    public ParseNode visitSimpleCase(StarRocksParser.SimpleCaseContext context) {
        return new CaseExpr(
                (Expr) visit(context.caseExpr),
                visit(context.whenClause(), CaseWhenClause.class),
                (Expr) visitIfPresent(context.elseExpression));
    }

    @Override
    public ParseNode visitSearchedCase(StarRocksParser.SearchedCaseContext context) {
        return new CaseExpr(
                null,
                visit(context.whenClause(), CaseWhenClause.class),
                (Expr) visitIfPresent(context.elseExpression));
    }

    @Override
    public ParseNode visitWhenClause(StarRocksParser.WhenClauseContext context) {
        return new CaseWhenClause((Expr) visit(context.condition), (Expr) visit(context.result));
    }

    @Override
    public ParseNode visitArithmeticUnary(StarRocksParser.ArithmeticUnaryContext context) {
        Expr child = (Expr) visit(context.primaryExpression());
        switch (context.operator.getType()) {
            case StarRocksLexer.MINUS_SYMBOL:
                if (child.isLiteral() && child.getType().isNumericType()) {
                    try {
                        ((LiteralExpr) child).swapSign();
                    } catch (NotImplementedException e) {
                        throw new ParsingException(e.getMessage());
                    }
                    return child;
                } else {
                    return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, new IntLiteral(-1), child);
                }
            case StarRocksLexer.PLUS_SYMBOL:
                return child;
            case StarRocksLexer.BITNOT:
                return new ArithmeticExpr(ArithmeticExpr.Operator.BITNOT, child, null);
            case StarRocksLexer.LOGICAL_NOT:
                return new CompoundPredicate(CompoundPredicate.Operator.NOT, child, null);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public ParseNode visitArithmeticBinary(StarRocksParser.ArithmeticBinaryContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);
        if (left instanceof IntervalLiteral) {
            return new TimestampArithmeticExpr(getArithmeticBinaryOperator(context.operator), right,
                    ((IntervalLiteral) left).getValue(),
                    ((IntervalLiteral) left).getUnitIdentifier().getDescription(), true);
        }

        if (right instanceof IntervalLiteral) {
            return new TimestampArithmeticExpr(getArithmeticBinaryOperator(context.operator), left,
                    ((IntervalLiteral) right).getValue(),
                    ((IntervalLiteral) right).getUnitIdentifier().getDescription(), false);
        }

        return new ArithmeticExpr(getArithmeticBinaryOperator(context.operator), left, right);
    }

    private static ArithmeticExpr.Operator getArithmeticBinaryOperator(Token operator) {
        switch (operator.getType()) {
            case StarRocksLexer.PLUS_SYMBOL:
                return ArithmeticExpr.Operator.ADD;
            case StarRocksLexer.MINUS_SYMBOL:
                return ArithmeticExpr.Operator.SUBTRACT;
            case StarRocksLexer.ASTERISK_SYMBOL:
                return ArithmeticExpr.Operator.MULTIPLY;
            case StarRocksLexer.SLASH_SYMBOL:
                return ArithmeticExpr.Operator.DIVIDE;
            case StarRocksLexer.PERCENT_SYMBOL:
                return ArithmeticExpr.Operator.MOD;
            case StarRocksLexer.INT_DIV:
                return ArithmeticExpr.Operator.INT_DIVIDE;
            case StarRocksLexer.BITAND:
                return ArithmeticExpr.Operator.BITAND;
            case StarRocksLexer.BITOR:
                return ArithmeticExpr.Operator.BITOR;
            case StarRocksLexer.BITXOR:
                return ArithmeticExpr.Operator.BITXOR;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    @Override
    public ParseNode visitOdbcFunctionCallExpression(StarRocksParser.OdbcFunctionCallExpressionContext context) {
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) visit(context.functionCall());
        OdbcScalarFunctionCall odbcScalarFunctionCall = new OdbcScalarFunctionCall(functionCallExpr);
        return odbcScalarFunctionCall.mappingFunction();
    }

    private static final List<String> DATE_FUNCTIONS =
            Lists.newArrayList(FunctionSet.DATE_ADD,
                    FunctionSet.ADDDATE,
                    FunctionSet.DATE_ADD, FunctionSet.DATE_SUB,
                    FunctionSet.SUBDATE,
                    FunctionSet.DAYS_SUB,
                    FunctionSet.TIME_SLICE);

    @Override
    public ParseNode visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext context) {

        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();

        if (DATE_FUNCTIONS.contains(functionName)) {
            if (context.expression().size() != 2) {
                throw new ParsingException(
                        functionName + " must as format " + functionName + "(date,INTERVAL expr unit)");
            }

            Expr e1 = (Expr) visit(context.expression(0));
            Expr e2 = (Expr) visit(context.expression(1));
            if (!(e2 instanceof IntervalLiteral)) {
                e2 = new IntervalLiteral(e2, new UnitIdentifier("DAY"));
            }
            IntervalLiteral intervalLiteral = (IntervalLiteral) e2;

            return new TimestampArithmeticExpr(functionName, e1, intervalLiteral.getValue(),
                    intervalLiteral.getUnitIdentifier().getDescription());
        }

        if (functionName.equals(FunctionSet.ISNULL)) {
            List<Expr> params = visit(context.expression(), Expr.class);
            if (params.size() != 1) {
                throw new SemanticException("No matching function with signature: %s(%s).", functionName,
                        Joiner.on(", ").join(params.stream().map(p -> p.getType().toSql()).collect(toList())));
            }
            return new IsNullPredicate(params.get(0), false);
        }

        FunctionName fnName = FunctionName.createFnName(functionName);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName,
                new FunctionParams(false, visit(context.expression(), Expr.class)));

        if (context.over() != null) {
            return buildOverClause(functionCallExpr, context.over());
        }
        return functionCallExpr;
    }

    @Override
    public ParseNode visitAggregationFunctionCall(StarRocksParser.AggregationFunctionCallContext context) {

        String functionName;
        if (context.aggregationFunction().COUNT() != null) {
            functionName = FunctionSet.COUNT;
        } else if (context.aggregationFunction().AVG() != null) {
            functionName = FunctionSet.AVG;
        } else if (context.aggregationFunction().SUM() != null) {
            functionName = FunctionSet.SUM;
        } else if (context.aggregationFunction().MIN() != null) {
            functionName = FunctionSet.MIN;
        } else if (context.aggregationFunction().MAX() != null) {
            functionName = FunctionSet.MAX;
        } else {
            throw new StarRocksPlannerException("Aggregate functions are not being parsed correctly",
                    ErrorType.INTERNAL_ERROR);
        }
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(functionName,
                context.aggregationFunction().ASTERISK_SYMBOL() == null ?
                        new FunctionParams(context.aggregationFunction().DISTINCT() != null,
                                visit(context.aggregationFunction().expression(), Expr.class)) :
                        FunctionParams.createStarParam());

        if (context.over() != null) {
            return buildOverClause(functionCallExpr, context.over());
        }
        return functionCallExpr;
    }

    @Override
    public ParseNode visitWindowFunctionCall(StarRocksParser.WindowFunctionCallContext context) {
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) visit(context.windowFunction());
        return buildOverClause(functionCallExpr, context.over());
    }

    public static final ImmutableSet<String> WindowFunctionSet = ImmutableSet.of(
            FunctionSet.ROW_NUMBER, FunctionSet.RANK, FunctionSet.DENSE_RANK, FunctionSet.NTILE, FunctionSet.LEAD,
            FunctionSet.LAG, FunctionSet.FIRST_VALUE, FunctionSet.LAST_VALUE);

    @Override
    public ParseNode visitWindowFunction(StarRocksParser.WindowFunctionContext context) {
        if (WindowFunctionSet.contains(context.name.getText().toLowerCase())) {
            return new FunctionCallExpr(context.name.getText().toLowerCase(),
                    new FunctionParams(false, visit(context.expression(), Expr.class)));
        }
        throw new ParsingException("Unknown window function " + context.name.getText());
    }

    private AnalyticExpr buildOverClause(FunctionCallExpr functionCallExpr, StarRocksParser.OverContext context) {
        functionCallExpr.setIsAnalyticFnCall(true);
        List<OrderByElement> orderByElements = new ArrayList<>();
        if (context.ORDER() != null) {
            orderByElements = visit(context.sortItem(), OrderByElement.class);
        }
        List<Expr> partitionExprs = visit(context.partition, Expr.class);

        return new AnalyticExpr(functionCallExpr, partitionExprs, orderByElements,
                (AnalyticWindow) visitIfPresent(context.windowFrame()));
    }

    @Override
    public ParseNode visitExtract(StarRocksParser.ExtractContext context) {
        String fieldString = context.identifier().getText();
        return new FunctionCallExpr(fieldString,
                new FunctionParams(Lists.newArrayList((Expr) visit(context.valueExpression()))));
    }

    @Override
    public ParseNode visitCast(StarRocksParser.CastContext context) {
        return new CastExpr(new TypeDef(getType(context.type())), (Expr) visit(context.expression()));
    }

    @Override
    public ParseNode visitConvert(StarRocksParser.ConvertContext context) {
        return new CastExpr(new TypeDef(getType(context.type())), (Expr) visit(context.expression()));
    }

    @Override
    public ParseNode visitInformationFunctionExpression(StarRocksParser.InformationFunctionExpressionContext context) {
        if (context.name.getText().equalsIgnoreCase("database")
                || context.name.getText().equalsIgnoreCase("schema")
                || context.name.getText().equalsIgnoreCase("user")
                || context.name.getText().equalsIgnoreCase("current_user")
                || context.name.getText().equalsIgnoreCase("connection_id")) {
            return new InformationFunction(context.name.getText().toUpperCase());
        }
        throw new ParsingException("Unknown special function " + context.name.getText());
    }

    @Override
    public ParseNode visitSpecialFunctionExpression(StarRocksParser.SpecialFunctionExpressionContext context) {
        if (context.CHAR() != null) {
            return new FunctionCallExpr("char", visit(context.expression(), Expr.class));
        } else if (context.CURRENT_TIMESTAMP() != null) {
            return new FunctionCallExpr("current_timestamp", Lists.newArrayList());
        } else if (context.DAY() != null) {
            return new FunctionCallExpr("day", visit(context.expression(), Expr.class));
        } else if (context.HOUR() != null) {
            return new FunctionCallExpr("hour", visit(context.expression(), Expr.class));
        } else if (context.IF() != null) {
            return new FunctionCallExpr("if", visit(context.expression(), Expr.class));
        } else if (context.LEFT() != null) {
            return new FunctionCallExpr("left", visit(context.expression(), Expr.class));
        } else if (context.LIKE() != null) {
            return new FunctionCallExpr("like", visit(context.expression(), Expr.class));
        } else if (context.MINUTE() != null) {
            return new FunctionCallExpr("minute", visit(context.expression(), Expr.class));
        } else if (context.MOD() != null) {
            return new FunctionCallExpr("mod", visit(context.expression(), Expr.class));
        } else if (context.MONTH() != null) {
            return new FunctionCallExpr("month", visit(context.expression(), Expr.class));
        } else if (context.QUARTER() != null) {
            return new FunctionCallExpr("quarter", visit(context.expression(), Expr.class));
        } else if (context.REGEXP() != null) {
            return new FunctionCallExpr("regexp", visit(context.expression(), Expr.class));
        } else if (context.REPLACE() != null) {
            return new FunctionCallExpr("replace", visit(context.expression(), Expr.class));
        } else if (context.RIGHT() != null) {
            return new FunctionCallExpr("right", visit(context.expression(), Expr.class));
        } else if (context.RLIKE() != null) {
            return new FunctionCallExpr("regexp", visit(context.expression(), Expr.class));
        } else if (context.SECOND() != null) {
            return new FunctionCallExpr("second", visit(context.expression(), Expr.class));
        } else if (context.YEAR() != null) {
            return new FunctionCallExpr("year", visit(context.expression(), Expr.class));
        } else if (context.PASSWORD() != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.string());
            return new StringLiteral(new String(MysqlPassword.makeScrambledPassword(stringLiteral.getValue())));
        }

        if (context.TIMESTAMPADD() != null || context.TIMESTAMPDIFF() != null) {
            String functionName = context.TIMESTAMPADD() != null ? "TIMESTAMPADD" : "TIMESTAMPDIFF";
            UnitIdentifier e1 = (UnitIdentifier) visit(context.unitIdentifier());
            Expr e2 = (Expr) visit(context.expression(0));
            Expr e3 = (Expr) visit(context.expression(1));

            return new TimestampArithmeticExpr(functionName, e3, e2, e1.getDescription());
        }

        throw new ParsingException("No matching function with signature: %s(%s).", context.getText(),
                visit(context.expression(), Expr.class));
    }

    @Override
    public ParseNode visitConcat(StarRocksParser.ConcatContext context) {
        Expr left = (Expr) visit(context.left);
        Expr right = (Expr) visit(context.right);
        return new FunctionCallExpr("concat", new FunctionParams(Lists.newArrayList(left, right)));
    }

    @Override
    public ParseNode visitNullLiteral(StarRocksParser.NullLiteralContext context) {
        return new NullLiteral();
    }

    @Override
    public ParseNode visitBooleanLiteral(StarRocksParser.BooleanLiteralContext context) {
        try {
            return new BoolLiteral(context.getText());
        } catch (AnalysisException e) {
            throw new ParsingException("Invalid boolean literal: " + context.getText());
        }
    }

    @Override
    public ParseNode visitNumericLiteral(StarRocksParser.NumericLiteralContext context) {
        return visit(context.number());
    }

    private static final BigInteger LONG_MAX = new BigInteger("9223372036854775807"); // 2^63 - 1

    private static final BigInteger LARGEINT_MAX_ABS =
            new BigInteger("170141183460469231731687303715884105728"); // 2^127

    @Override
    public ParseNode visitIntegerValue(StarRocksParser.IntegerValueContext context) {
        try {
            BigInteger intLiteral = new BigInteger(context.getText());
            // Note: val is positive, because we do not recognize minus character in 'IntegerLiteral'
            // -2^63 will be recognized as large int(__int128)
            if (intLiteral.compareTo(LONG_MAX) <= 0) {
                return new IntLiteral(intLiteral.longValue());
            } else if (intLiteral.compareTo(LARGEINT_MAX_ABS) <= 0) {
                return new LargeIntLiteral(intLiteral.toString());
            } else {
                throw new ParsingException("Numeric overflow " + intLiteral);
            }
        } catch (NumberFormatException | AnalysisException e) {
            throw new ParsingException("Invalid numeric literal: " + context.getText());
        }
    }

    @Override
    public ParseNode visitDoubleValue(StarRocksParser.DoubleValueContext context) {
        try {
            if (SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_DOUBLE_LITERAL)) {
                return new FloatLiteral(context.getText());
            } else {
                BigDecimal decimal = new BigDecimal(context.getText());
                int precision = DecimalLiteral.getRealPrecision(decimal);
                int scale = DecimalLiteral.getRealScale(decimal);
                int integerPartWidth = precision - scale;
                if (integerPartWidth > 38) {
                    return new FloatLiteral(context.getText());
                }
                return new DecimalLiteral(decimal);
            }

        } catch (AnalysisException | NumberFormatException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    public ParseNode visitDecimalValue(StarRocksParser.DecimalValueContext context) {
        try {
            if (SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_DOUBLE_LITERAL)) {
                return new FloatLiteral(context.getText());
            } else {
                return new DecimalLiteral(context.getText());
            }
        } catch (AnalysisException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    public ParseNode visitDateLiteral(StarRocksParser.DateLiteralContext context) {
        String value = ((StringLiteral) visit(context.string())).getValue();
        try {
            if (context.DATE() != null) {
                return new DateLiteral(value, Type.DATE);
            }
            if (context.DATETIME() != null) {
                return new DateLiteral(value, Type.DATETIME);
            }
        } catch (AnalysisException e) {
            throw new ParsingException(e.getMessage());
        }
        throw new ParsingException("Parse Error : unknown type " + context.getText());
    }

    @Override
    public ParseNode visitString(StarRocksParser.StringContext context) {
        String quotedString;
        if (context.SINGLE_QUOTED_TEXT() != null) {
            quotedString = context.SINGLE_QUOTED_TEXT().getText();
            // For support mysql embedded quotation
            // In a single-quoted string, two single-quotes are combined into one single-quote
            quotedString = quotedString.substring(1, quotedString.length() - 1).replace("''", "'");
        } else {
            quotedString = context.DOUBLE_QUOTED_TEXT().getText();
            // For support mysql embedded quotation
            // In a double-quoted string, two double-quotes are combined into one double-quote
            quotedString = quotedString.substring(1, quotedString.length() - 1).replace("\"\"", "\"");
        }
        return new StringLiteral(escapeBackSlash(quotedString));
    }

    private static String escapeBackSlash(String str) {
        StringWriter writer = new StringWriter();
        int strLen = str.length();
        for (int i = 0; i < strLen; ++i) {
            char c = str.charAt(i);
            if (c == '\\' && (i + 1) < strLen) {
                switch (str.charAt(i + 1)) {
                    case 'n':
                        writer.append('\n');
                        break;
                    case 't':
                        writer.append('\t');
                        break;
                    case 'r':
                        writer.append('\r');
                        break;
                    case 'b':
                        writer.append('\b');
                        break;
                    case '0':
                        writer.append('\0'); // Ascii null
                        break;
                    case 'Z': // ^Z must be escaped on Win32
                        writer.append('\032');
                        break;
                    case '_':
                    case '%':
                        writer.append('\\'); // remember prefix for wildcard
                        /* Fall through */
                    default:
                        writer.append(str.charAt(i + 1));
                        break;
                }
                i++;
            } else {
                writer.append(c);
            }
        }

        return writer.toString();
    }

    @Override
    public ParseNode visitArrayConstructor(StarRocksParser.ArrayConstructorContext context) {
        if (context.arrayType() != null) {
            return new ArrayExpr(
                    new ArrayType(getType(context.arrayType().type())),
                    visit(context.expression(), Expr.class));
        }

        return new ArrayExpr(null, visit(context.expression(), Expr.class));
    }

    @Override
    public ParseNode visitArraySubscript(StarRocksParser.ArraySubscriptContext context) {
        Expr value = (Expr) visit(context.value);
        Expr index = (Expr) visit(context.index);
        return new ArrayElementExpr(value, index);
    }

    @Override
    public ParseNode visitArraySlice(StarRocksParser.ArraySliceContext context) {
        throw new ParsingException("Array slice is not currently supported");
        //TODO: support array slice in BE
        /*
        Expr expr = (Expr) visit(context.primaryExpression());

        IntLiteral lowerBound;
        if (context.start != null) {
            lowerBound = new IntLiteral(Long.parseLong(context.start.getText()));
        } else {
            lowerBound = new IntLiteral(0);
        }
        IntLiteral upperBound;
        if (context.end != null) {
            upperBound = new IntLiteral(Long.parseLong(context.end.getText()));
        } else {
            upperBound = new IntLiteral(-1);
        }

        return new ArraySliceExpr(expr, lowerBound, upperBound);
         */
    }

    @Override
    public ParseNode visitInterval(StarRocksParser.IntervalContext context) {
        return new IntervalLiteral((Expr) visit(context.value), (UnitIdentifier) visit(context.from));
    }

    @Override
    public ParseNode visitUnitIdentifier(StarRocksParser.UnitIdentifierContext context) {
        return new UnitIdentifier(context.getText());
    }

    @Override
    public ParseNode visitColumnReference(StarRocksParser.ColumnReferenceContext context) {
        if (context.identifier() != null) {
            Identifier identifier = (Identifier) visit(context.identifier());
            return new SlotRef(null, identifier.getValue(), identifier.getValue());
        } else {
            QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
            List<String> parts = qualifiedName.getParts();
            TableName tableName;
            if (parts.size() == 4) {
                tableName = new TableName(qualifiedName.getParts().get(0),
                        qualifiedName.getParts().get(1), qualifiedName.getParts().get(2));
                return new SlotRef(tableName, parts.get(3), parts.get(3));
            } else if (parts.size() == 3) {
                tableName = new TableName(qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
                return new SlotRef(tableName, parts.get(2), parts.get(2));
            } else if (parts.size() == 2) {
                tableName = new TableName(null, qualifiedName.getParts().get(0));
                return new SlotRef(tableName, parts.get(1), parts.get(1));
            } else {
                throw new ParsingException("Unqualified column reference " + qualifiedName);
            }
        }
    }

    @Override
    public ParseNode visitArrowExpression(StarRocksParser.ArrowExpressionContext context) {
        Expr expr = (Expr) visit(context.primaryExpression());
        StringLiteral stringLiteral = (StringLiteral) visit(context.string());

        return new ArrowExpr(expr, stringLiteral);
    }

    @Override
    public ParseNode visitUserVariable(StarRocksParser.UserVariableContext context) {
        String variable = ((Identifier) visit(context.identifierOrString())).getValue();
        return new VariableExpr(variable, SetType.USER);
    }

    @Override
    public ParseNode visitSystemVariable(StarRocksParser.SystemVariableContext context) {
        SetType setType = getVariableType(context.varType());
        return new VariableExpr(((Identifier) visit(context.identifier())).getValue(), setType);
    }

    @Override
    public ParseNode visitCollate(StarRocksParser.CollateContext context) {
        return visit(context.primaryExpression());
    }

    @Override
    public ParseNode visitParenthesizedExpression(StarRocksParser.ParenthesizedExpressionContext context) {
        return visit(context.expression());
    }

    @Override
    public ParseNode visitUnquotedIdentifier(StarRocksParser.UnquotedIdentifierContext context) {
        return new Identifier(context.getText());
    }

    @Override
    public ParseNode visitBackQuotedIdentifier(StarRocksParser.BackQuotedIdentifierContext context) {
        return new Identifier(context.getText().replace("`", ""));
    }

    @Override
    public ParseNode visitDigitIdentifier(StarRocksParser.DigitIdentifierContext context) {
        return new Identifier(context.getText());
    }

    // ------------------------------------------- COMMON AST --------------------------------------------------------------

    private static StatementBase.ExplainLevel getExplainType(StarRocksParser.ExplainDescContext context) {
        StatementBase.ExplainLevel explainLevel = StatementBase.ExplainLevel.NORMAL;
        if (context.LOGICAL() != null) {
            explainLevel = StatementBase.ExplainLevel.LOGICAL;
        } else if (context.VERBOSE() != null) {
            explainLevel = StatementBase.ExplainLevel.VERBOSE;
        } else if (context.COSTS() != null) {
            explainLevel = StatementBase.ExplainLevel.COST;
        }
        return explainLevel;
    }

    public static SetType getVariableType(StarRocksParser.VarTypeContext context) {
        if (context == null) {
            return SetType.DEFAULT;
        }

        if (context.GLOBAL() != null) {
            return SetType.GLOBAL;
        } else if (context.LOCAL() != null || context.SESSION() != null) {
            return SetType.SESSION;
        } else {
            return SetType.DEFAULT;
        }
    }

    @Override
    public ParseNode visitAssignment(StarRocksParser.AssignmentContext context) {
        String column = ((Identifier) visit(context.identifier())).getValue();
        Expr expr = (Expr) visit(context.expressionOrDefault());
        return new ColumnAssignment(column, expr);
    }

    @Override
    public ParseNode visitPartitionDesc(StarRocksParser.PartitionDescContext context) {
        List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
        List<PartitionDesc> partitionDesc = visit(context.rangePartitionDesc(), PartitionDesc.class);
        return new RangePartitionDesc(
                identifierList.stream().map(Identifier::getValue).collect(toList()),
                partitionDesc);
    }

    @Override
    public ParseNode visitSingleRangePartition(StarRocksParser.SingleRangePartitionContext context) {
        PartitionKeyDesc partitionKeyDesc = (PartitionKeyDesc) visit(context.partitionKeyDesc());
        boolean ifNotExists = context.IF() != null;
        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new SingleRangePartitionDesc(ifNotExists, ((Identifier) visit(context.identifier())).getValue(),
                partitionKeyDesc, properties);
    }

    @Override
    public ParseNode visitMultiRangePartition(StarRocksParser.MultiRangePartitionContext context) {
        if (context.interval() != null) {
            IntervalLiteral intervalLiteral = (IntervalLiteral) visit(context.interval());
            Expr expr = intervalLiteral.getValue();
            long intervalVal;
            if (expr instanceof IntLiteral) {
                intervalVal = ((IntLiteral) expr).getLongValue();
            } else {
                throw new IllegalArgumentException("Unsupported interval expr: " + expr);
            }
            return new MultiRangePartitionDesc(
                    ((StringLiteral) visit(context.string(0))).getStringValue(),
                    ((StringLiteral) visit(context.string(1))).getStringValue(),
                    intervalVal,
                    intervalLiteral.getUnitIdentifier().getDescription());
        } else {
            return new MultiRangePartitionDesc(
                    ((StringLiteral) visit(context.string(0))).getStringValue(),
                    ((StringLiteral) visit(context.string(1))).getStringValue(),
                    Long.parseLong(context.INTEGER_VALUE().getText()));
        }
    }

    @Override
    public ParseNode visitSingleItemListPartitionDesc(StarRocksParser.SingleItemListPartitionDescContext context) {
        List<String> values =
                context.stringList().string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue())
                        .collect(toList());
        boolean ifNotExists = context.IF() != null;
        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new SingleItemListPartitionDesc(ifNotExists, ((Identifier) visit(context.identifier())).getValue(),
                values, properties);
    }

    @Override
    public ParseNode visitMultiItemListPartitionDesc(StarRocksParser.MultiItemListPartitionDescContext context) {
        boolean ifNotExists = context.IF() != null;
        List<List<String>> multiValues = new ArrayList<>();
        for (StarRocksParser.StringListContext stringListContext : context.stringList()) {
            List<String> values =
                    stringListContext.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue())
                            .collect(toList());
            multiValues.add(values);
        }
        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new MultiItemListPartitionDesc(ifNotExists, ((Identifier) visit(context.identifier())).getValue(),
                multiValues, properties);
    }

    @Override
    public ParseNode visitPartitionKeyDesc(StarRocksParser.PartitionKeyDescContext context) {
        PartitionKeyDesc partitionKeyDesc;
        if (context.LESS() != null) {
            if (context.MAXVALUE() != null) {
                return PartitionKeyDesc.createMaxKeyDesc();
            }
            List<PartitionValue> partitionValueList =
                    visit(context.partitionValueList().get(0).partitionValue(), PartitionValue.class);
            partitionKeyDesc = new PartitionKeyDesc(partitionValueList);
        } else {
            List<PartitionValue> lowerPartitionValueList =
                    visit(context.partitionValueList().get(0).partitionValue(), PartitionValue.class);
            List<PartitionValue> upperPartitionValueList =
                    visit(context.partitionValueList().get(1).partitionValue(), PartitionValue.class);
            partitionKeyDesc = new PartitionKeyDesc(lowerPartitionValueList, upperPartitionValueList);
        }
        return partitionKeyDesc;
    }

    @Override
    public ParseNode visitPartitionValue(StarRocksParser.PartitionValueContext context) {
        if (context.MAXVALUE() != null) {
            return PartitionValue.MAX_VALUE;
        } else {
            return new PartitionValue(((StringLiteral) visit(context.string())).getStringValue());
        }
    }

    @Override
    public ParseNode visitDistributionDesc(StarRocksParser.DistributionDescContext context) {
        //default buckets number
        int buckets = 10;

        if (context.INTEGER_VALUE() != null) {
            buckets = Integer.parseInt(context.INTEGER_VALUE().getText());
        }
        List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);

        return new HashDistributionDesc(buckets, identifierList.stream().map(Identifier::getValue).collect(toList()));
    }

    @Override
    public ParseNode visitRefreshSchemeDesc(StarRocksParser.RefreshSchemeDescContext context) {
        LocalDateTime startTime = LocalDateTime.now();
        IntervalLiteral intervalLiteral = null;
        if (context.ASYNC() != null) {
            if (context.START() != null && context.interval() == null) {
                throw new SemanticException("Please input interval clause");
            }
            boolean defineStartTime = false;
            if (context.START() != null) {
                StringLiteral stringLiteral = (StringLiteral) visit(context.string());
                DateTimeFormatter dateTimeFormatter = null;
                try {
                    dateTimeFormatter = DateUtils.probeFormat(stringLiteral.getStringValue());
                    LocalDateTime tempStartTime = DateUtils.
                            parseStringWithDefaultHSM(stringLiteral.getStringValue(), dateTimeFormatter);
                    if (tempStartTime.isBefore(LocalDateTime.now())) {
                        throw new IllegalArgumentException("Refresh start must be after current time");
                    }
                    startTime = tempStartTime;
                    defineStartTime = true;
                } catch (AnalysisException e) {
                    throw new IllegalArgumentException(
                            "Refresh start " +
                                    stringLiteral.getStringValue() + " is incorrect");
                }
            }

            if (context.interval() != null) {
                intervalLiteral = (IntervalLiteral) visit(context.interval());
                if (!(intervalLiteral.getValue() instanceof IntLiteral)) {
                    throw new IllegalArgumentException(
                            "Refresh every " + intervalLiteral.getValue() + " must be IntLiteral");
                }
            }
            return new AsyncRefreshSchemeDesc(defineStartTime, startTime, intervalLiteral);
        } else if (context.MANUAL() != null) {
            return new ManualRefreshSchemeDesc();
        }
        return null;
    }

    @Override
    public ParseNode visitProperty(StarRocksParser.PropertyContext context) {
        return new Property(
                ((StringLiteral) visit(context.key)).getStringValue(),
                ((StringLiteral) visit(context.value)).getStringValue());
    }

    @Override
    public ParseNode visitOutfile(StarRocksParser.OutfileContext context) {
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        String format = null;
        if (context.fileFormat() != null) {
            if (context.fileFormat().identifier() != null) {
                format = ((Identifier) visit(context.fileFormat().identifier())).getValue();
            } else if (context.fileFormat().string() != null) {
                format = ((StringLiteral) visit(context.fileFormat().string())).getStringValue();
            }
        }

        return new OutFileClause(
                ((StringLiteral) visit(context.file)).getStringValue(),
                format,
                properties);
    }

    @Override
    public ParseNode visitColumnNameWithComment(StarRocksParser.ColumnNameWithCommentContext context) {
        String comment = null;
        if (context.comment() != null) {
            comment = ((StringLiteral) visit(context.comment())).getStringValue();
        }

        return new ColWithComment(((Identifier) visit(context.identifier())).getValue(), comment);
    }

    @Override
    public ParseNode visitIdentifierOrString(StarRocksParser.IdentifierOrStringContext context) {
        String s = null;
        if (context.identifier() != null) {
            s = ((Identifier) visit(context.identifier())).getValue();
        } else if (context.string() != null) {
            s = ((StringLiteral) visit(context.string())).getStringValue();
        }
        return new Identifier(s);
    }

    @Override
    public ParseNode visitUserWithHostAndBlanket(StarRocksParser.UserWithHostAndBlanketContext context) {
        Identifier user = (Identifier) visit(context.identifierOrString(0));
        Identifier host = (Identifier) visit(context.identifierOrString(1));
        return new UserIdentifier(user.getValue(), host.getValue(), true);
    }

    @Override
    public ParseNode visitUserWithHost(StarRocksParser.UserWithHostContext context) {
        Identifier user = (Identifier) visit(context.identifierOrString(0));
        Identifier host = (Identifier) visit(context.identifierOrString(1));
        return new UserIdentifier(user.getValue(), host.getValue(), false);
    }

    @Override
    public ParseNode visitUserWithoutHost(StarRocksParser.UserWithoutHostContext context) {
        Identifier user = (Identifier) visit(context.identifierOrString());
        return new UserIdentifier(user.getValue(), "%", false);
    }

    // ------------------------------------------- Procedure Statement -------------------------------------------

    @Override
    public ParseNode visitShowProcedureStatement(StarRocksParser.ShowProcedureStatementContext context) {
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowProcedureStmt(stringLiteral.getValue());
        } else if (context.expression() != null) {
            return new ShowProcedureStmt((Expr) visit(context.expression()));
        } else {
            return new ShowProcedureStmt();
        }
    }

    // ------------------------------------------- Util Functions -------------------------------------------

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private <T> List<T> visitIfPresent(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        if (contexts != null && contexts.size() != 0) {
            return contexts.stream()
                    .map(this::visit)
                    .map(clazz::cast)
                    .collect(toList());
        } else {
            return null;
        }
    }

    private ParseNode visitIfPresent(ParserRuleContext context) {
        if (context != null) {
            return visit(context);
        } else {
            return null;
        }
    }

    private FunctionArgsDef getFunctionArgsDef(StarRocksParser.TypeListContext typeList) {
        List<TypeDef> typeDefList = new ArrayList<>();
        for (StarRocksParser.TypeContext typeContext : typeList.type()) {
            typeDefList.add(new TypeDef(getType(typeContext)));
        }
        boolean isVariadic = typeList.DOTDOTDOT() != null;
        return new FunctionArgsDef(typeDefList, isVariadic);
    }

    private String getIdentifierName(StarRocksParser.IdentifierContext context) {
        return ((Identifier) visit(context)).getValue();
    }

    private QualifiedName getQualifiedName(StarRocksParser.QualifiedNameContext context) {
        List<String> parts = visit(context.identifier(), Identifier.class).stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private TableName qualifiedNameToTableName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.table
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 3) {
            return new TableName(parts.get(0), parts.get(1), parts.get(2));
        } else if (parts.size() == 2) {
            return new TableName(null, qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
        } else if (parts.size() == 1) {
            return new TableName(null, null, qualifiedName.getParts().get(0));
        } else {
            throw new ParsingException("error table name ");
        }
    }

    public Type getType(StarRocksParser.TypeContext context) {
        if (context.baseType() != null) {
            return getBaseType(context.baseType());
        } else if (context.decimalType() != null) {
            return getDecimalType(context.decimalType());
        } else if (context.arrayType() != null) {
            return getArrayType(context.arrayType());
        }
        throw new IllegalArgumentException("Unsupported type specification: " + context.getText());
    }

    private Type getBaseType(StarRocksParser.BaseTypeContext context) {
        int length = -1;
        if (context.typeParameter() != null) {
            length = Integer.parseInt(context.typeParameter().INTEGER_VALUE().toString());
        }
        if (context.STRING() != null) {
            ScalarType type = ScalarType.createVarcharType(ScalarType.DEFAULT_STRING_LENGTH);
            type.setAssignedStrLenInColDefinition();
            return type;
        } else if (context.VARCHAR() != null) {
            ScalarType type = ScalarType.createVarcharType(length);
            if (length != -1) {
                type.setAssignedStrLenInColDefinition();
            }
            return type;
        } else if (context.CHAR() != null) {
            ScalarType type = ScalarType.createCharType(length);
            if (length != -1) {
                type.setAssignedStrLenInColDefinition();
            }
            return type;
        } else if (context.SIGNED() != null) {
            return Type.INT;
        } else if (context.HLL() != null) {
            ScalarType type = ScalarType.createHllType();
            type.setAssignedStrLenInColDefinition();
            return type;
        } else {
            return ScalarType.createType(context.getChild(0).getText());
        }
    }

    public ScalarType getDecimalType(StarRocksParser.DecimalTypeContext context) {
        Integer precision = null;
        Integer scale = null;
        if (context.precision != null) {
            precision = Integer.parseInt(context.precision.getText());
            if (context.scale != null) {
                scale = Integer.parseInt(context.scale.getText());
            }
        }
        if (context.DECIMAL() != null) {
            if (precision != null) {
                if (scale != null) {
                    return ScalarType.createUnifiedDecimalType(precision, scale);
                }
                try {
                    return ScalarType.createUnifiedDecimalType(precision);
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            }
            return ScalarType.createUnifiedDecimalType(10, 0);
        } else if (context.DECIMAL32() != null || context.DECIMAL64() != null || context.DECIMAL128() != null) {
            try {
                ScalarType.checkEnableDecimalV3();
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            final PrimitiveType primitiveType = PrimitiveType.valueOf(context.children.get(0).getText().toUpperCase());
            if (precision != null) {
                if (scale != null) {
                    return ScalarType.createDecimalV3Type(primitiveType, precision, scale);
                }
                return ScalarType.createDecimalV3Type(primitiveType, precision);
            }
            return ScalarType.createDecimalV3Type(primitiveType);
        } else if (context.DECIMALV2() != null) {
            if (precision != null) {
                if (scale != null) {
                    return ScalarType.createDecimalV2Type(precision, scale);
                }
                return ScalarType.createDecimalV2Type(precision);
            }
            return ScalarType.createDecimalV2Type();
        } else {
            throw new IllegalArgumentException("Unsupported type " + context.getText());
        }
    }

    public ArrayType getArrayType(StarRocksParser.ArrayTypeContext context) {
        return new ArrayType(getType(context.type()));
    }

    @Override
    public ParseNode visitDescTableStatement(StarRocksParser.DescTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new DescribeStmt(targetTableName, context.ALL() != null);
    }

    @Override
    public ParseNode visitShowProcStatement(StarRocksParser.ShowProcStatementContext context) {
        StringLiteral stringLiteral = (StringLiteral) visit(context.path);
        return new ShowProcStmt(stringLiteral.getValue());
    }

    @Override
    public ParseNode visitRecoverPartitionStatement(StarRocksParser.RecoverPartitionStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        String partitionName = ((Identifier) visit(context.identifier())).getValue();
        return new RecoverPartitionStmt(tableName, partitionName);
    }

    @Override
    public ParseNode visitShowCharsetStatement(StarRocksParser.ShowCharsetStatementContext context) {
        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowCharsetStmt(pattern, where);
    }

    private Map<String, String> getJobProperties(StarRocksParser.JobPropertiesContext jobPropertiesContext) {
        Map<String, String> jobProperties = new HashMap<>();
        if (jobPropertiesContext != null) {
            List<Property> propertyList = visit(jobPropertiesContext.properties().property(), Property.class);
            for (Property property : propertyList) {
                jobProperties.put(property.getKey(), property.getValue());
            }
        }
        return jobProperties;
    }
}
