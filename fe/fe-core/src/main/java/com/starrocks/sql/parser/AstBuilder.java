// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.parser;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrowExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MultiInPredicate;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OdbcScalarFunctionCall;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TypeDef;
import com.starrocks.analysis.UserDesc;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.analysis.VarBinaryLiteral;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadErrorUrlClause;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.AnalyzeBasicDesc;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropFollowerClause;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropObserverClause;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.EmptyStmt;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.FunctionArgsDef;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.GrantRevokePrivilegeObjects;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.IncrementalRefreshSchemeDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.ManualRefreshSchemeDesc;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.Property;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.ResourceDesc;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.RowDelimiter;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetTransaction;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.SetUserPropertyVar;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowAuthorStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.ShowClustersStmt;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowEventsStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowLoadWarningsStmt;
import com.starrocks.sql.ast.ShowMaterializedViewStmt;
import com.starrocks.sql.ast.ShowOpenTableStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowPluginsStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowProcedureStmt;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.sql.ast.ShowRepositoriesStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowStatusStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowTriggersStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarehousesStmt;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.sql.ast.ShowWhiteListStmt;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.SuspendWarehouseStmt;
import com.starrocks.sql.ast.SwapTableClause;
import com.starrocks.sql.ast.SyncRefreshSchemeDesc;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UnitBoundary;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.UnsupportedStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UseWarehouseStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentifier;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.ValueList;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;

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
        if (context.statement() != null) {
            return visit(context.statement());
        } else {
            return visit(context.emptyStatement());
        }
    }

    @Override
    public ParseNode visitEmptyStatement(StarRocksParser.EmptyStatementContext context) {
        return new EmptyStmt();
    }

    // ---------------------------------------- Database Statement -----------------------------------------------------

    @Override
    public ParseNode visitUseDatabaseStatement(StarRocksParser.UseDatabaseStatementContext context) {
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

    @Override
    public ParseNode visitUseCatalogStatement(StarRocksParser.UseCatalogStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String catalogName = identifier.getValue();
        return new UseCatalogStmt(catalogName);
    }

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
    public ParseNode visitAlterDbQuotaStatement(StarRocksParser.AlterDbQuotaStatementContext context) {
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
        return new CreateDbStmt(context.IF() != null, dbName);
    }

    @Override
    public ParseNode visitDropDbStatement(StarRocksParser.DropDbStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new DropDbStmt(context.IF() != null, dbName, context.FORCE() != null);
    }

    @Override
    public ParseNode visitShowCreateDbStatement(StarRocksParser.ShowCreateDbStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier())).getValue();
        return new ShowCreateDbStmt(dbName);
    }

    @Override
    public ParseNode visitAlterDatabaseRenameStatement(StarRocksParser.AlterDatabaseRenameStatementContext context) {
        String dbName = ((Identifier) visit(context.identifier(0))).getValue();
        String newName = ((Identifier) visit(context.identifier(1))).getValue();
        return new AlterDatabaseRenameStatement(dbName, newName);
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
                context.engineDesc() == null ? null :
                        ((Identifier) visit(context.engineDesc().identifier())).getValue(),
                context.charsetDesc() == null ? null :
                        ((Identifier) visit(context.charsetDesc().identifierOrString())).getValue(),
                context.keyDesc() == null ? null : getKeysDesc(context.keyDesc()),
                context.partitionDesc() == null ? null : getPartitionDesc(context.partitionDesc()),
                context.distributionDesc() == null ? null : (DistributionDesc) visit(context.distributionDesc()),
                properties,
                extProperties,
                context.comment() == null ? null : ((StringLiteral) visit(context.comment().string())).getStringValue(),
                context.rollupDesc() == null ?
                        null : context.rollupDesc().rollupItem().stream().map(this::getRollup).collect(toList()),
                context.orderByDesc() == null ? null :
                        visit(context.orderByDesc().identifierList().identifier(), Identifier.class)
                                .stream().map(Identifier::getValue).collect(toList()));
    }

    private PartitionDesc getPartitionDesc(StarRocksParser.PartitionDescContext context) {
        List<PartitionDesc> partitionDescList = new ArrayList<>();
        if (context.functionCall() != null) {
            for (StarRocksParser.RangePartitionDescContext rangePartitionDescContext : context.rangePartitionDesc()) {
                final PartitionDesc rangePartitionDesc = (PartitionDesc) visit(rangePartitionDescContext);
                partitionDescList.add(rangePartitionDesc);
            }
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) visit(context.functionCall());
            String functionName = functionCallExpr.getFnName().getFunction();
            List<String> columnList = Lists.newArrayList();
            List<Expr> paramsExpr = functionCallExpr.getParams().exprs();
            if (FunctionSet.DATE_TRUNC.equals(functionName)) {
                if (paramsExpr.size() != 2) {
                    throw new ParsingException("The date_trunc function should have 2 parameters");
                }
                Expr firstExpr = paramsExpr.get(0);
                if (firstExpr instanceof StringLiteral) {
                    StringLiteral stringLiteral = (StringLiteral) firstExpr;
                    String fmt = stringLiteral.getValue();
                    if (!AnalyzerUtils.SUPPORTED_PARTITION_FORMAT.contains(fmt.toLowerCase())) {
                        throw new ParsingException("Unsupported date_trunc format %s", fmt);
                    }
                } else {
                    throw new ParsingException("Unsupported date_trunc params: %s", firstExpr.toSql());
                }
                Expr secondExpr = paramsExpr.get(1);
                if (secondExpr instanceof SlotRef) {
                    columnList.add(((SlotRef) secondExpr).getColumnName());
                } else {
                    throw new ParsingException("The second parameter of date_trunc only supports fields");
                }
            } else if (FunctionSet.TIME_SLICE.equals(functionName)) {
                Expr firstExpr = paramsExpr.get(0);
                if (firstExpr instanceof SlotRef) {
                    columnList.add(((SlotRef) firstExpr).getColumnName());
                } else {
                    throw new ParsingException("The first parameter of time_slice only supports fields");
                }
                Expr secondExpr = paramsExpr.get(1);
                Expr thirdExpr = paramsExpr.get(2);
                if (secondExpr instanceof IntLiteral && thirdExpr instanceof StringLiteral) {
                    StringLiteral stringLiteral = (StringLiteral) thirdExpr;
                    String fmt = stringLiteral.getValue();
                    if (!AnalyzerUtils.SUPPORTED_PARTITION_FORMAT.contains(fmt.toLowerCase())) {
                        throw new ParsingException("Unsupported time_slice format %s", fmt);
                    }
                } else {
                    throw new ParsingException("Unsupported time_slice params: %s %s",
                            secondExpr.toSql(), thirdExpr.toSql());
                }
            } else {
                throw new ParsingException("Unsupported partition expression: %s", functionCallExpr.toSql());
            }
            RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(columnList, partitionDescList);
            return new ExpressionPartitionDesc(rangePartitionDesc, functionCallExpr);
        }

        List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
        List<String> columnList = identifierList.stream().map(Identifier::getValue).collect(toList());
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

    private AlterClause getRollup(StarRocksParser.RollupItemContext rollupItemContext) {
        String rollupName = ((Identifier) visit(rollupItemContext.identifier())).getValue();
        List<Identifier> columnList =
                visit(rollupItemContext.identifierList().identifier(), Identifier.class);
        List<String> dupKeys = null;
        if (rollupItemContext.dupKeys() != null) {
            final List<Identifier> identifierList =
                    visit(rollupItemContext.dupKeys().identifierList().identifier(), Identifier.class);
            dupKeys = identifierList.stream().map(Identifier::getValue).collect(toList());
        }
        String baseRollupName = rollupItemContext.fromRollup() != null ?
                ((Identifier) visit(rollupItemContext.fromRollup().identifier())).getValue() : null;
        Map<String, String> properties = null;
        if (rollupItemContext.properties() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(rollupItemContext.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new AddRollupClause(rollupName, columnList.stream().map(Identifier::getValue).collect(toList()),
                dupKeys, baseRollupName,
                properties);
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
            } else if (defaultDescContext.qualifiedName() != null) {
                String functionName = defaultDescContext.qualifiedName().getText().toLowerCase();
                defaultValueDef = new ColumnDef.DefaultValueDef(true,
                        new FunctionCallExpr(functionName, new ArrayList<>()));
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
                context.keyDesc() == null ? null : getKeysDesc(context.keyDesc()),
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
    public ParseNode visitCreateTableLikeStatement(StarRocksParser.CreateTableLikeStatementContext context) {
        return new CreateTableLikeStmt(context.IF() != null,
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName(0))),
                qualifiedNameToTableName(getQualifiedName(context.qualifiedName(1))));
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
    public ParseNode visitDropTableStatement(StarRocksParser.DropTableStatementContext context) {
        boolean ifExists = context.IF() != null && context.EXISTS() != null;
        boolean force = context.FORCE() != null;
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new DropTableStmt(ifExists, targetTableName, force);
    }

    @Override
    public ParseNode visitRecoverTableStatement(StarRocksParser.RecoverTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        return new RecoverTableStmt(tableName);
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
    public ParseNode visitShowTableStatement(StarRocksParser.ShowTableStatementContext context) {
        boolean isVerbose = context.FULL() != null;
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

        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new ShowTableStmt(database, isVerbose, stringLiteral.getValue(), catalog);
        } else if (context.expression() != null) {
            return new ShowTableStmt(database, isVerbose, null, (Expr) visit(context.expression()), catalog);
        } else {
            return new ShowTableStmt(database, isVerbose, null, catalog);
        }
    }

    @Override
    public ParseNode visitDescTableStatement(StarRocksParser.DescTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        return new DescribeStmt(targetTableName, context.ALL() != null);
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
    public ParseNode visitAlterTableStatement(StarRocksParser.AlterTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        if (context.ROLLUP() != null) {
            if (context.ADD() != null) {
                List<AlterClause> clauses = context.rollupItem().stream().map(this::getRollup).collect(toList());
                return new AlterTableStmt(targetTableName, clauses);
            } else {
                List<Identifier> rollupList = visit(context.identifier(), Identifier.class);
                List<AlterClause> clauses = new ArrayList<>();
                for (Identifier rollupName : rollupList) {
                    clauses.add(new DropRollupClause(rollupName.getValue(), null));
                }
                return new AlterTableStmt(targetTableName, clauses);
            }
        } else {
            List<AlterClause> alterClauses = visit(context.alterClause(), AlterClause.class);
            return new AlterTableStmt(targetTableName, alterClauses);
        }
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
        return new CancelAlterTableStmt(alterType, dbTableName, alterJobIdList);
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

    // ------------------------------------------- Partition Statement ------------------------------------------------------

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
    public ParseNode visitRecoverPartitionStatement(StarRocksParser.RecoverPartitionStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        String partitionName = ((Identifier) visit(context.identifier())).getValue();
        return new RecoverPartitionStmt(tableName, partitionName);
    }

    // ------------------------------------------- Index Statement ------------------------------------------------------

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
    public ParseNode visitShowIndexStatement(StarRocksParser.ShowIndexStatementContext context) {
        QualifiedName tableName = getQualifiedName(context.table);
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        return new ShowIndexStmt(dbName == null ? null : dbName.toString(),
                qualifiedNameToTableName(tableName));
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

        RefreshSchemeDesc refreshSchemeDesc = null;
        Map<String, String> properties = new HashMap<>();
        ExpressionPartitionDesc expressionPartitionDesc = null;
        DistributionDesc distributionDesc = null;

        for (StarRocksParser.MaterializedViewDescContext desc : ListUtils.emptyIfNull(context.materializedViewDesc())) {
            // process properties
            if (desc.properties() != null) {
                if (MapUtils.isNotEmpty(properties)) {
                    throw new IllegalArgumentException("Duplicated PROPERTIES clause");
                }
                List<Property> propertyList = visit(desc.properties().property(), Property.class);
                for (Property property : propertyList) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            // process refresh
            if (desc.refreshSchemeDesc() != null) {
                if (refreshSchemeDesc != null) {
                    throw new IllegalArgumentException("Duplicated REFRESH clause");
                }
                refreshSchemeDesc = ((RefreshSchemeDesc) visit(desc.refreshSchemeDesc()));
            }

            // process partition by
            if (desc.primaryExpression() != null) {
                if (expressionPartitionDesc != null) {
                    throw new IllegalArgumentException("Duplicated PARTITION clause");
                }
                Expr expr = (Expr) visit(desc.primaryExpression());
                if (expr instanceof SlotRef) {
                    expressionPartitionDesc = new ExpressionPartitionDesc(expr);
                } else if (expr instanceof FunctionCallExpr) {
                    for (Expr child : expr.getChildren()) {
                        if (child instanceof SlotRef) {
                            expressionPartitionDesc = new ExpressionPartitionDesc(expr);
                            break;
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Partition exp not supports:" + expr.toSql());
                }

                if (expressionPartitionDesc == null) {
                    throw new IllegalArgumentException("Partition exp not supports:" + expr.toSql());
                }
            }

            // process distribution
            if (desc.distributionDesc() != null) {
                if (distributionDesc != null) {
                    throw new IllegalArgumentException("Duplicated DISTRIBUTED BY clause");
                }
                distributionDesc = (DistributionDesc) visit(desc.distributionDesc());
            }
        }

        if (refreshSchemeDesc == null) {
            if (distributionDesc == null) {
                // use old materialized index
                refreshSchemeDesc = new SyncRefreshSchemeDesc();
            } else {
                // use new manual refresh
                refreshSchemeDesc = new ManualRefreshSchemeDesc();
            }
        }
        if (refreshSchemeDesc instanceof SyncRefreshSchemeDesc) {
            if (expressionPartitionDesc != null) {
                throw new IllegalArgumentException(
                        "Partition by is not supported by SYNC refresh type int materialized view");
            }
            if (distributionDesc != null) {
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
        // process refresh
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
        ModifyTablePropertiesClause modifyTablePropertiesClause = null;
        if (context.modifyTablePropertiesClause() != null) {
            modifyTablePropertiesClause = (ModifyTablePropertiesClause) visit(context.modifyTablePropertiesClause());
        }
        return new AlterMaterializedViewStmt(mvName, newMvName, refreshSchemeDesc, modifyTablePropertiesClause);
    }

    @Override
    public ParseNode visitRefreshMaterializedViewStatement(
            StarRocksParser.RefreshMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        PartitionRangeDesc partitionRangeDesc = null;
        if (context.partitionRangeDesc() != null) {
            partitionRangeDesc =
                    (PartitionRangeDesc) visit(context.partitionRangeDesc());
        }
        return new RefreshMaterializedViewStatement(mvName, partitionRangeDesc, context.FORCE() != null);
    }

    @Override
    public ParseNode visitCancelRefreshMaterializedViewStatement(
            StarRocksParser.CancelRefreshMaterializedViewStatementContext context) {
        QualifiedName mvQualifiedName = getQualifiedName(context.qualifiedName());
        TableName mvName = qualifiedNameToTableName(mvQualifiedName);
        return new CancelRefreshMaterializedViewStmt(mvName);
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
    public ParseNode visitShowCreateExternalCatalogStatement(StarRocksParser.ShowCreateExternalCatalogStatementContext context) {
        Identifier identifier = (Identifier) visit(context.catalogName);
        String catalogName = identifier.getValue();
        return new ShowCreateExternalCatalogStmt(catalogName);
    }

    @Override
    public ParseNode visitShowCatalogsStatement(StarRocksParser.ShowCatalogsStatementContext context) {
        return new ShowCatalogsStmt();
    }

    // ---------------------------------------- Warehouse Statement -----------------------------------------------------

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
        return new CreateWarehouseStmt(context.IF() != null, whName, properties);
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

        return new ShowWarehousesStmt(pattern, where);
    }

    @Override
    public ParseNode visitShowClustersStatement(StarRocksParser.ShowClustersStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        return new ShowClustersStmt(whName);
    }

    @Override
    public ParseNode visitAlterWarehouseStatement(StarRocksParser.AlterWarehouseStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        if (context.CLUSTER() != null) {
            if (context.ADD() != null) {
                // add cluster clause
                return new AlterWarehouseStmt(whName,
                        AlterWarehouseStmt.OpType.ADD_CLUSTER, null);
            } else if (context.REMOVE() != null) {
                // remove cluster clause
                return new AlterWarehouseStmt(whName,
                        AlterWarehouseStmt.OpType.REMOVE_CLUSTER, null);
            }
        } else {
            // set property clause
            Map<String, String> properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
            return new AlterWarehouseStmt(whName,
                    AlterWarehouseStmt.OpType.SET_PROPERTY,
                    properties);
        }
        return null;
    }

    @Override
    public ParseNode visitSuspendWarehouseStatement(StarRocksParser.SuspendWarehouseStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        return new SuspendWarehouseStmt(whName);
    }

    public ParseNode visitResumeWarehouseStatement(StarRocksParser.ResumeWarehouseStatementContext context) {
        String whName = ((Identifier) visit(context.identifier())).getValue();
        return new ResumeWarehouseStmt(whName);
    }

    @Override
    public ParseNode visitDropWarehouseStatement(StarRocksParser.DropWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String whName = identifier.getValue();
        return new DropWarehouseStmt(context.IF() != null, whName);
    }

    @Override
    public ParseNode visitUseWarehouseStatement(StarRocksParser.UseWarehouseStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        String warehouseName = identifier.getValue();
        return new UseWarehouseStmt(warehouseName);
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
            List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getRow).collect(toList());

            List<String> colNames = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); ++i) {
                colNames.add("column_" + i);
            }

            queryStatement = new QueryStatement(new ValuesRelation(rows, colNames));
        } else {
            queryStatement = (QueryStatement) visit(context.queryStatement());
        }

        if (context.explainDesc() != null) {
            queryStatement.setIsExplain(true, getExplainType(context.explainDesc()));
        }

        return new InsertStmt(targetTableName, partitionNames,
                context.label == null ? null : ((Identifier) visit(context.label)).getValue(),
                getColumnNames(context.columnAliases()), queryStatement, context.OVERWRITE() != null);
    }

    @Override
    public ParseNode visitUpdateStatement(StarRocksParser.UpdateStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        List<ColumnAssignment> assignments = visit(context.assignmentList().assignment(), ColumnAssignment.class);
        List<Relation> fromRelations = null;
        if (context.fromClause() instanceof StarRocksParser.DualContext) {
            ArrayList<Expr> row = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            row.add(NullLiteral.create(Type.NULL));
            columnNames.add("");
            List<ArrayList<Expr>> rows = new ArrayList<>();
            rows.add(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, columnNames);
            valuesRelation.setNullValues(true);
            fromRelations = Lists.newArrayList(valuesRelation);
        } else {
            StarRocksParser.FromContext fromContext = (StarRocksParser.FromContext) context.fromClause();
            if (fromContext.relations() != null) {
                fromRelations = visit(fromContext.relations().relation(), Relation.class);
            }
        }
        Expr where = context.where != null ? (Expr) visit(context.where) : null;
        List<CTERelation> ctes = null;
        if (context.withClause() != null) {
            ctes = visit(context.withClause().commonTableExpression(), CTERelation.class);
        }
        UpdateStmt ret = new UpdateStmt(targetTableName, assignments, fromRelations, where, ctes);
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
        List<Relation> usingRelations = context.using != null ? visit(context.using.relation(), Relation.class) : null;
        Expr where = context.where != null ? (Expr) visit(context.where) : null;
        List<CTERelation> ctes = null;
        if (context.withClause() != null) {
            ctes = visit(context.withClause().commonTableExpression(), CTERelation.class);
        }
        DeleteStmt ret = new DeleteStmt(targetTableName, partitionNames, usingRelations, where, ctes);
        if (context.explainDesc() != null) {
            ret.setIsExplain(true, getExplainType(context.explainDesc()));
        }
        return ret;
    }

    // ------------------------------------------- Routine Statement ---------------------------------------------------

    @Override
    public ParseNode visitCreateRoutineLoadStatement(StarRocksParser.CreateRoutineLoadStatementContext context) {
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        QualifiedName tableName = null;
        if (context.table != null) {
            tableName = getQualifiedName(context.table);
        }

        String name = ((Identifier) visit(context.name)).getValue();
        List<StarRocksParser.LoadPropertiesContext> loadPropertiesContexts = context.loadProperties();
        List<ParseNode> loadPropertyList = getLoadPropertyList(loadPropertiesContexts);
        String typeName = context.source.getText();
        Map<String, String> jobProperties = getJobProperties(context.jobProperties());
        Map<String, String> dataSourceProperties = getDataSourceProperties(context.dataSourceProperties());

        return new CreateRoutineLoadStmt(new LabelName(dbName == null ? null : dbName.toString(), name),
                tableName == null ? null : tableName.toString(), loadPropertyList, jobProperties, typeName,
                dataSourceProperties);
    }

    @Override
    public ParseNode visitAlterRoutineLoadStatement(StarRocksParser.AlterRoutineLoadStatementContext context) {
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        String name = getIdentifierName(context.name);
        List<StarRocksParser.LoadPropertiesContext> loadPropertiesContexts = context.loadProperties();
        List<ParseNode> loadPropertyList = getLoadPropertyList(loadPropertiesContexts);
        Map<String, String> jobProperties = getJobProperties(context.jobProperties());

        if (context.dataSource() != null) {
            String typeName = context.dataSource().source.getText();
            Map<String, String> dataSourceProperties =
                    getDataSourceProperties(context.dataSource().dataSourceProperties());
            RoutineLoadDataSourceProperties dataSource =
                    new RoutineLoadDataSourceProperties(typeName, dataSourceProperties);
            return new AlterRoutineLoadStmt(new LabelName(dbName == null ? null : dbName.toString(), name),
                    loadPropertyList, jobProperties, dataSource);
        }

        return new AlterRoutineLoadStmt(new LabelName(dbName == null ? null : dbName.toString(), name),
                loadPropertyList, jobProperties, new RoutineLoadDataSourceProperties());
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

    @Override
    public ParseNode visitStopRoutineLoadStatement(StarRocksParser.StopRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = getQualifiedName(context.db).toString();
        }
        String name = null;
        if (context.name != null) {
            name = getIdentifierName(context.name);
        }
        return new StopRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitResumeRoutineLoadStatement(StarRocksParser.ResumeRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = getQualifiedName(context.db).toString();
        }
        String name = null;
        if (context.name != null) {
            name = getIdentifierName(context.name);
        }
        return new ResumeRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitPauseRoutineLoadStatement(StarRocksParser.PauseRoutineLoadStatementContext context) {
        String database = null;
        if (context.db != null) {
            database = getQualifiedName(context.db).toString();
        }
        String name = null;
        if (context.name != null) {
            name = getIdentifierName(context.name);
        }
        return new PauseRoutineLoadStmt(new LabelName(database, name));
    }

    @Override
    public ParseNode visitShowRoutineLoadStatement(StarRocksParser.ShowRoutineLoadStatementContext context) {
        boolean isVerbose = context.ALL() != null;
        String database = null;
        if (context.db != null) {
            database = getQualifiedName(context.db).toString();
        }
        String name = null;
        if (context.name != null) {
            name = getIdentifierName(context.name);
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

    @Override
    public ParseNode visitShowRoutineLoadTaskStatement(StarRocksParser.ShowRoutineLoadTaskStatementContext context) {
        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }
        return new ShowRoutineLoadTaskStmt(dbName == null ? null : dbName.toString(), where);
    }

    @Override
    public ParseNode visitShowStreamLoadStatement(StarRocksParser.ShowStreamLoadStatementContext context) {
        boolean isVerbose = context.ALL() != null;
        String database = null;
        if (context.db != null) {
            database = getQualifiedName(context.db).toString();
        }
        String name = null;
        if (context.name != null) {
            name = getIdentifierName(context.name);
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
        return new ShowStreamLoadStmt(new LabelName(database, name), isVerbose, where, orderByElements, limitElement);
    }

    // ------------------------------------------- Admin Statement -----------------------------------------------------

    @Override
    public ParseNode visitAdminSetConfigStatement(StarRocksParser.AdminSetConfigStatementContext context) {
        Map<String, String> configs = new HashMap<>();
        Property property = (Property) visitProperty(context.property());
        String configKey = property.getKey();
        String configValue = property.getValue();
        configs.put(configKey, configValue);
        return new AdminSetConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, configs);
    }

    @Override
    public ParseNode visitAdminSetReplicaStatusStatement(
            StarRocksParser.AdminSetReplicaStatusStatementContext context) {
        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.properties().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        return new AdminSetReplicaStatusStmt(properties);
    }

    @Override
    public ParseNode visitAdminShowConfigStatement(StarRocksParser.AdminShowConfigStatementContext context) {
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            return new AdminShowConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, stringLiteral.getValue());
        }
        return new AdminShowConfigStmt(AdminSetConfigStmt.ConfigType.FRONTEND, null);
    }

    @Override
    public ParseNode visitAdminShowReplicaDistributionStatement(
            StarRocksParser.AdminShowReplicaDistributionStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new AdminShowReplicaDistributionStmt(new TableRef(targetTableName, null, partitionNames));
    }

    @Override
    public ParseNode visitAdminShowReplicaStatusStatement(
            StarRocksParser.AdminShowReplicaStatusStatementContext context) {
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
    public ParseNode visitAdminRepairTableStatement(StarRocksParser.AdminRepairTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new AdminRepairTableStmt(new TableRef(targetTableName, null, partitionNames));
    }

    @Override
    public ParseNode visitAdminCancelRepairTableStatement(
            StarRocksParser.AdminCancelRepairTableStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableName targetTableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.partitionNames());
        }
        return new AdminCancelRepairTableStmt(new TableRef(targetTableName, null, partitionNames));
    }

    @Override
    public ParseNode visitAdminCheckTabletsStatement(StarRocksParser.AdminCheckTabletsStatementContext context) {
        // tablet_ids and properties
        List<Long> tabletIds = Lists.newArrayList();
        if (context.tabletList() != null) {
            tabletIds = context.tabletList().INTEGER_VALUE().stream().map(ParseTree::getText)
                    .map(Long::parseLong).collect(toList());
        }
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new AdminCheckTabletsStmt(tabletIds, properties);
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
    public ParseNode visitSyncStatement(StarRocksParser.SyncStatementContext context) {
        return new SyncStmt();
    }

    // ------------------------------------------- Cluster Management Statement ----------------------------------------

    @Override
    public ParseNode visitAlterSystemStatement(StarRocksParser.AlterSystemStatementContext context) {
        return new AlterSystemStmt((AlterClause) visit(context.alterClause()));
    }

    @Override
    public ParseNode visitCancelAlterSystemStatement(StarRocksParser.CancelAlterSystemStatementContext context) {
        return new CancelAlterSystemStmt(visit(context.string(), StringLiteral.class)
                .stream().map(StringLiteral::getValue).collect(toList()));
    }

    @Override
    public ParseNode visitShowComputeNodesStatement(StarRocksParser.ShowComputeNodesStatementContext context) {
        return new ShowComputeNodesStmt();
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

    // ------------------------------------------- Resource Group Statement --------------------------------------------

    public ParseNode visitCreateResourceGroupStatement(StarRocksParser.CreateResourceGroupStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        String name = identifier.getValue();

        List<List<Predicate>> predicatesList = new ArrayList<>();
        for (StarRocksParser.ClassifierContext classifierContext : context.classifier()) {
            List<Predicate> p = visit(classifierContext.expressionList().expression(), Predicate.class);
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
                List<Predicate> p = visit(classifierContext.expressionList().expression(), Predicate.class);
                predicatesList.add(p);
            }

            return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.AddClassifiers(predicatesList));
        } else if (context.DROP() != null) {
            if (context.ALL() != null) {
                return new AlterResourceGroupStmt(name, new AlterResourceGroupStmt.DropAllClassifiers());
            } else {
                return new AlterResourceGroupStmt(name,
                        new AlterResourceGroupStmt.DropClassifiers(context.INTEGER_VALUE()
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

    // ------------------------------------------- External Resource Statement -----------------------------------------

    public ParseNode visitCreateResourceStatement(StarRocksParser.CreateResourceStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new CreateResourceStmt(context.EXTERNAL() != null, identifier.getValue(), properties);
    }

    public ParseNode visitDropResourceStatement(StarRocksParser.DropResourceStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        return new DropResourceStmt(identifier.getValue());
    }

    public ParseNode visitAlterResourceStatement(StarRocksParser.AlterResourceStatementContext context) {
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        Map<String, String> properties = new HashMap<>();
        if (context.properties() != null) {
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        return new AlterResourceStmt(identifier.getValue(), properties);
    }

    public ParseNode visitShowResourceStatement(StarRocksParser.ShowResourceStatementContext context) {
        return new ShowResourcesStmt();
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
            colMappingList = visit(context.colMappingList.expressionList().expression(), Expr.class);
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

    // ------------------------------------------- Show Statement ------------------------------------------------------

    @Override
    public ParseNode visitShowAuthorStatement(StarRocksParser.ShowAuthorStatementContext context) {
        return new ShowAuthorStmt();
    }

    @Override
    public ParseNode visitShowBackendsStatement(StarRocksParser.ShowBackendsStatementContext context) {
        return new ShowBackendsStmt();
    }

    @Override
    public ParseNode visitShowBrokerStatement(StarRocksParser.ShowBrokerStatementContext context) {
        return new ShowBrokerStmt();
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

    @Override
    public ParseNode visitShowCollationStatement(StarRocksParser.ShowCollationStatementContext context) {
        String pattern = null;
        if (context.pattern != null) {
            StringLiteral stringLiteral = (StringLiteral) visit(context.pattern);
            pattern = stringLiteral.getValue();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowCollationStmt(pattern, where);
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
    public ParseNode visitShowDynamicPartitionStatement(StarRocksParser.ShowDynamicPartitionStatementContext context) {

        QualifiedName dbName = null;
        if (context.db != null) {
            dbName = getQualifiedName(context.db);
        }

        return new ShowDynamicPartitionStmt(dbName == null ? null : dbName.toString());
    }

    @Override
    public ParseNode visitShowEventsStatement(StarRocksParser.ShowEventsStatementContext context) {
        return new ShowEventsStmt();
    }

    @Override
    public ParseNode visitShowEnginesStatement(StarRocksParser.ShowEnginesStatementContext context) {
        return new ShowEnginesStmt();
    }

    @Override
    public ParseNode visitShowFrontendsStatement(StarRocksParser.ShowFrontendsStatementContext context) {
        return new ShowFrontendsStmt();
    }

    @Override
    public ParseNode visitShowPluginsStatement(StarRocksParser.ShowPluginsStatementContext context) {
        return new ShowPluginsStmt();
    }

    @Override
    public ParseNode visitShowRepositoriesStatement(StarRocksParser.ShowRepositoriesStatementContext context) {
        return new ShowRepositoriesStmt();
    }

    @Override
    public ParseNode visitShowOpenTableStatement(StarRocksParser.ShowOpenTableStatementContext context) {
        return new ShowOpenTableStmt();
    }

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

    @Override
    public ParseNode visitShowProcStatement(StarRocksParser.ShowProcStatementContext context) {
        StringLiteral stringLiteral = (StringLiteral) visit(context.path);
        return new ShowProcStmt(stringLiteral.getValue());
    }

    @Override
    public ParseNode visitShowProcesslistStatement(StarRocksParser.ShowProcesslistStatementContext context) {
        boolean isShowFull = context.FULL() != null;
        return new ShowProcesslistStmt(isShowFull);
    }

    @Override
    public ParseNode visitShowTransactionStatement(StarRocksParser.ShowTransactionStatementContext context) {

        String database = null;
        if (context.qualifiedName() != null) {
            database = getQualifiedName(context.qualifiedName()).toString();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }

        return new ShowTransactionStmt(database, where);
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
    public ParseNode visitShowTriggersStatement(StarRocksParser.ShowTriggersStatementContext context) {
        return new ShowTriggersStmt();
    }

    @Override
    public ParseNode visitShowUserStatement(StarRocksParser.ShowUserStatementContext context) {
        return new ShowUserStmt();
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
    public ParseNode visitShowWarningStatement(StarRocksParser.ShowWarningStatementContext context) {
        if (context.limitElement() != null) {
            return new ShowWarningStmt((LimitElement) visit(context.limitElement()));
        }
        return new ShowWarningStmt();
    }

    @Override
    public ParseNode visitHelpStatement(StarRocksParser.HelpStatementContext context) {
        String mask = ((Identifier) visit(context.identifierOrString())).getValue();
        return new HelpStmt(mask);
    }

    // ------------------------------------------- Backup Store Statement ----------------------------------------------

    @Override
    public ParseNode visitBackupStatement(StarRocksParser.BackupStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        LabelName labelName = qualifiedNameToLabelName(qualifiedName);
        List<TableRef> tblRefs = new ArrayList<>();
        for (StarRocksParser.TableDescContext tableDescContext : context.tableDesc()) {
            StarRocksParser.QualifiedNameContext qualifiedNameContext = tableDescContext.qualifiedName();
            qualifiedName = getQualifiedName(qualifiedNameContext);
            TableName tableName = qualifiedNameToTableName(qualifiedName);
            PartitionNames partitionNames = null;
            if (tableDescContext.partitionNames() != null) {
                partitionNames = (PartitionNames) visit(tableDescContext.partitionNames());
            }
            TableRef tableRef = new TableRef(tableName, null, partitionNames);
            tblRefs.add(tableRef);
        }

        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        String repoName = ((Identifier) visit(context.identifier())).getValue();
        return new BackupStmt(labelName, repoName, tblRefs, properties);
    }

    @Override
    public ParseNode visitCancelBackupStatement(StarRocksParser.CancelBackupStatementContext context) {
        return new CancelBackupStmt(((Identifier) visit(context.identifier())).getValue(), false);
    }

    @Override
    public ParseNode visitShowBackupStatement(StarRocksParser.ShowBackupStatementContext context) {
        if (context.identifier() == null) {
            return new ShowBackupStmt(null);
        }
        return new ShowBackupStmt(((Identifier) visit(context.identifier())).getValue());
    }

    @Override
    public ParseNode visitRestoreStatement(StarRocksParser.RestoreStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        LabelName labelName = qualifiedNameToLabelName(qualifiedName);

        List<TableRef> tblRefs = new ArrayList<>();
        for (StarRocksParser.RestoreTableDescContext tableDescContext : context.restoreTableDesc()) {
            StarRocksParser.QualifiedNameContext qualifiedNameContext = tableDescContext.qualifiedName();
            qualifiedName = getQualifiedName(qualifiedNameContext);
            TableName tableName = qualifiedNameToTableName(qualifiedName);
            PartitionNames partitionNames = null;
            if (tableDescContext.partitionNames() != null) {
                partitionNames = (PartitionNames) visit(tableDescContext.partitionNames());
            }

            String alias = null;
            if (tableDescContext.identifier() != null) {
                alias = ((Identifier) visit(tableDescContext.identifier())).getValue();
            }

            TableRef tableRef = new TableRef(tableName, alias, partitionNames);
            tblRefs.add(tableRef);
        }
        Map<String, String> properties = null;
        if (context.propertyList() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }

        String repoName = ((Identifier) visit(context.identifier())).getValue();
        return new RestoreStmt(labelName, repoName, tblRefs, properties);
    }

    @Override
    public ParseNode visitCancelRestoreStatement(StarRocksParser.CancelRestoreStatementContext context) {
        return new CancelBackupStmt(((Identifier) visit(context.identifier())).getValue(), true);
    }

    @Override
    public ParseNode visitShowRestoreStatement(StarRocksParser.ShowRestoreStatementContext context) {
        if (context.identifier() == null) {
            return new ShowRestoreStmt(null, null);
        }
        if (context.expression() != null) {
            return new ShowRestoreStmt(((Identifier) visit(context.identifier())).getValue(),
                    (Expr) visit(context.expression()));
        } else {
            return new ShowRestoreStmt(((Identifier) visit(context.identifier())).getValue(), null);
        }
    }

    @Override
    public ParseNode visitShowSnapshotStatement(StarRocksParser.ShowSnapshotStatementContext context) {
        StarRocksParser.ExpressionContext expression = context.expression();
        Expr where = null;
        if (expression != null) {
            where = (Expr) visit(context.expression());
        }

        String repoName = ((Identifier) visit(context.identifier())).getValue();

        return new ShowSnapshotStmt(repoName, where);
    }

    // ----------------------------------------------- Repository Statement --------------------------------------------

    @Override
    public ParseNode visitCreateRepositoryStatement(StarRocksParser.CreateRepositoryStatementContext context) {
        boolean isReadOnly = context.READ() != null && context.ONLY() != null;

        Map<String, String> properties = new HashMap<>();
        List<Property> propertyList = visit(context.propertyList().property(), Property.class);
        for (Property property : propertyList) {
            properties.put(property.getKey(), property.getValue());
        }
        String location = ((StringLiteral) visit(context.string())).getValue();
        String repoName = ((Identifier) visit(context.identifier(0))).getValue();
        String brokerName = null;
        if (context.identifier().size() == 2) {
            brokerName = ((Identifier) visit(context.identifier(1))).getValue();
        }

        return new CreateRepositoryStmt(isReadOnly, repoName, brokerName,
                location, properties);
    }

    @Override
    public ParseNode visitDropRepositoryStatement(StarRocksParser.DropRepositoryStatementContext context) {
        return new DropRepositoryStmt(((Identifier) visit(context.identifier())).getValue());
    }

    // -------------------------------- Sql BlackList And WhiteList Statement ------------------------------------------

    @Override
    public ParseNode visitAddSqlBlackListStatement(StarRocksParser.AddSqlBlackListStatementContext context) {
        String sql = ((StringLiteral) visit(context.string())).getStringValue();
        if (sql == null || sql.isEmpty()) {
            throw new IllegalArgumentException("Sql to be add black list is empty");
        }
        return new AddSqlBlackListStmt(sql);
    }

    @Override
    public ParseNode visitDelSqlBlackListStatement(StarRocksParser.DelSqlBlackListStatementContext context) {
        List<Long> indexes = context.INTEGER_VALUE().stream().map(ParseTree::getText)
                .map(Long::parseLong).collect(toList());
        return new DelSqlBlackListStmt(indexes);
    }

    @Override
    public ParseNode visitShowSqlBlackListStatement(StarRocksParser.ShowSqlBlackListStatementContext context) {
        return new ShowSqlBlackListStmt();
    }

    @Override
    public ParseNode visitShowWhiteListStatement(StarRocksParser.ShowWhiteListStatementContext context) {
        return new ShowWhiteListStmt();
    }

    // ----------------------------------------------- Export Statement ------------------------------------------------
    @Override
    public ParseNode visitExportStatement(StarRocksParser.ExportStatementContext context) {
        StarRocksParser.QualifiedNameContext qualifiedNameContext = context.tableDesc().qualifiedName();
        QualifiedName qualifiedName = getQualifiedName(qualifiedNameContext);
        TableName tableName = qualifiedNameToTableName(qualifiedName);
        PartitionNames partitionNames = null;
        if (context.tableDesc().partitionNames() != null) {
            partitionNames = (PartitionNames) visit(context.tableDesc().partitionNames());
        }
        TableRef tableRef = new TableRef(tableName, null, partitionNames);

        StringLiteral stringLiteral = (StringLiteral) visit(context.string());
        // properties
        Map<String, String> properties = null;
        if (context.properties() != null) {
            properties = new HashMap<>();
            List<Property> propertyList = visit(context.properties().property(), Property.class);
            for (Property property : propertyList) {
                properties.put(property.getKey(), property.getValue());
            }
        }
        // brokers
        BrokerDesc brokerDesc = getBrokerDesc(context.brokerDesc());
        return new ExportStmt(tableRef, getColumnNames(context.columnAliases()),
                stringLiteral.getValue(), properties, brokerDesc);
    }

    @Override
    public ParseNode visitCancelExportStatement(StarRocksParser.CancelExportStatementContext context) {
        String db = null;

        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }

        Expr where = null;
        if (context.expression() != null) {
            where = (Expr) visit(context.expression());
        }
        return new CancelExportStmt(catalog, where);
    }

    @Override
    public ParseNode visitShowExportStatement(StarRocksParser.ShowExportStatementContext context) {
        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }

        LimitElement le = null;
        if (context.limitElement() != null) {
            le = (LimitElement) visit(context.limitElement());
        }
        List<OrderByElement> orderByElements = null;
        if (context.ORDER() != null) {
            orderByElements = new ArrayList<>();
            orderByElements.addAll(visit(context.sortItem(), OrderByElement.class));
        }
        Expr whereExpr = null;
        if (context.expression() != null) {
            whereExpr = (Expr) visit(context.expression());
        }
        return new ShowExportStmt(catalog, whereExpr, orderByElements, le);
    }

    // ------------------------------------------------- Plugin Statement --------------------------------------------------------

    @Override
    public ParseNode visitInstallPluginStatement(StarRocksParser.InstallPluginStatementContext context) {
        String pluginPath = ((Identifier) visit(context.identifierOrString())).getValue();
        Map<String, String> properties = getProperties(context.properties());
        return new InstallPluginStmt(pluginPath, properties);
    }

    @Override
    public ParseNode visitUninstallPluginStatement(StarRocksParser.UninstallPluginStatementContext context) {
        String pluginPath = ((Identifier) visit(context.identifierOrString())).getValue();
        return new UninstallPluginStmt(pluginPath);
    }

    // ------------------------------------------------- File Statement ----------------------------------------------------------

    @Override
    public ParseNode visitCreateFileStatement(StarRocksParser.CreateFileStatementContext context) {
        String fileName = ((StringLiteral) visit(context.string())).getStringValue();

        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }
        Map<String, String> properties = getProperties(context.properties());

        return new CreateFileStmt(fileName, catalog, properties);
    }

    @Override
    public ParseNode visitDropFileStatement(StarRocksParser.DropFileStatementContext context) {
        String fileName = ((StringLiteral) visit(context.string())).getStringValue();

        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }
        Map<String, String> properties = getProperties(context.properties());

        return new DropFileStmt(fileName, catalog, properties);
    }

    @Override
    public ParseNode visitShowSmallFilesStatement(StarRocksParser.ShowSmallFilesStatementContext context) {

        String catalog = null;
        if (context.catalog != null) {
            QualifiedName dbName = getQualifiedName(context.catalog);
            catalog = dbName.toString();
        }

        return new ShowSmallFilesStmt(catalog);
    }

    // ------------------------------------------------- Set Statement -----------------------------------------------------------

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
    public ParseNode visitSetTransaction(StarRocksParser.SetTransactionContext context) {
        return new SetTransaction();
    }

    @Override
    public ParseNode visitSetRole(StarRocksParser.SetRoleContext context) {
        List<String> roles = context.roleList().string().stream().map(
                x -> ((StringLiteral) visit(x)).getStringValue()).collect(toList());
        return new SetRoleStmt(roles, false);
    }

    @Override
    public ParseNode visitSetRoleAll(StarRocksParser.SetRoleAllContext context) {
        List<String> roles = null;
        if (context.EXCEPT() != null) {
            roles = context.roleList().string().stream().map(
                    x -> ((StringLiteral) visit(x)).getStringValue()).collect(toList());
        }
        return new SetRoleStmt(roles, true);
    }

    // ----------------------------------------------- Unsupported Statement -----------------------------------------------------

    @Override
    public ParseNode visitUnsupportedStatement(StarRocksParser.UnsupportedStatementContext context) {
        return new UnsupportedStmt();
    }

    // ----------------------------------------------  Alter Clause --------------------------------------------------------------

    // ---------Alter system clause---------
    @Override
    public ParseNode visitAddFrontendClause(StarRocksParser.AddFrontendClauseContext context) {
        String cluster = ((StringLiteral) visit(context.string())).getStringValue();
        if (context.FOLLOWER() != null) {
            return new AddFollowerClause(cluster);
        } else if (context.OBSERVER() != null) {
            return new AddObserverClause(cluster);
        } else {
            throw new StarRocksPlannerException("frontend clause error.", ErrorType.INTERNAL_ERROR);
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
            throw new StarRocksPlannerException("frontend clause error.", ErrorType.INTERNAL_ERROR);
        }
    }

    @Override
    public ParseNode visitModifyFrontendHostClause(StarRocksParser.ModifyFrontendHostClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new ModifyFrontendAddressClause(clusters.get(0), clusters.get(1));
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
    public ParseNode visitDecommissionBackendClause(StarRocksParser.DecommissionBackendClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new DecommissionBackendClause(clusters);
    }

    @Override
    public ParseNode visitModifyBackendHostClause(StarRocksParser.ModifyBackendHostClauseContext context) {
        List<String> clusters =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        return new ModifyBackendAddressClause(clusters.get(0), clusters.get(1));
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
    public ParseNode visitModifyBrokerClause(StarRocksParser.ModifyBrokerClauseContext context) {
        String brokerName = ((Identifier) visit(context.identifierOrString())).getValue();
        if (context.ALL() != null) {
            return ModifyBrokerClause.createDropAllBrokerClause(brokerName);
        }
        List<String> hostPorts =
                context.string().stream().map(c -> ((StringLiteral) visit(c)).getStringValue()).collect(toList());
        if (context.ADD() != null) {
            return ModifyBrokerClause.createAddBrokerClause(brokerName, hostPorts);
        }
        return ModifyBrokerClause.createDropBrokerClause(brokerName, hostPorts);
    }

    @Override
    public ParseNode visitAlterLoadErrorUrlClause(StarRocksParser.AlterLoadErrorUrlClauseContext context) {
        return new AlterLoadErrorUrlClause(getProperties(context.properties()));
    }

    // ---------Alter table clause---------

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
        List<String> cols =
                context.identifierList().identifier().stream().map(this::getIdentifierName).collect(toList());
        String rollupName = null;
        if (context.rollupName != null) {
            rollupName = getIdentifierName(context.rollupName);
        }
        return new ReorderColumnsClause(cols, rollupName, getProperties(context.properties()));
    }

    @Override
    public ParseNode visitRollupRenameClause(StarRocksParser.RollupRenameClauseContext context) {
        String rollupName = ((Identifier) visit(context.rollupName)).getValue();
        String newRollupName = ((Identifier) visit(context.newRollupName)).getValue();
        return new RollupRenameClause(rollupName, newRollupName);
    }

    // ---------Alter partition clause---------

    @Override
    public ParseNode visitAddPartitionClause(StarRocksParser.AddPartitionClauseContext context) {
        boolean temporary = context.TEMPORARY() != null;
        PartitionDesc partitionDesc = null;
        if (context.singleRangePartition() != null) {
            partitionDesc = (PartitionDesc) visitSingleRangePartition(context.singleRangePartition());
        } else if (context.multiRangePartition() != null) {
            partitionDesc = (PartitionDesc) visitMultiRangePartition(context.multiRangePartition());
        }  else if (context.singleItemListPartitionDesc() != null) {
            partitionDesc = (PartitionDesc) visitSingleItemListPartitionDesc(context.singleItemListPartitionDesc());
        } else if (context.multiItemListPartitionDesc() != null) {
            partitionDesc = (PartitionDesc) visitMultiItemListPartitionDesc(context.multiItemListPartitionDesc());
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
    public ParseNode visitReplacePartitionClause(StarRocksParser.ReplacePartitionClauseContext context) {
        PartitionNames partitionNames = (PartitionNames) visit(context.parName);
        PartitionNames newPartitionNames = (PartitionNames) visit(context.tempParName);

        return new ReplacePartitionClause(partitionNames, newPartitionNames,
                getProperties(context.properties()));
    }

    @Override
    public ParseNode visitPartitionRenameClause(StarRocksParser.PartitionRenameClauseContext context) {
        String partitionName = ((Identifier) visit(context.parName)).getValue();
        String newPartitionName = ((Identifier) visit(context.newParName)).getValue();

        return new PartitionRenameClause(partitionName, newPartitionName);
    }

    // ------------------------------------------- Query Statement -----------------------------------------------------

    @Override
    public ParseNode visitQueryStatement(StarRocksParser.QueryStatementContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.queryRelation());
        QueryStatement queryStatement = new QueryStatement(queryRelation);
        if (context.outfile() != null) {
            queryStatement.setOutFileClause((OutFileClause) visit(context.outfile()));
        }

        if (context.explainDesc() != null) {
            queryStatement.setIsExplain(true, getExplainType(context.explainDesc()));
        }

        if (context.optimizerTrace() != null) {
            queryStatement.setIsExplain(true, StatementBase.ExplainLevel.OPTIMIZER);
        }

        return queryStatement;
    }

    @Override
    public ParseNode visitQueryRelation(StarRocksParser.QueryRelationContext context) {
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
        QueryRelation queryRelation = (QueryRelation) visit(context.queryRelation());
        // Regenerate cteID when generating plan
        return new CTERelation(
                RelationId.of(queryRelation).hashCode(),
                ((Identifier) visit(context.name)).getValue(),
                getColumnNames(context.columnAliases()),
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

        QueryRelation queryRelation = (QueryRelation) visit(context.queryPrimary());
        queryRelation.setOrderBy(orderByElements);
        queryRelation.setLimit(limitElement);
        return queryRelation;
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

        SelectRelation resultSelectRelation = new SelectRelation(
                selectList,
                from,
                (Expr) visitIfPresent(context.where),
                (GroupByClause) visitIfPresent(context.groupingElement()),
                (Expr) visitIfPresent(context.having));

        // extend Query with QUALIFY to nested queries with filter.
        if (context.qualifyFunction != null) {
            resultSelectRelation.setOrderBy(new ArrayList<OrderByElement>());

            // used to indicate nested query, represent thie 'from' part of outer query.
            SubqueryRelation subqueryRelation = new SubqueryRelation(new QueryStatement(resultSelectRelation));

            // use virtual table name to indicate subquery.
            TableName qualifyTableName = new TableName(null, "__QUALIFY__TABLE");
            subqueryRelation.setAlias(qualifyTableName);

            // use virtual item name to indicate column of window function.
            SelectListItem windowFunction = selectItems.get(selectItems.size() - 1);
            windowFunction.setAlias("__QUALIFY__VALUE");

            long selectValue = Long.parseLong(context.limit.getText());

            // need delete last item, because It shouldn't appear in result.
            List<SelectListItem> selectItemsVirtual = new ArrayList<>();
            selectItemsVirtual.addAll(selectItems);
            selectItemsVirtual.remove(selectItemsVirtual.size() - 1);

            List<SelectListItem> selectItemsOuter = new ArrayList<>();
            for (SelectListItem item : selectItemsVirtual) {
                if (item.getExpr() instanceof SlotRef) {
                    SlotRef exprRef = (SlotRef) item.getExpr();
                    String columnName = exprRef.getColumnName();
                    SlotRef resultSlotRef = new SlotRef(qualifyTableName, columnName);
                    selectItemsOuter.add(new SelectListItem(resultSlotRef, null));
                } else {
                    throw new ParsingException("Can't support result other than column.");
                }
            }

            // used to represent result, caused by we use nested query.
            SelectList selectListOuter = new SelectList(selectItemsOuter, isDistinct);

            // used to construct BinaryPredicate for QUALIFY.
            IntLiteral rightValue = new IntLiteral(selectValue);
            SlotRef leftSlotRef = new SlotRef(qualifyTableName, "__QUALIFY__VALUE");

            BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator()
                    .getChild(0)).getSymbol());
            return new SelectRelation(selectListOuter, subqueryRelation,
                    new BinaryPredicate(op, leftSlotRef, rightValue), null, null);
        } else {
            return resultSelectRelation;
        }
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
        return new GroupByClause(new ArrayList<>(visit(context.expressionList().expression(), Expr.class)),
                GroupByClause.GroupingType.GROUP_BY);
    }

    @Override
    public ParseNode visitRollup(StarRocksParser.RollupContext context) {
        List<Expr> groupingExprs = visit(context.expressionList().expression(), Expr.class);
        return new GroupByClause(new ArrayList<>(groupingExprs), GroupByClause.GroupingType.ROLLUP);
    }

    @Override
    public ParseNode visitCube(StarRocksParser.CubeContext context) {
        List<Expr> groupingExprs = visit(context.expressionList().expression(), Expr.class);
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
                // just ignore the hint if failed to add it which is the same as the previous behaviour
                tableRelation.addTableHint(identifier.getValue());
            }
        }

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableRelation.setAlias(new TableName(null, identifier.getValue()));
        }

        if (context.temporalClause() != null) {
            StringBuilder sb = new StringBuilder();
            for (ParseTree child : context.temporalClause().children) {
                sb.append(child.getText());
                sb.append(" ");
            }
            tableRelation.setTemporalClause(sb.toString());
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
        List<ArrayList<Expr>> rows = rowValues.stream().map(ValueList::getRow).collect(toList());

        List<String> colNames = getColumnNames(context.columnAliases());
        if (colNames == null) {
            colNames = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); ++i) {
                colNames.add("column_" + i);
            }
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
                new FunctionParams(false, visit(context.expressionList().expression(), Expr.class)));

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            tableFunctionRelation.setAlias(new TableName(null, identifier.getValue()));
        }
        tableFunctionRelation.setColumnOutputNames(getColumnNames(context.columnAliases()));

        return tableFunctionRelation;
    }

    @Override
    public ParseNode visitRowConstructor(StarRocksParser.RowConstructorContext context) {
        ArrayList<Expr> row = new ArrayList<>(visit(context.expressionList().expression(), Expr.class));
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
        return visit(context.queryRelation());
    }

    @Override
    public ParseNode visitQueryWithParentheses(StarRocksParser.QueryWithParenthesesContext context) {
        QueryRelation relation = (QueryRelation) visit(context.subquery());
        return new SubqueryRelation(new QueryStatement(relation));
    }

    @Override
    public ParseNode visitSubqueryWithAlias(StarRocksParser.SubqueryWithAliasContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.subquery());
        SubqueryRelation subqueryRelation = new SubqueryRelation(new QueryStatement(queryRelation));

        if (context.alias != null) {
            Identifier identifier = (Identifier) visit(context.alias);
            subqueryRelation.setAlias(new TableName(null, identifier.getValue()));
        } else {
            subqueryRelation.setAlias(new TableName(null, null));
        }

        subqueryRelation.setColumnOutputNames(getColumnNames(context.columnAliases()));

        return subqueryRelation;
    }

    @Override
    public ParseNode visitSubqueryExpression(StarRocksParser.SubqueryExpressionContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(context.subquery());
        return new Subquery(new QueryStatement(queryRelation));
    }

    @Override
    public ParseNode visitInSubquery(StarRocksParser.InSubqueryContext context) {
        boolean isNotIn = context.NOT() != null;
        QueryRelation query = (QueryRelation) visit(context.queryRelation());

        return new InPredicate((Expr) visit(context.value), new Subquery(new QueryStatement(query)), isNotIn);
    }

    @Override
    public ParseNode visitTupleInSubquery(StarRocksParser.TupleInSubqueryContext context) {
        boolean isNotIn = context.NOT() != null;
        QueryRelation query = (QueryRelation) visit(context.queryRelation());
        List<Expr> tupleExpressions = visit(context.expression(), Expr.class);

        return new MultiInPredicate(tupleExpressions, new Subquery(new QueryStatement(query)), isNotIn);
    }

    @Override
    public ParseNode visitExists(StarRocksParser.ExistsContext context) {
        QueryRelation query = (QueryRelation) visit(context.queryRelation());
        return new ExistsPredicate(new Subquery(new QueryStatement(query)), false);
    }

    @Override
    public ParseNode visitScalarSubquery(StarRocksParser.ScalarSubqueryContext context) {
        BinaryPredicate.Operator op = getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0))
                .getSymbol());
        Subquery subquery = new Subquery(new QueryStatement((QueryRelation) visit(context.queryRelation())));
        return new BinaryPredicate(op, (Expr) visit(context.booleanExpression()), subquery);
    }

    @Override
    public ParseNode visitShowFunctionsStatement(StarRocksParser.ShowFunctionsStatementContext context) {
        boolean isBuiltIn = context.BUILTIN() != null;
        boolean isGlobal = context.GLOBAL() != null;
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

        return new ShowFunctionsStmt(dbName, isBuiltIn, isGlobal, isVerbose, pattern, where);
    }

    @Override
    public ParseNode visitDropFunctionStatement(StarRocksParser.DropFunctionStatementContext context) {
        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();
        boolean isGlobal = context.GLOBAL() != null;
        FunctionName fnName = FunctionName.createFnName(functionName);
        if (isGlobal) {
            if (!Strings.isNullOrEmpty(fnName.getDb())) {
                throw new IllegalArgumentException(
                        "Global function name does not support qualified name: " + functionName);
            }
            fnName.setAsGlobalFunction();
        }

        return new DropFunctionStmt(fnName, getFunctionArgsDef(context.typeList()));
    }

    @Override
    public ParseNode visitCreateFunctionStatement(StarRocksParser.CreateFunctionStatementContext context) {
        String functionType = "SCALAR";
        boolean isGlobal = context.GLOBAL() != null;
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

        FunctionName fnName = FunctionName.createFnName(functionName);
        if (isGlobal) {
            if (!Strings.isNullOrEmpty(fnName.getDb())) {
                throw new IllegalArgumentException(
                        "Global function name does not support qualified name: " + functionName);
            }
            fnName.setAsGlobalFunction();
        }

        return new CreateFunctionStmt(functionType, fnName,
                getFunctionArgsDef(context.typeList()), returnTypeDef, intermediateType, properties);
    }

    // ------------------------------------------- Privilege Statement -------------------------------------------------

    @Override
    public ParseNode visitDropUserStatement(StarRocksParser.DropUserStatementContext context) {
        UserIdentifier user = (UserIdentifier) visit(context.user());
        return new DropUserStmt(user.getUserIdentity());
    }

    @Override
    public ParseNode visitCreateUserStatement(StarRocksParser.CreateUserStatementContext context) {
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
    public ParseNode visitAlterUserStatement(StarRocksParser.AlterUserStatementContext context) {
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
    public ParseNode visitCreateRoleStatement(StarRocksParser.CreateRoleStatementContext context) {
        Identifier role = (Identifier) visit(context.identifierOrString());
        return new CreateRoleStmt(role.getValue());
    }

    @Override
    public ParseNode visitShowRolesStatement(StarRocksParser.ShowRolesStatementContext context) {
        return new ShowRolesStmt();
    }

    @Override
    public ParseNode visitShowGrantsStatement(StarRocksParser.ShowGrantsStatementContext context) {
        boolean isAll = context.ALL() != null;
        UserIdentity userId =
                context.user() == null ? null : ((UserIdentifier) visit(context.user())).getUserIdentity();
        return new ShowGrantsStmt(userId, isAll);
    }

    @Override
    public ParseNode visitDropRoleStatement(StarRocksParser.DropRoleStatementContext context) {
        Identifier role = (Identifier) visit(context.identifierOrString());
        return new DropRoleStmt(role.getValue());
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
        String authString =
                context.string() == null ? null : ((StringLiteral) visit(context.string())).getStringValue();
        boolean isPasswordPlain = context.AS() == null;
        return new UserAuthOption(null, authPlugin.getValue().toUpperCase(), authString, isPasswordPlain);
    }

    @Override
    public ParseNode visitLambdaFunctionExpr(StarRocksParser.LambdaFunctionExprContext context) {
        List<String> names = Lists.newLinkedList();
        if (context.identifierList() != null) {
            final List<Identifier> identifierList = visit(context.identifierList().identifier(), Identifier.class);
            names = identifierList.stream().map(Identifier::getValue).collect(toList());
        } else {
            names.add(((Identifier) visit(context.identifier())).getValue());
        }
        List<Expr> arguments = Lists.newLinkedList();
        Expr expr = (Expr) visit(context.expression());
        arguments.add(expr); // put lambda body to the first argument
        for (int i = 0; i < names.size(); ++i) {
            arguments.add(new LambdaArgument(names.get(i)));
        }
        return new LambdaFunctionExpr(arguments);
    }

    @Override
    public ParseNode visitGrantRoleToUser(StarRocksParser.GrantRoleToUserContext context) {
        UserIdentifier user = (UserIdentifier) visit(context.user());
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        return new GrantRoleStmt(identifier.getValue(), user.getUserIdentity());
    }

    @Override
    public ParseNode visitGrantRoleToRole(StarRocksParser.GrantRoleToRoleContext context) {
        return new GrantRoleStmt(
                ((Identifier) visit(context.identifierOrString(0))).getValue(),
                ((Identifier) visit(context.identifierOrString(1))).getValue());
    }

    @Override
    public ParseNode visitRevokeRoleFromUser(StarRocksParser.RevokeRoleFromUserContext context) {
        UserIdentifier user = (UserIdentifier) visit(context.user());
        Identifier identifier = (Identifier) visit(context.identifierOrString());
        return new RevokeRoleStmt(identifier.getValue(), user.getUserIdentity());
    }

    @Override
    public ParseNode visitRevokeRoleFromRole(StarRocksParser.RevokeRoleFromRoleContext context) {
        return new RevokeRoleStmt(
                ((Identifier) visit(context.identifierOrString(0))).getValue(),
                ((Identifier) visit(context.identifierOrString(1))).getValue());
    }

    @Override
    public ParseNode visitGrantRevokeClause(StarRocksParser.GrantRevokeClauseContext context) {
        boolean withGrantOption = context.WITH() != null;
        if (context.user() != null) {
            UserIdentity user = ((UserIdentifier) visit(context.user())).getUserIdentity();
            return new GrantRevokeClause(user, null, withGrantOption);
        } else {
            String roleName = ((Identifier) visit(context.identifierOrString())).getValue();
            return new GrantRevokeClause(null, roleName, withGrantOption);
        }
    }

    @Override
    public ParseNode visitDeprecatedDbPrivilegeObject(StarRocksParser.DeprecatedDbPrivilegeObjectContext context) {
        GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
        String token = ((Identifier) visit(context.identifierOrStringOrStar())).getValue();
        ret.setPrivilegeObjectNameTokensList(Collections.singletonList(Collections.singletonList(token)));
        return ret;
    }

    @Override
    public ParseNode visitDeprecatedTablePrivilegeObject(
            StarRocksParser.DeprecatedTablePrivilegeObjectContext context) {
        GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
        List<String> tokenList = context.identifierOrStringOrStar().stream().map(
                c -> ((Identifier) visit(c)).getValue()).collect(toList());
        ret.setPrivilegeObjectNameTokensList(Collections.singletonList(tokenList));
        return ret;
    }

    @Override
    public ParseNode visitTablePrivilegeObjectNameList(StarRocksParser.TablePrivilegeObjectNameListContext context) {
        GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
        List<List<String>> l = new ArrayList<>();
        for (StarRocksParser.TablePrivilegeObjectNameContext oneContext : context.tablePrivilegeObjectName()) {
            l.add(oneContext.identifierOrString().stream().map(
                    c -> ((Identifier) visit(c)).getValue()).collect(toList()));
        }
        ret.setPrivilegeObjectNameTokensList(l);
        return ret;
    }

    @Override
    public ParseNode visitIdentifierOrStringList(StarRocksParser.IdentifierOrStringListContext context) {
        GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
        List<List<String>> l = new ArrayList<>();
        for (StarRocksParser.IdentifierOrStringContext oneContext : context.identifierOrString()) {
            String token = ((Identifier) visit(oneContext)).getValue();
            l.add(Collections.singletonList(token));
        }
        ret.setPrivilegeObjectNameTokensList(l);
        return ret;
    }

    @Override
    public ParseNode visitUserList(StarRocksParser.UserListContext context) {
        GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
        List<UserIdentity> l = new ArrayList<>();
        for (StarRocksParser.UserContext oneContext : context.user()) {
            l.add(((UserIdentifier) visit(oneContext)).getUserIdentity());
        }
        ret.setUserPrivilegeObjectList(l);
        return ret;
    }

    private GrantRevokePrivilegeObjects parsePrivilegeObjectNameList(
            String type, StarRocksParser.PrivilegeObjectNameListContext context) {
        // GRANT ADMIN ON SYSTEM TO userx, should return null as privilege object list
        if (context == null) {
            return null;
        }
        if (context.ASTERISK_SYMBOL() != null) {
            GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
            ret.setPrivilegeObjectNameTokensList(Collections.singletonList(Collections.singletonList("*")));
            return ret;
        } else if (context.userList() != null) {
            return (GrantRevokePrivilegeObjects) visit(context.userList());
        } else if (context.identifierOrStringList() != null) {
            if (type.equals("USER")) {
                // GRANT IMPERSONATE ON USER user1,user2
                // it's actually a list of user although it matches the `IdentifierOrString`
                GrantRevokePrivilegeObjects ret = new GrantRevokePrivilegeObjects();
                List<UserIdentity> l = new ArrayList<>();
                for (StarRocksParser.IdentifierOrStringContext oneContext :
                        context.identifierOrStringList().identifierOrString()) {
                    l.add(new UserIdentity(((Identifier) visit(oneContext)).getValue(), "%", false));
                }
                ret.setUserPrivilegeObjectList(l);
                return ret;
            }
            return (GrantRevokePrivilegeObjects) visit(context.identifierOrStringList());
        } else {
            return (GrantRevokePrivilegeObjects) visit(context.tablePrivilegeObjectNameList());
        }
    }

    @Override
    public ParseNode visitGrantImpersonateBrief(StarRocksParser.GrantImpersonateBriefContext context) {
        List<String> privList = Collections.singletonList("IMPERSONATE");
        GrantRevokeClause clause = (GrantRevokeClause) visit(context.grantRevokeClause());
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        List<UserIdentity> users =
                Collections.singletonList(((UserIdentifier) visit(context.user())).getUserIdentity());
        objects.setUserPrivilegeObjectList(users);
        return new GrantPrivilegeStmt(privList, "USER", clause, objects);
    }

    @Override
    public ParseNode visitRevokeImpersonateBrief(StarRocksParser.RevokeImpersonateBriefContext context) {
        List<String> privList = Collections.singletonList("IMPERSONATE");
        GrantRevokeClause clause = (GrantRevokeClause) visit(context.grantRevokeClause());
        List<UserIdentity> users =
                Collections.singletonList(((UserIdentifier) visit(context.user())).getUserIdentity());
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        objects.setUserPrivilegeObjectList(users);
        return new RevokePrivilegeStmt(privList, "USER", clause, objects);
    }

    @Override
    public ParseNode visitGrantTablePrivBrief(StarRocksParser.GrantTablePrivBriefContext context) {
        GrantRevokePrivilegeObjects objects =
                (GrantRevokePrivilegeObjects) visit(context.tableDbPrivilegeObjectNameList());
        int objectTokenSize = objects.getPrivilegeObjectNameTokensList().get(0).size();
        String type = objectTokenSize == 1 ? "DATABASE" : "TABLE";
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, true);
    }

    @Override
    public ParseNode visitRevokeTablePrivBrief(StarRocksParser.RevokeTablePrivBriefContext context) {
        GrantRevokePrivilegeObjects objects =
                (GrantRevokePrivilegeObjects) visit(context.tableDbPrivilegeObjectNameList());
        int objectTokenSize = objects.getPrivilegeObjectNameTokensList().get(0).size();
        String type = objectTokenSize == 1 ? "DATABASE" : "TABLE";
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, false);
    }

    @Override
    public ParseNode visitGrantPrivWithType(StarRocksParser.GrantPrivWithTypeContext context) {
        String type = ((Identifier) visit(context.privilegeType())).getValue().toUpperCase();
        GrantRevokePrivilegeObjects objects = parsePrivilegeObjectNameList(type, context.privilegeObjectNameList());
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, true);
    }

    @Override
    public ParseNode visitRevokePrivWithType(StarRocksParser.RevokePrivWithTypeContext context) {
        String type = ((Identifier) visit(context.privilegeType())).getValue().toUpperCase();
        GrantRevokePrivilegeObjects objects = parsePrivilegeObjectNameList(type, context.privilegeObjectNameList());
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, false);
    }

    public String extendPrivilegeType(boolean isGlobal, String type) {
        if (isGlobal) {
            if (type.equals("FUNCTIONS") || type.equals("FUNCTION")) {
                return "GLOBAL_" + type;
            }
        }
        return type;
    }

    @Override
    public ParseNode visitGrantOnAll(StarRocksParser.GrantOnAllContext context) {
        GrantRevokePrivilegeObjects objects =
                parseGrantRevokeOnAll(context.privilegeType(), context.identifierOrString());
        String type = ((Identifier) visit(context.privilegeType(0))).getValue().toUpperCase();
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, true);
    }

    public ParseNode visitAllGlobalFunctions(StarRocksParser.GrantRevokeClauseContext grantRevokeClauseContext,
                                             StarRocksParser.PrivilegeActionListContext privilegeActionListContext,
                                             boolean isGrant) {
        String type = PrivilegeType.GLOBAL_FUNCTION.getPlural();
        List<String> allTypes = ImmutableList.of(type);
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        objects.setAll(allTypes, null, null);
        return newGrantRevokePrivilegeStmt(
                type, privilegeActionListContext, grantRevokeClauseContext, objects, isGrant);
    }

    @Override
    public ParseNode visitGrantOnAllGlobalFunctions(StarRocksParser.GrantOnAllGlobalFunctionsContext context) {
        return visitAllGlobalFunctions(context.grantRevokeClause(), context.privilegeActionList(), true);
    }

    @Override
    public ParseNode visitRevokeOnAllGlobalFunctions(StarRocksParser.RevokeOnAllGlobalFunctionsContext context) {
        return visitAllGlobalFunctions(context.grantRevokeClause(), context.privilegeActionList(), false);
    }

    @Override
    public ParseNode visitRevokeOnAll(StarRocksParser.RevokeOnAllContext context) {
        GrantRevokePrivilegeObjects objects =
                parseGrantRevokeOnAll(context.privilegeType(), context.identifierOrString());
        String type = ((Identifier) visit(context.privilegeType(0))).getValue().toUpperCase();
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, false);
    }

    @Override
    public ParseNode visitGrantPrivWithFunc(StarRocksParser.GrantPrivWithFuncContext context) {
        String type = ((Identifier) visit(context.privilegeType())).getValue().toUpperCase();
        type = extendPrivilegeType(context.GLOBAL() != null, type);
        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();
        FunctionArgsDef argsDef = getFunctionArgsDef(context.typeList());
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        objects.setFunctionArgsDef(argsDef);
        objects.setFunctionName(functionName);
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, true);
    }

    @Override
    public ParseNode visitRevokePrivWithFunc(StarRocksParser.RevokePrivWithFuncContext context) {
        String type = ((Identifier) visit(context.privilegeType())).getValue().toUpperCase();
        type = extendPrivilegeType(context.GLOBAL() != null, type);
        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();
        FunctionArgsDef argsDef = getFunctionArgsDef(context.typeList());
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        objects.setFunctionArgsDef(argsDef);
        objects.setFunctionName(functionName);
        return newGrantRevokePrivilegeStmt(
                type, context.privilegeActionList(), context.grantRevokeClause(), objects, false);
    }

    private GrantRevokePrivilegeObjects parseGrantRevokeOnAll(
            List<StarRocksParser.PrivilegeTypeContext> privilegeTypeContexts,
            StarRocksParser.IdentifierOrStringContext identifierOrStringContext) {
        GrantRevokePrivilegeObjects objects = new GrantRevokePrivilegeObjects();
        List<String> l = privilegeTypeContexts.stream().map(
                k -> ((Identifier) visit(k)).getValue().toUpperCase()).collect(toList());
        if (identifierOrStringContext == null) {
            // all xxx in all xxx
            objects.setAll(l, null, null);
        } else {
            // all xx in xx bb, we should regard the last privilege type as the restrict type
            int lastIndex = l.size() - 1;
            String restrictType = l.get(lastIndex);
            l.remove(lastIndex);
            objects.setAll(l, restrictType, ((Identifier) visit(identifierOrStringContext)).getValue());
        }
        return objects;
    }

    private BaseGrantRevokePrivilegeStmt newGrantRevokePrivilegeStmt(
            String privilegeType,
            StarRocksParser.PrivilegeActionListContext privListContext,
            StarRocksParser.GrantRevokeClauseContext clauseContext,
            GrantRevokePrivilegeObjects objects,
            boolean isGrant) {
        List<String> privilegeList = privListContext.privilegeAction().stream().map(
                c -> ((Identifier) visit(c)).getValue().toUpperCase()).collect(toList());
        GrantRevokeClause clause = (GrantRevokeClause) visit(clauseContext);
        if (isGrant) {
            return new GrantPrivilegeStmt(privilegeList, privilegeType.toUpperCase(), clause, objects);
        } else {
            return new RevokePrivilegeStmt(privilegeList, privilegeType.toUpperCase(), clause, objects);
        }

    }

    @Override
    public ParseNode visitPrivilegeAction(StarRocksParser.PrivilegeActionContext context) {
        if (context.privilegeActionReserved() != null) {
            return new Identifier(context.privilegeActionReserved().getText());
        } else {
            return visit(context.identifier());
        }
    }

    @Override
    public ParseNode visitPrivilegeType(StarRocksParser.PrivilegeTypeContext context) {
        if (context.privilegeTypeReserved() != null) {
            return new Identifier(context.privilegeTypeReserved().getText());
        } else {
            return visit(context.identifier());
        }
    }

    @Override
    public ParseNode visitExecuteAsStatement(StarRocksParser.ExecuteAsStatementContext context) {
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
        return new CompoundPredicate(getLogicalBinaryOperator(context.operator), left, right);
    }

    private static CompoundPredicate.Operator getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case StarRocksLexer.AND:
            case StarRocksLexer.LOGICAL_AND:
                return CompoundPredicate.Operator.AND;
            case StarRocksLexer.OR:
            case StarRocksLexer.LOGICAL_OR:
                return CompoundPredicate.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    @Override
    public ParseNode visitPredicate(StarRocksParser.PredicateContext context) {
        if (context.predicateOperations() != null) {
            return visit(context.predicateOperations());
        } else if (context.tupleInSubquery() != null) {
            return visit(context.tupleInSubquery());
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
                visit(context.expressionList().expression(), Expr.class), isNotIn);
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
            case StarRocksLexer.BIT_SHIFT_LEFT:
                return ArithmeticExpr.Operator.BIT_SHIFT_LEFT;
            case StarRocksLexer.BIT_SHIFT_RIGHT:
                return ArithmeticExpr.Operator.BIT_SHIFT_RIGHT;
            case StarRocksLexer.BIT_SHIFT_RIGHT_LOGICAL:
                return ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL;
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
                    FunctionSet.DAYS_SUB);

    private static List<Expr> getArgumentsForTimeSlice(Expr time, Expr value, String ident, String boundary) {
        List<Expr> exprs = Lists.newLinkedList();
        exprs.add(time);
        exprs.add(value);
        exprs.add(new StringLiteral(ident));
        exprs.add(new StringLiteral(boundary));

        return exprs;
    }

    @Override
    public ParseNode visitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext context) {

        String functionName = getQualifiedName(context.qualifiedName()).toString().toLowerCase();

        FunctionName fnName = FunctionName.createFnName(functionName);
        if (functionName.equals(FunctionSet.TIME_SLICE) || functionName.equals(FunctionSet.DATE_SLICE)) {
            if (context.expression().size() == 2) {
                Expr e1 = (Expr) visit(context.expression(0));
                Expr e2 = (Expr) visit(context.expression(1));
                if (!(e2 instanceof IntervalLiteral)) {
                    e2 = new IntervalLiteral(e2, new UnitIdentifier("DAY"));
                }
                IntervalLiteral intervalLiteral = (IntervalLiteral) e2;
                FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName, getArgumentsForTimeSlice(e1,
                        intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription().toLowerCase(),
                        "floor"));

                return functionCallExpr;
            } else if (context.expression().size() == 3) {
                Expr e1 = (Expr) visit(context.expression(0));
                Expr e2 = (Expr) visit(context.expression(1));
                if (!(e2 instanceof IntervalLiteral)) {
                    e2 = new IntervalLiteral(e2, new UnitIdentifier("DAY"));
                }
                IntervalLiteral intervalLiteral = (IntervalLiteral) e2;

                ParseNode e3 = visit(context.expression(2));
                if (!(e3 instanceof UnitBoundary)) {
                    throw new ParsingException(functionName + " must use FLOOR/CEIL as third parameter");
                }
                UnitBoundary unitBoundary = (UnitBoundary) e3;
                FunctionCallExpr functionCallExpr = new FunctionCallExpr(fnName, getArgumentsForTimeSlice(e1,
                        intervalLiteral.getValue(), intervalLiteral.getUnitIdentifier().getDescription().toLowerCase(),
                        unitBoundary.getDescription().toLowerCase()));

                return functionCallExpr;
            } else if (context.expression().size() == 4) {
                Expr e1 = (Expr) visit(context.expression(0));
                Expr e2 = (Expr) visit(context.expression(1));
                Expr e3 = (Expr) visit(context.expression(2));
                Expr e4 = (Expr) visit(context.expression(3));
                return new FunctionCallExpr(fnName, ImmutableList.of(e1, e2, e3, e4));
            } else {
                throw new ParsingException(
                        functionName + " must as format " + functionName + "(date,INTERVAL expr unit[, FLOOR"
                                + " | CEIL])");
            }
        }

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

        if (ArithmeticExpr.isArithmeticExpr(fnName.getFunction())) {
            if (context.expression().size() < 1) {
                throw new ParsingException("Arithmetic expression least one parameter");
            }

            Expr e1 = (Expr) visit(context.expression(0));
            Expr e2 = context.expression().size() > 1 ? (Expr) visit(context.expression(1)) : null;
            return new ArithmeticExpr(ArithmeticExpr.getArithmeticOperator(fnName.getFunction()), e1, e2);
        }

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

    public static final ImmutableSet<String> WINDOW_FUNCTION_SET = ImmutableSet.of(
            FunctionSet.ROW_NUMBER, FunctionSet.RANK, FunctionSet.DENSE_RANK, FunctionSet.NTILE, FunctionSet.LEAD,
            FunctionSet.LAG, FunctionSet.FIRST_VALUE, FunctionSet.LAST_VALUE);

    @Override
    public ParseNode visitWindowFunction(StarRocksParser.WindowFunctionContext context) {
        if (WINDOW_FUNCTION_SET.contains(context.name.getText().toLowerCase())) {
            FunctionCallExpr functionCallExpr = new FunctionCallExpr(context.name.getText().toLowerCase(),
                    new FunctionParams(false, visit(context.expression(), Expr.class)));
            functionCallExpr.setIgnoreNulls(context.ignoreNulls() != null);
            return functionCallExpr;
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
    public ParseNode visitSpecialDateTimeExpression(StarRocksParser.SpecialDateTimeExpressionContext context) {
        if (context.name.getText().equalsIgnoreCase("current_timestamp")
                || context.name.getText().equalsIgnoreCase("current_time")
                || context.name.getText().equalsIgnoreCase("current_date")
                || context.name.getText().equalsIgnoreCase("localtime")
                || context.name.getText().equalsIgnoreCase("localtimestamp")) {
            return new FunctionCallExpr(context.name.getText().toUpperCase(), Lists.newArrayList());
        }
        throw new ParsingException("Unknown special function " + context.name.getText());
    }

    @Override
    public ParseNode visitSpecialFunctionExpression(StarRocksParser.SpecialFunctionExpressionContext context) {
        if (context.CHAR() != null) {
            return new FunctionCallExpr("char", visit(context.expression(), Expr.class));
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
        } else if (context.FLOOR() != null) {
            return new FunctionCallExpr("floor", visit(context.expression(), Expr.class));
        } else if (context.CEIL() != null) {
            return new FunctionCallExpr("ceil", visit(context.expression(), Expr.class));
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

    @Override
    public ParseNode visitBinary(StarRocksParser.BinaryContext context) {
        String quotedText;
        if (context.BINARY_SINGLE_QUOTED_TEXT() != null) {
            quotedText = context.BINARY_SINGLE_QUOTED_TEXT().getText();
        } else {
            quotedText = context.BINARY_DOUBLE_QUOTED_TEXT().getText();
        }
        return new VarBinaryLiteral(quotedText.substring(2, quotedText.length() - 1));
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
                    visit(context.expressionList().expression(), Expr.class));
        }

        List<Expr> exprs;
        if (context.expressionList() != null) {
            exprs = visit(context.expressionList().expression(), Expr.class);
        } else {
            exprs = Collections.emptyList();
        }
        return new ArrayExpr(null, exprs);
    }

    @Override
    public ParseNode visitCollectionSubscript(StarRocksParser.CollectionSubscriptContext context) {
        Expr value = (Expr) visit(context.value);
        Expr index = (Expr) visit(context.index);
        return new CollectionElementExpr(value, index);
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
    public ParseNode visitUnitBoundary(StarRocksParser.UnitBoundaryContext context) {
        return new UnitBoundary(context.getText());
    }

    @Override
    public ParseNode visitDereference(StarRocksParser.DereferenceContext ctx) {
        Expr base = (Expr) visit(ctx.base);

        String fieldName;
        if (ctx.DOT_IDENTIFIER() != null) {
            fieldName = ctx.DOT_IDENTIFIER().getText().substring(1);
        } else {
            fieldName = ((Identifier) visit(ctx.fieldName)).getValue();
        }

        // Trick method
        // If left is SlotRef type, we merge fieldName to SlotRef
        // The reason do this is to maintain compatibility with the original SlotRef
        if (base instanceof SlotRef) {
            // do merge
            SlotRef tmp = (SlotRef) base;
            List<String> parts = new ArrayList<>(tmp.getQualifiedName().getParts());
            parts.add(fieldName);
            return new SlotRef(QualifiedName.of(parts));
        } else if (base instanceof SubfieldExpr) {
            // Merge multi-level subfield access
            SubfieldExpr subfieldExpr = (SubfieldExpr) base;
            ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
            for (String tmpFieldName : subfieldExpr.getFieldNames()) {
                builder.add(tmpFieldName);
            }
            builder.add(fieldName);
            return new SubfieldExpr(subfieldExpr.getChild(0), builder.build());
        } else {
            // If left is not a SlotRef, we can assume left node must be an StructType,
            // and fieldName must be StructType's subfield name.
            return new SubfieldExpr(base, ImmutableList.of(fieldName));
        }
    }

    @Override
    public ParseNode visitColumnReference(StarRocksParser.ColumnReferenceContext context) {
        Identifier identifier = (Identifier) visit(context.identifier());
        List<String> parts = new ArrayList<>();
        parts.add(identifier.getValue());
        QualifiedName qualifiedName = QualifiedName.of(parts);
        return new SlotRef(qualifiedName);
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
        } else if (context.VERBOSE() != null) {
            return SetType.VERBOSE;
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
    public ParseNode visitPartitionRangeDesc(StarRocksParser.PartitionRangeDescContext context) {
        return new PartitionRangeDesc(((StringLiteral) visit(context.string(0))).getStringValue(),
                ((StringLiteral) visit(context.string(1))).getStringValue());
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
        int buckets = 0;

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
        } else if (context.INCREMENTAL() != null) {
            return new IncrementalRefreshSchemeDesc();
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
    public ParseNode visitIdentifierOrStringOrStar(StarRocksParser.IdentifierOrStringOrStarContext context) {
        String s = null;
        if (context.identifier() != null) {
            return visit(context.identifier());
        } else if (context.string() != null) {
            s = ((StringLiteral) visit(context.string())).getStringValue();
        } else if (context.ASTERISK_SYMBOL() != null) {
            s = "*";
        }
        return new Identifier(s);
    }

    @Override
    public ParseNode visitIdentifierOrString(StarRocksParser.IdentifierOrStringContext context) {
        String s = null;
        if (context.identifier() != null) {
            return visit(context.identifier());
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
        List<String> parts = new ArrayList<>();
        for (ParseTree c : context.children) {
            if (c instanceof TerminalNode) {
                TerminalNode t = (TerminalNode) c;
                if (t.getSymbol().getType() == StarRocksParser.DOT_IDENTIFIER) {
                    parts.add(t.getText().substring(1));
                }
            } else if (c instanceof StarRocksParser.IdentifierContext) {
                StarRocksParser.IdentifierContext identifierContext = (StarRocksParser.IdentifierContext) c;
                Identifier identifier = (Identifier) visit(identifierContext);
                parts.add(identifier.getValue());
            }
        }

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
        } else if (context.structType() != null) {
            return getStructType(context.structType());
        } else if (context.mapType() != null) {
            return getMapType(context.mapType());
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
        } else if (context.VARBINARY() != null) {
            ScalarType type = ScalarType.createVarbinary(length);
            if (length != -1) {
                type.setAssignedStrLenInColDefinition();
            }
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

    public StructType getStructType(StarRocksParser.StructTypeContext context) {
        ArrayList<StructField> fields = new ArrayList<>();
        List<StarRocksParser.SubfieldDescContext> subfields =
                context.subfieldDescs().subfieldDesc();
        for (StarRocksParser.SubfieldDescContext type : subfields) {
            fields.add(new StructField(type.identifier().getText(), getType(type.type()), null));
        }

        return new StructType(fields);
    }

    public MapType getMapType(StarRocksParser.MapTypeContext context) {
        Type keyType = getType(context.type(0));
        if (keyType.isComplexType()) {
            throw new IllegalArgumentException("Unsupported type specification: " + context.getText());
        }
        Type valueType = getType(context.type(1));
        return new MapType(keyType, valueType);
    }

    private LabelName qualifiedNameToLabelName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.table
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 2) {
            return new LabelName(parts.get(0), parts.get(1));
        } else if (parts.size() == 1) {
            return new LabelName(null, parts.get(0));
        } else {
            throw new ParsingException("error table name ");
        }
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

    private List<ParseNode> getLoadPropertyList(List<StarRocksParser.LoadPropertiesContext> loadPropertiesContexts) {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        Preconditions.checkNotNull(loadPropertiesContexts, "load properties is null");
        for (StarRocksParser.LoadPropertiesContext loadPropertiesContext : loadPropertiesContexts) {
            if (loadPropertiesContext.colSeparatorProperty() != null) {
                String literal =
                        ((StringLiteral) visit(loadPropertiesContext.colSeparatorProperty().string())).getValue();
                loadPropertyList.add(new ColumnSeparator(literal));
            }

            if (loadPropertiesContext.rowDelimiterProperty() != null) {
                String literal =
                        ((StringLiteral) visit(loadPropertiesContext.rowDelimiterProperty().string())).getValue();
                loadPropertyList.add(new RowDelimiter(literal));
            }

            if (loadPropertiesContext.importColumns() != null) {
                ImportColumnsStmt importColumnsStmt = (ImportColumnsStmt) visit(loadPropertiesContext.importColumns());
                loadPropertyList.add(importColumnsStmt);
            }

            if (loadPropertiesContext.expression() != null) {
                Expr where = (Expr) visit(loadPropertiesContext.expression());
                loadPropertyList.add(new ImportWhereStmt(where));
            }

            if (loadPropertiesContext.partitionNames() != null) {
                loadPropertyList.add(visit(loadPropertiesContext.partitionNames()));
            }
        }
        return loadPropertyList;
    }

    @Override
    public ParseNode visitImportColumns(StarRocksParser.ImportColumnsContext importColumnsContext) {
        List<ImportColumnDesc> columns = new ArrayList<>();
        for (StarRocksParser.QualifiedNameContext qualifiedNameContext :
                importColumnsContext.columnProperties().qualifiedName()) {
            String column = ((Identifier) (visit(qualifiedNameContext))).getValue();
            ImportColumnDesc columnDesc = new ImportColumnDesc(column);
            columns.add(columnDesc);
        }
        for (StarRocksParser.AssignmentContext assignmentContext :
                importColumnsContext.columnProperties().assignment()) {
            ColumnAssignment columnAssignment = (ColumnAssignment) (visit(assignmentContext));
            Expr expr = columnAssignment.getExpr();
            ImportColumnDesc columnDesc = new ImportColumnDesc(columnAssignment.getColumn(), expr);
            columns.add(columnDesc);
        }
        return new ImportColumnsStmt(columns);
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

    private Map<String, String> getDataSourceProperties(
            StarRocksParser.DataSourcePropertiesContext dataSourcePropertiesContext) {
        Map<String, String> dataSourceProperties = new HashMap<>();
        if (dataSourcePropertiesContext != null) {
            List<Property> propertyList = visit(dataSourcePropertiesContext.propertyList().property(), Property.class);
            for (Property property : propertyList) {
                dataSourceProperties.put(property.getKey(), property.getValue());
            }
        }
        return dataSourceProperties;
    }

    public List<String> getColumnNames(StarRocksParser.ColumnAliasesContext context) {
        if (context == null) {
            return null;
        }

        // StarRocks tables are not case-sensitive, so targetColumnNames are converted
        // to lowercase characters to facilitate subsequent matching.
        List<Identifier> targetColumnNamesIdentifiers = visitIfPresent(context.identifier(), Identifier.class);
        if (targetColumnNamesIdentifiers != null) {
            return targetColumnNamesIdentifiers.stream()
                    .map(Identifier::getValue).map(String::toLowerCase).collect(toList());
        } else {
            return null;
        }
    }
}

