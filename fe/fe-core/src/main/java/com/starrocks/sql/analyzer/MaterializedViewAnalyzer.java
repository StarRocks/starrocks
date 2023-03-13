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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.Util;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.logging.log4j.util.Strings;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static com.starrocks.server.CatalogMgr.isInternalCatalog;

public class MaterializedViewAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public enum RefreshTimeUnit {
            DAY,
            HOUR,
            MINUTE,
            SECOND
        }

        private boolean isSupportBasedOnTable(Table table) {
            return table instanceof OlapTable || table instanceof HiveTable || table instanceof HudiTable ||
                    table instanceof IcebergTable;
        }

        private boolean isExternalTableFromResource(Table table) {
            if (table instanceof OlapTable) {
                return false;
            } else if (table instanceof HiveTable || table instanceof HudiTable) {
                HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
                String catalogName = hiveMetaStoreTable.getCatalogName();
                return Strings.isBlank(catalogName) || isResourceMappingCatalog(catalogName);
            } else if (table instanceof IcebergTable) {
                IcebergTable icebergTable = (IcebergTable) table;
                String catalogName = icebergTable.getCatalogName();
                return Strings.isBlank(catalogName) || isResourceMappingCatalog(catalogName);
            } else {
                return true;
            }
        }

        static class SelectRelationCollector extends AstVisitor<Void, Void> {
            private final List<SelectRelation> selectRelations = new ArrayList<>();

            public static List<SelectRelation> collectBaseRelations(QueryRelation queryRelation) {
                SelectRelationCollector collector = new SelectRelationCollector();
                queryRelation.accept(collector, null);
                return collector.selectRelations;
            }

            @Override
            public Void visitRelation(Relation node, Void context) {
                return null;
            }

            @Override
            public Void visitSelect(SelectRelation node, Void context) {
                selectRelations.add(node);
                return null;
            }

            @Override
            public Void visitSetOp(SetOperationRelation node, Void context) {
                for (QueryRelation sub : node.getRelations()) {
                    selectRelations.addAll(collectBaseRelations(sub));
                }
                return null;
            }
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            final TableName tableNameObject = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableNameObject);
            final String tableName = tableNameObject.getTbl();
            try {
                FeNameFormat.checkTableName(tableName);
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
            }
            QueryStatement queryStatement = statement.getQueryStatement();
            // check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation) &&
                    !(queryStatement.getQueryRelation() instanceof SetOperationRelation)) {
                throw new SemanticException("Materialized view query statement only support select or set operation",
                        queryStatement.getQueryRelation().getPos());
            }

            List<SelectRelation> selectRelations =
                    SelectRelationCollector.collectBaseRelations(queryStatement.getQueryRelation());
            if (CollectionUtils.isEmpty(selectRelations)) {
                throw new SemanticException("Materialized view query statement must contain at least one select");
            }

            // derive alias
            final HashSet<String> aliases = Sets.newHashSet();
            for (SelectRelation selectRelation : selectRelations) {
                deriveSelectAlias(selectRelation.getSelectList(), aliases);
            }

            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);

            // for select star, use `analyze` to deduce its child's output and validate select list then.
            for (SelectRelation selectRelation : selectRelations) {
                // check alias except * and SlotRef
                validateSelectItem(selectRelation);
            }

            // convert queryStatement to sql and set
            statement.setInlineViewDef(AstToSQLBuilder.toSQL(queryStatement));
            statement.setSimpleViewDef(AstToSQLBuilder.buildSimple(queryStatement));
            // collect table from query statement
            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTableAndView(queryStatement);
            List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
            Database db = context.getGlobalStateMgr().getDb(statement.getTableName().getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + statement.getTableName().getDb(),
                        statement.getTableName().getPos());
            }
            if (tableNameTableMap.isEmpty()) {
                throw new SemanticException("Can not find base table in query statement");
            }
            tableNameTableMap.forEach((tableNameInfo, table) -> {
                Preconditions.checkState(table != null, "Materialized view base table is null");
                if (!isSupportBasedOnTable(table)) {
                    throw new SemanticException("Create materialized view do not support the table type: " +
                            table.getType(), tableNameInfo.getPos());
                }
                if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                    throw new SemanticException(
                            "Create materialized view from inactive materialized view: " + table.getName(),
                            tableNameInfo.getPos());
                }
                if (isExternalTableFromResource(table)) {
                    throw new SemanticException(
                            "Only supports creating materialized views based on the external table " +
                                    "which created by catalog", tableNameInfo.getPos());
                }
                Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableNameInfo.getCatalog(),
                        tableNameInfo.getDb());
                if (isInternalCatalog(tableNameInfo.getCatalog())) {
                    baseTableInfos.add(new BaseTableInfo(database.getId(), database.getFullName(),
                            table.getId()));
                } else {
                    baseTableInfos.add(new BaseTableInfo(tableNameInfo.getCatalog(),
                            tableNameInfo.getDb(), table.getTableIdentifier()));
                }
            });
            statement.setBaseTableInfos(baseTableInfos);
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            Map<TableName, Table> aliasTableMap = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);

            // get outputExpressions and convert it to columns which in selectRelation
            // set the columns into createMaterializedViewStatement
            // record the relationship between columns and outputExpressions for next check
            genColumnAndSetIntoStmt(statement, selectRelations.get(0), columnExprMap);
            // some check if partition exp exists
            if (statement.getPartitionExpDesc() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement, columnExprMap);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpParams(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, aliasTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement, aliasTableMap);

            planMVQuery(statement, queryStatement, context);
            return null;
        }

        private void deriveSelectAlias(SelectList selectList, HashSet<String> aliases) {
            for (SelectListItem selectListItem : selectList.getItems()) {
                if (!(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    String alias = Util.deriveAliasFromOrdinal(aliases.size());
                    selectListItem.setAlias(alias);
                    aliases.add(alias);
                }
            }
        }

        private void validateSelectItem(SelectRelation selectRelation) {
            for (SelectListItem selectListItem : selectRelation.getSelectList().getItems()) {
                Preconditions.checkState((selectListItem.getExpr() instanceof SlotRef)
                        || selectListItem.getAlias() != null);
            }
            for (Expr expr : selectRelation.getOutputExpression()) {
                checkNondeterministicFunction(expr);
            }
        }

        // TODO(murphy) implement
        // Plan the query statement and store in memory
        private void planMVQuery(CreateMaterializedViewStatement createStmt, QueryStatement query, ConnectContext ctx) {
            if (!ctx.getSessionVariable().isEnableIncrementalRefreshMV()) {
                return;
            }
            if (!createStmt.getRefreshSchemeDesc().getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                return;
            }

            try {
                ctx.getSessionVariable().setMVPlanner(true);

                QueryRelation queryRelation = query.getQueryRelation();
                ColumnRefFactory columnRefFactory = new ColumnRefFactory();
                LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, ctx).transform(queryRelation);
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
                List<ColumnRefOperator> outputColumns = new ArrayList<>();
                for (int colIdx = 0; colIdx < logicalPlan.getOutputColumn().size(); colIdx++) {
                    ColumnRefOperator ref = logicalPlan.getOutputColumn().get(colIdx);
                    outputColumns.add(ref);
                    columnRefMap.put(ref, ref);
                }

                // Build logical plan for view query
                OptExprBuilder optExprBuilder = logicalPlan.getRootBuilder();
                logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());
                Optimizer optimizer = new Optimizer();
                PhysicalPropertySet requiredPropertySet = PhysicalPropertySet.EMPTY;
                OptExpression optimizedPlan = optimizer.optimize(
                        ctx,
                        logicalPlan.getRoot(),
                        requiredPropertySet,
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        columnRefFactory);
                optimizedPlan.deriveMVProperty();

                // TODO: refine rules for mv plan
                // TODO: infer state
                // TODO: store the plan in create-mv statement and persist it at executor
                ExecPlan execPlan =
                        PlanFragmentBuilder.createPhysicalPlanForMV(ctx, createStmt, optimizedPlan, logicalPlan,
                                queryRelation, columnRefFactory);
            } catch (DdlException ex) {
                throw new RuntimeException(ex);
            } finally {
                ctx.getSessionVariable().setMVPlanner(false);
            }
        }

        private void checkNondeterministicFunction(Expr expr) {
            if (expr instanceof FunctionCallExpr) {
                if (((FunctionCallExpr) expr).isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function", expr.getPos());
                }
            }
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                checkNondeterministicFunction(child);
            }
        }

        private void genColumnAndSetIntoStmt(CreateMaterializedViewStatement statement, QueryRelation queryRelation,
                                             Map<Column, Expr> columnExprMap) {
            List<Column> mvColumns = Lists.newArrayList();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                Type type = AnalyzerUtils.transformTypeForMv(outputExpression.get(i).getType());
                Column column = new Column(columnOutputNames.get(i), type,
                        outputExpression.get(i).isNullable());
                // set default aggregate type, look comments in class Column
                column.setAggregationType(AggregateType.NONE, false);
                mvColumns.add(column);
                columnExprMap.put(column, outputExpression.get(i));
            }
            // set duplicate key
            int theBeginIndexOfValue = 0;
            int keySizeByte = 0;
            for (; theBeginIndexOfValue < mvColumns.size(); theBeginIndexOfValue++) {
                Column column = mvColumns.get(theBeginIndexOfValue);
                keySizeByte += column.getType().getIndexSize();
                if (theBeginIndexOfValue + 1 > FeConstants.SHORTKEY_MAX_COLUMN_COUNT ||
                        keySizeByte > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
                    if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                        column.setIsKey(true);
                        column.setAggregationType(null, false);
                        theBeginIndexOfValue++;
                    }
                    break;
                }
                if (!column.getType().canBeMVKey()) {
                    break;
                }
                if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    column.setIsKey(true);
                    column.setAggregationType(null, false);
                    theBeginIndexOfValue++;
                    break;
                }
                column.setIsKey(true);
                column.setAggregationType(null, false);
            }
            if (theBeginIndexOfValue == 0) {
                throw new SemanticException("Data type of first column cannot be " + mvColumns.get(0).getType());
            }
            statement.setMvColumnItems(mvColumns);
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement,
                                      Map<Column, Expr> columnExprMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = getSlotRef(expressionPartitionDesc.getExpr());
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                throw new SemanticException("Materialized view partition exp: "
                        + slotRef.toSql() + " must related to column", expressionPartitionDesc.getExpr().getPos());
            }
            int columnId = 0;
            for (Column column : columns) {
                if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                    statement.setPartitionColumn(column);
                    SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                            column.getType(), column.isAllowNull());
                    slotRef.setDesc(slotDescriptor);
                    break;
                }
                columnId++;
            }
            if (statement.getPartitionColumn() == null) {
                throw new SemanticException("Materialized view partition exp column:"
                        + slotRef.getColumnName() + " is not found in query statement");
            }
            checkExpWithRefExpr(statement, columnExprMap);
        }

        private void checkExpWithRefExpr(CreateMaterializedViewStatement statement,
                                         Map<Column, Expr> columnExprMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            Column partitionColumn = statement.getPartitionColumn();
            Expr refExpr = columnExprMap.get(partitionColumn);
            if (expressionPartitionDesc.isFunction()) {
                // e.g. partition by date_trunc('month', ss)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
                if (!(refExpr instanceof SlotRef)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column", functionCallExpr.getPos());
                }
                SlotRef slotRef = getSlotRef(functionCallExpr);
                slotRef.setType(partitionColumn.getType());
                // copy function and set it into partitionRefTableExpr
                Expr partitionRefTableExpr = functionCallExpr.clone();
                List<Expr> children = partitionRefTableExpr.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof SlotRef) {
                        partitionRefTableExpr.setChild(i, refExpr);
                    }
                }
                statement.setPartitionRefTableExpr(partitionRefTableExpr);
            } else {
                // e.g. partition by date_trunc('day',ss) or partition by ss
                if (refExpr instanceof FunctionCallExpr || refExpr instanceof SlotRef) {
                    statement.setPartitionRefTableExpr(refExpr);
                } else {
                    throw new SemanticException(
                            "Materialized view partition function must related with column", expressionPartitionDesc.getPos());
                }
            }
        }

        private void checkPartitionExpParams(CreateMaterializedViewStatement statement) {
            Expr expr = statement.getPartitionRefTableExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                CheckPartitionFunction checkPartitionFunction =
                        PartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support", functionCallExpr.getPos());
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed", functionCallExpr.getPos());
                }
            }
        }

        private void checkPartitionColumnWithBaseTable(CreateMaterializedViewStatement statement,
                                                       Map<TableName, Table> tableNameTableMap) {
            SlotRef slotRef = getSlotRef(statement.getPartitionRefTableExpr());
            Table table = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());

            if (table == null) {
                throw new SemanticException("Materialized view partition expression %s could only ref to base table",
                        slotRef.toSql());
            } else if (table.isNativeTable()) {
                checkPartitionColumnWithBaseOlapTable(slotRef, (OlapTable) table);
            } else if (table.isHiveTable() || table.isHudiTable()) {
                checkPartitionColumnWithBaseHMSTable(slotRef, (HiveMetaStoreTable) table);
            } else if (table.isIcebergTable()) {
                checkPartitionColumnWithBaseIcebergTable(slotRef, (IcebergTable) table);
            } else {
                throw new SemanticException("Materialized view do not support base table type : %s", table.getType());
            }
            replaceTableAlias(slotRef, statement, tableNameTableMap);
        }

        private void checkPartitionColumnWithBaseOlapTable(SlotRef slotRef, OlapTable table) {
            PartitionInfo partitionInfo = table.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() != 1) {
                    throw new SemanticException("Materialized view related base table partition columns " +
                            "only supports single column");
                }
                String partitionColumn = partitionColumns.get(0).getName();
                if (!partitionColumn.equalsIgnoreCase(slotRef.getColumnName())) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            } else {
                throw new SemanticException("Materialized view related base table partition type:" +
                        partitionInfo.getType().name() + "not supports");
            }
        }

        private void checkPartitionColumnWithBaseHMSTable(SlotRef slotRef, HiveMetaStoreTable table) {
            List<String> partitionColumnNames = table.getPartitionColumnNames();
            if (table.isUnPartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                boolean found = false;
                for (String partitionColumn : partitionColumnNames) {
                    if (partitionColumn.equalsIgnoreCase(slotRef.getColumnName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            }
        }

        private void checkPartitionColumnWithBaseIcebergTable(SlotRef slotRef, IcebergTable table) {
            org.apache.iceberg.Table icebergTable = table.getNativeTable();
            PartitionSpec partitionSpec = icebergTable.spec();
            if (partitionSpec.isUnpartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                boolean found = false;
                for (PartitionField partitionField : partitionSpec.fields()) {
                    String partitionColumn = partitionField.name();
                    if (partitionColumn.equalsIgnoreCase(slotRef.getColumnName())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            }
        }

        private SlotRef getSlotRef(Expr expr) {
            if (expr instanceof SlotRef) {
                return ((SlotRef) expr);
            } else {
                List<SlotRef> slotRefs = Lists.newArrayList();
                expr.collect(SlotRef.class, slotRefs);
                Preconditions.checkState(slotRefs.size() == 1);
                return slotRefs.get(0);
            }
        }

        private void replaceTableAlias(SlotRef slotRef,
                                       CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            TableName tableName = slotRef.getTblNameWithoutAnalyzed();
            Table table = tableNameTableMap.get(tableName);
            List<BaseTableInfo> baseTableInfos = statement.getBaseTableInfos();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                if (table.isNativeTable()) {
                    if (baseTableInfo.getTable().equals(table)) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                } else if (table.isHiveTable() || table.isHudiTable()) {
                    HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
                    if (hiveMetaStoreTable.getCatalogName().equals(baseTableInfo.getCatalogName()) &&
                            hiveMetaStoreTable.getDbName().equals(baseTableInfo.getDbName()) &&
                            table.getTableIdentifier().equals(baseTableInfo.getTableIdentifier())) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                } else if (table.isIcebergTable()) {
                    IcebergTable icebergTable = (IcebergTable) table;
                    if (icebergTable.getCatalogName().equals(baseTableInfo.getCatalogName()) &&
                            icebergTable.getRemoteDbName().equals(baseTableInfo.getDbName()) &&
                            table.getTableIdentifier().equals(baseTableInfo.getTableIdentifier())) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                }
            }
        }

        private void checkDistribution(CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            DistributionDesc distributionDesc = statement.getDistributionDesc();
            List<Column> mvColumnItems = statement.getMvColumnItems();
            Map<String, String> properties = statement.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
                statement.setProperties(properties);
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                        autoInferReplicationNum(tableNameTableMap).toString());
            }
            if (distributionDesc == null) {
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    distributionDesc = new HashDistributionDesc(0,
                            Lists.newArrayList(mvColumnItems.get(0).getName()));
                    statement.setDistributionDesc(distributionDesc);
                } else {
                    throw new SemanticException("Materialized view should contain distribution desc");
                }
            }
            Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (Column columnDef : mvColumnItems) {
                if (!columnSet.add(columnDef.getName())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
                }
            }
            distributionDesc.analyze(columnSet);
        }

        private Short autoInferReplicationNum(Map<TableName, Table> tableNameTableMap) {
            // For replication_num, we select the maximum value of all tables replication_num
            Short defaultReplicationNum = 1;
            for (Table table : tableNameTableMap.values()) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    Short replicationNum = olapTable.getDefaultReplicationNum();
                    if (replicationNum > defaultReplicationNum) {
                        defaultReplicationNum = replicationNum;
                    }
                }
            }
            return defaultReplicationNum;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement,
                                                        ConnectContext context) {
            statement.getMvName().normalization(context);
            final RefreshSchemeDesc refreshSchemeDesc = statement.getRefreshSchemeDesc();
            final String newMvName = statement.getNewMvName();
            if (newMvName != null) {
                if (statement.getMvName().getTbl().equals(newMvName)) {
                    throw new SemanticException("Same materialized view name " + newMvName, statement.getMvName().getPos());
                }
            } else if (refreshSchemeDesc != null) {
                if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
                    throw new SemanticException("Unsupported change to SYNC refresh type", refreshSchemeDesc.getPos());
                }
                if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
                    AsyncRefreshSchemeDesc async = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
                    final IntervalLiteral intervalLiteral = async.getIntervalLiteral();
                    if (intervalLiteral != null) {
                        long step = ((IntLiteral) intervalLiteral.getValue()).getLongValue();
                        if (step <= 0) {
                            throw new SemanticException("Unsupported negative or zero step value: " + step, async.getPos());
                        }
                        final String unit = intervalLiteral.getUnitIdentifier().getDescription().toUpperCase();
                        try {
                            RefreshTimeUnit.valueOf(unit);
                        } catch (IllegalArgumentException e) {
                            String msg = String.format("Unsupported interval unit: %s, only timeunit %s are supported", unit,
                                    Arrays.asList(RefreshTimeUnit.values()));
                            throw new SemanticException(msg, intervalLiteral.getUnitIdentifier().getPos());
                        }
                    }
                }
            } else if (statement.getModifyTablePropertiesClause() != null) {
                TableName mvName = statement.getMvName();
                Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
                if (db == null) {
                    throw new SemanticException("Can not find database:" + mvName.getDb(), mvName.getPos());
                }
                OlapTable table = (OlapTable) db.getTable(mvName.getTbl());
                if (table == null) {
                    throw new SemanticException("Can not find materialized view:" + mvName.getTbl(), mvName.getPos());
                }
                if (!(table instanceof MaterializedView)) {
                    throw new SemanticException(mvName.getTbl() + " is not async materialized view", mvName.getPos());
                }
            } else {
                throw new SemanticException("Unsupported modification for materialized view");
            }
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            statement.getMvName().normalization(context);
            TableName mvName = statement.getMvName();
            Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + mvName.getDb(), mvName.getPos());
            }
            OlapTable table = (OlapTable) db.getTable(mvName.getTbl());
            if (table == null) {
                throw new SemanticException("Can not find materialized view:" + mvName.getTbl(), mvName.getPos());
            }
            Preconditions.checkState(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;
            if (!mv.isActive()) {
                throw new SemanticException(
                        "Refresh materialized view failed because " + mv.getName() + " is not active", mvName.getPos());
            }
            if (statement.getPartitionRangeDesc() == null) {
                return null;
            }
            if (!(table.getPartitionInfo() instanceof RangePartitionInfo)) {
                throw new SemanticException("Not support refresh by partition for single partition mv", mvName.getPos());
            }
            Column partitionColumn =
                    ((RangePartitionInfo) table.getPartitionInfo()).getPartitionColumns().get(0);
            if (partitionColumn.getType().isDateType()) {
                validateDateTypePartition(statement.getPartitionRangeDesc());
            } else if (partitionColumn.getType().isIntegerType()) {
                validateNumberTypePartition(statement.getPartitionRangeDesc());
            } else {
                throw new SemanticException(
                        "Unsupported batch partition build type:" + partitionColumn.getType());
            }
            return null;
        }

        private void validateNumberTypePartition(PartitionRangeDesc partitionRangeDesc)
                throws SemanticException {
            String start = partitionRangeDesc.getPartitionStart();
            String end = partitionRangeDesc.getPartitionEnd();
            long startNum;
            long endNum;
            try {
                startNum = Long.parseLong(start);
                endNum = Long.parseLong(end);
            } catch (NumberFormatException ex) {
                throw new SemanticException("Batch build partition EVERY is number type " +
                        "but START or END does not type match", partitionRangeDesc.getPos());
            }
            if (startNum >= endNum) {
                throw new SemanticException("Batch build partition start value should less then end value",
                        partitionRangeDesc.getPos());
            }
        }

        private void validateDateTypePartition(PartitionRangeDesc partitionRangeDesc)
                throws SemanticException {
            String start = partitionRangeDesc.getPartitionStart();
            String end = partitionRangeDesc.getPartitionEnd();
            LocalDateTime startTime;
            LocalDateTime endTime;
            DateTimeFormatter startDateTimeFormat;
            DateTimeFormatter endDateTimeFormat;
            try {
                startDateTimeFormat = DateUtils.probeFormat(start);
                endDateTimeFormat = DateUtils.probeFormat(end);
                startTime = DateUtils.parseStringWithDefaultHSM(start, startDateTimeFormat);
                endTime = DateUtils.parseStringWithDefaultHSM(end, endDateTimeFormat);
            } catch (Exception ex) {
                throw new SemanticException("Batch build partition EVERY is date type " +
                        "but START or END does not type match", partitionRangeDesc.getPos());
            }
            if (!startTime.isBefore(endTime)) {
                throw new SemanticException("Batch build partition start date should less than end date",
                        partitionRangeDesc.getPos());
            }
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            statement.getMvName().normalization(context);
            return null;
        }
    }

    @FunctionalInterface
    public interface CheckPartitionFunction {

        boolean check(Expr expr);
    }

    static class PartitionFunctionChecker {

        public static final Map<String, CheckPartitionFunction> FN_NAME_TO_PATTERN;

        static {
            FN_NAME_TO_PATTERN = Maps.newHashMap();
            // can add some other functions
            FN_NAME_TO_PATTERN.put("date_trunc", PartitionFunctionChecker::checkDateTrunc);
        }

        public static boolean checkDateTrunc(Expr expr) {
            if (!(expr instanceof FunctionCallExpr)) {
                return false;
            }
            FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
            String fnNameString = fnExpr.getFnName().getFunction();
            if (!fnNameString.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                return false;
            }
            if (fnExpr.getChild(0) instanceof StringLiteral && fnExpr.getChild(1) instanceof SlotRef) {
                String fmt = ((StringLiteral) fnExpr.getChild(0)).getValue();
                if (fmt.equalsIgnoreCase("week")) {
                    throw new SemanticException("The function date_trunc used by the materialized view for partition" +
                            " does not support week formatting", expr.getPos());
                }
                SlotRef slotRef = (SlotRef) fnExpr.getChild(1);
                PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
                // must check slotRef type, because function analyze don't check it.
                return primitiveType == PrimitiveType.DATETIME || primitiveType == PrimitiveType.DATE;
            } else {
                return false;
            }
        }
    }

}
