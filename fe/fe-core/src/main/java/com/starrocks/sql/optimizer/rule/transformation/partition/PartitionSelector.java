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
package com.starrocks.sql.optimizer.rule.transformation.partition;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.information.PartitionsMetaSystemTable;
import com.starrocks.common.Pair;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner.doFurtherPartitionPrune;
import static com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner.isNeedFurtherPrune;
import static com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner.buildDeducedConjunct;
import static com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner.checkDeduceConjunct;

public class PartitionSelector {

    private static final Logger LOG = LogManager.getLogger(PartitionSelector.class);
    // why not use `PARTITION_ID` here? because partition_id in partitions_meta is physical partition id which may be confused.
    private static final String PARTITIONS_META_TEMPLATE = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS_META " +
            "WHERE DB_NAME ='%s' and TABLE_NAME='%s' AND %s;";
    private static final String EXTERNAL_TABLE_PARTITION_META_TEMPLATE = "SELECT PARTITION_NAME, SUM(ROW_COUNT) as ROW_COUNT," +
            "CAST(SUM(DATA_SIZE) AS BIGINT) as DATA_SIZE FROM _statistics_.external_column_statistics " +
            "WHERE TABLE_UUID = '%s' AND PARTITION_NAME in ('%s') GROUP BY PARTITION_NAME;";
    // NOTE: `json` to `datetime` is not supported yet, so we use `string` here.
    private static final String JSON_QUERY_TEMPLATE = "CAST(CAST(JSON_QUERY(%s, '$[0].[%d]') AS STRING) AS %s)";

    /**
     * Return filtered partition names by whereExpr.
     * @isRecyclingOrRetention, true for recycling/dropping condition, false for retention condition.
     */
    public static List<String> getPartitionNamesByExpr(ConnectContext context,
                                                       TableName tableName,
                                                       OlapTable olapTable,
                                                       Expr whereExpr,
                                                       boolean isDropPartitionCondition) {
        List<Long> selectedPartitionIds = getPartitionIdsByExpr(context, tableName, olapTable, whereExpr,
                isDropPartitionCondition);
        return selectedPartitionIds.stream()
                .map(p -> olapTable.getPartition(p))
                .map(Partition::getName)
                .collect(Collectors.toList());
    }

    /**
     * Return filtered partition ids by whereExpr.
     * @isRecyclingOrRetention, true for recycling/dropping condition, false for retention condition.
     */
    public static List<Long> getPartitionIdsByExpr(ConnectContext context,
                                                   TableName tableName,
                                                   OlapTable olapTable,
                                                   Expr whereExpr,
                                                   boolean isDropPartitionCondition) {
        return getPartitionIdsByExpr(context, tableName, olapTable, whereExpr, isDropPartitionCondition, null);
    }

    public static List<Long> getPartitionIdsByExpr(ConnectContext context,
                                                   TableName tableName,
                                                   OlapTable olapTable,
                                                   Expr whereExpr,
                                                   boolean isDropPartitionCondition,
                                                   Map<Expr, Expr> partitionByExprMap) {
        return getPartitionIdsByExpr(context, tableName, olapTable, whereExpr, isDropPartitionCondition,
                null, partitionByExprMap);
    }

    public static String getPartitionColumnDefinedQuery(OlapTable olapTable, Column column) {
        if (column.isGeneratedColumn()) {
            String definedQuery = column.generatedColumnExprToString();
            if (!Strings.isNullOrEmpty(definedQuery)) {
                return "`" + column.generatedColumnExprToString() + "`";
            }
            Expr deinfedExpr = column.getGeneratedColumnExpr(olapTable.getIdToColumn());
            if (deinfedExpr != null) {
                return deinfedExpr.toSqlWithoutTbl();
            }
            return "`" + column.getName() + "`";
        } else {
            return "`" + column.getName() + "`";
        }
    }

    /**
     * Return filtered partition ids by whereExpr with extra input cells which are used for mv refresh.
     */
    private static List<Long> getPartitionIdsByExpr(ConnectContext context,
                                                    TableName tableName,
                                                    OlapTable olapTable,
                                                    Expr whereExpr,
                                                    boolean isDropPartitionCondition,
                                                    Map<Long, PCell> inputCells,
                                                    Map<Expr, Expr> partitionByExprMap) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (!partitionInfo.isPartitioned()) {
            throw new SemanticException(String.format("Partition condition `%s` is supported for a partitioned table",
                    whereExpr.toSql()));
        }
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields(
                olapTable.getBaseSchema().stream()
                        .map(col -> new Field(col.getName(), col.getType(), tableName, null))
                        .collect(Collectors.toList())));
        ExpressionAnalyzer.analyzeExpression(whereExpr, new AnalyzeState(), scope, context);

        // replace partitionByExpr with partition slotRef if partitionByExprMap is not empty.
        if (olapTable.isMaterializedView() && CollectionUtils.sizeIsEmpty(partitionByExprMap)) {
            partitionByExprMap = MaterializedViewAnalyzer.getMVPartitionByExprToAdjustMap(tableName,
                    (MaterializedView) olapTable);
        }
        whereExpr = MaterializedViewAnalyzer.adjustWhereExprIfNeeded(partitionByExprMap, whereExpr, scope, context);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<Column, ColumnRefOperator> columnRefOperatorMap = Maps.newHashMap();
        List<ColumnRefOperator> columnRefOperators = Lists.newArrayList();
        for (Column col : olapTable.getBaseSchema()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(col.getName(),
                    col.getType(), col.isAllowNull());
            columnRefOperatorMap.put(col, columnRefOperator);
            columnRefOperators.add(columnRefOperator);
        }
        List<Column> partitionCols = olapTable.getPartitionColumns();
        Map<Column, Integer> partitionColIdxMap = Maps.newHashMap();
        for (int i = 0; i < partitionCols.size(); i++) {
            partitionColIdxMap.put(partitionCols.get(i), i);
        }
        Set<String> partitionColNames = partitionCols
                .stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        // NOTE: columnRefOperators's order should be same with olapTable.getBaseSchema()
        ExpressionMapping expressionMapping = new ExpressionMapping(scope, columnRefOperators);
        // expr to column index map
        Map<Expr, Integer> exprToColumnIdxes = Maps.newHashMap();
        Map<ScalarOperator, ColumnRefOperator> gcExprToColRefMap = Maps.newHashMap();
        for (Column column : olapTable.getBaseSchema()) {
            if (!partitionColNames.contains(column.getName())) {
                continue;
            }
            SlotRef slotRef = new SlotRef(tableName, column.getName());
            slotRef.setType(column.getType());
            Expr gcExpr = column.getGeneratedColumnExpr(olapTable.getIdToColumn());
            if (gcExpr == null) {
                exprToColumnIdxes.put(slotRef, partitionColIdxMap.get(column));
                continue;
            }
            exprToColumnIdxes.put(gcExpr, partitionColIdxMap.get(column));
            ExpressionAnalyzer.analyzeExpression(gcExpr, new AnalyzeState(), scope, context);
            ExpressionAnalyzer.analyzeExpression(slotRef, new AnalyzeState(), scope, context);
            ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(gcExpr,
                    expressionMapping, columnRefFactory);
            gcExprToColRefMap.put(scalarOperator, columnRefOperatorMap.get(column));
        }
        expressionMapping.addGeneratedColumnExprOpToColumnRef(gcExprToColRefMap);
        // substitute generated column expr if whereExpr is a mv which contains iceberg transform expr.
        // translate whereExpr to scalarOperator and replace whereExpr's generatedColumnExpr to partition slotRef.
        ScalarOperator scalarOperator =
                SqlToScalarOperatorTranslator.translate(whereExpr, expressionMapping, Lists.newArrayList(),
                        columnRefFactory, context, null, null, null, false);

        List<String> partitionDefinedQueries = partitionCols.stream()
                .map(col -> getPartitionColumnDefinedQuery(olapTable, col))
                .collect(Collectors.toUnmodifiableList());
        String partitionExpressions = Joiner.on("/").join(partitionDefinedQueries);
        if (scalarOperator == null) {
            throw new SemanticException(String.format("Failed to parse the partition condition:%s, please use " +
                            "table's partition expressions directly: %s", whereExpr.toSql(), partitionExpressions));
        }
        // validate scalar operator
        validateRetentionConditionPredicate(olapTable, scalarOperator);

        LOG.debug("Get partition ids by where expression: {}", scalarOperator.toString());

        // deduce generated column expr to partition slotRef
        try {
            scalarOperator = deduceGenerateColumns(scalarOperator, olapTable, columnRefFactory);
        } catch (Exception e) {
            LOG.warn("Failed to deduce generated column expr to partition slotRef: " + e.getMessage());
        }

        LOG.debug("Get partition ids by where expression after deduce: {}", scalarOperator.toString());

        List<ColumnRefOperator> usedPartitionColumnRefs = Lists.newArrayList();
        scalarOperator.getColumnRefs(usedPartitionColumnRefs);
        if (CollectionUtils.isEmpty(usedPartitionColumnRefs)) {
            throw new SemanticException(String.format("No partition columns are used in the partition condition: %s, " +
                    "please use table's partition expressions directly: %s" + whereExpr.toSql(), partitionExpressions));
        }
        // check if all used columns are partition columns
        for (ColumnRefOperator colRef : usedPartitionColumnRefs) {
            if (!partitionColNames.contains(colRef.getName())) {

                throw new SemanticException(String.format("Column `%s` in the partition condition is not a table's partition " +
                                "expression, please use table's partition expressions: %s",
                        colRef.getName(), partitionExpressions));
            }
        }
        List<Long> selectedPartitionIds;
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            selectedPartitionIds = getRangePartitionIdsByExpr(olapTable, rangePartitionInfo, scalarOperator,
                    columnRefOperatorMap, isDropPartitionCondition, inputCells);
        } else if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            selectedPartitionIds = getListPartitionIdsByExpr(tableName.getDb(), olapTable, listPartitionInfo,
                    whereExpr, scalarOperator, exprToColumnIdxes, isDropPartitionCondition, inputCells);
        } else {
            throw new SemanticException("Unsupported partition type: " + partitionInfo.getType());
        }
        if (selectedPartitionIds == null) {
            throw new SemanticException("Failed to get partitions with partition condition: " + whereExpr.toSql());
        }
        return selectedPartitionIds;
    }

    private static ScalarOperator deduceGenerateColumns(ScalarOperator scalarOperator,
                                              OlapTable olapTable, ColumnRefFactory columnRefFactory) {
        Map<String, ColumnRefOperator>  columnNameToColRefMap = Maps.newHashMap();
        List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);

        List<ColumnRefOperator> partitionColumnRefs = Lists.newArrayList();
        for (Column column : olapTable.getPartitionColumns()) {
            if (column.isGeneratedColumn()) {
                ColumnRefOperator columnRef = columnRefFactory.create(
                        column.getName(), column.getType(), column.isAllowNull());
                partitionColumnRefs.add(columnRef);
                columnRefOperatorList.add(columnRef);
            }
        }
        for (ColumnRefOperator columnRef : columnRefOperatorList) {
            Column column = olapTable.getColumn(columnRef.getName());
            if (column != null) {
                if (columnNameToColRefMap.containsKey(column.getName())) {
                    columnRef.setType(column.getType());
                }
                columnNameToColRefMap.put(column.getName(), columnRef);
            }
        }
        Function<SlotRef, ColumnRefOperator> slotRefResolver = (slot) -> {
            return columnNameToColRefMap.get(slot.getColumnName());
        };
        // The GeneratedColumn doesn't have the correct type info, let's help it
        Consumer<SlotRef> slotRefConsumer = (slot) -> {
            ColumnRefOperator ref = columnNameToColRefMap.get(slot.getColumnName());
            if (ref != null) {
                slot.setType(ref.getType());
            }
        };

        // Build a map of c1 -> c3, in which c3=fn(c1)
        Map<ColumnRefOperator, Pair<ColumnRefOperator, ScalarOperator>> refedGeneratedColumnMap = Maps.newHashMap();
        for (ColumnRefOperator partitionColumn : partitionColumnRefs) {
            Column column = olapTable.getColumn(partitionColumn.getName());
            if (column != null && column.isGeneratedColumn()) {
                Expr generatedExpr = column.getGeneratedColumnExpr(olapTable.getBaseSchema());
                ExpressionAnalyzer.analyzeExpressionResolveSlot(generatedExpr, ConnectContext.get(), slotRefConsumer);
                ScalarOperator call =
                        SqlToScalarOperatorTranslator.translateWithSlotRef(generatedExpr, slotRefResolver);

                if (call instanceof CallOperator &&
                        OperatorFunctionChecker.onlyContainMonotonicFunctions((CallOperator) call).first) {
                    columnRefOperatorList = Utils.extractColumnRef(call);

                    for (ColumnRefOperator ref : columnRefOperatorList) {
                        refedGeneratedColumnMap.put(ref, Pair.create(partitionColumn, call));
                    }
                }
            }
        }

        // No GeneratedColumn with partition column
        if (refedGeneratedColumnMap.isEmpty()) {
            return scalarOperator;
        }

        ScalarOperatorVisitor<ScalarOperator, Void> visitor = new ScalarOperatorVisitor<>() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
                if (checkDeduceConjunct(partitionColumnRefs, scalarOperator, columnRefOperatorList)) {
                    ColumnRefOperator referenced = columnRefOperatorList.get(0);
                    Pair<ColumnRefOperator, ScalarOperator> pair = refedGeneratedColumnMap.get(referenced);
                    if (pair != null) {
                        ColumnRefOperator generatedColumn = pair.first;
                        ScalarOperator generatedExpr = pair.second;
                        ScalarOperator result = buildDeducedConjunct(scalarOperator, generatedExpr, generatedColumn);
                        return result;
                    }
                }
                List<ScalarOperator> children = scalarOperator.getChildren();
                for (int i = 0; i < children.size(); ++i) {
                    ScalarOperator child = children.get(i);
                    ScalarOperator result = child.accept(this, context);
                    if (result != null) {
                        scalarOperator.setChild(i, result);
                    }
                }
                return scalarOperator;
            }
        };
        ScalarOperator result = scalarOperator.accept(visitor, null);
        if (result != null) {
            return result;
        } else {
            return scalarOperator;
        }
    }

    private static List<Long> getPartitionsByRetentionCondition(Database db,
                                                                OlapTable olapTable,
                                                                String ttlCondition,
                                                                Map<String, PCell> inputCells,
                                                                Map<Long, String> inputCellIdToNameMap,
                                                                boolean isMockPartitionIds) {
        TableName tableName = new TableName(db.getFullName(), olapTable.getName());
        ConnectContext context = ConnectContext.get() != null ? ConnectContext.get() : new ConnectContext();
        // needs to parse the expr each schedule because it can be changed dynamically
        // TODO: cache the parsed expr to avoid parsing it every time later.
        Expr whereExpr;
        try {
            whereExpr = SqlParser.parseSqlToExpr(ttlCondition, SqlModeHelper.MODE_DEFAULT);
            if (whereExpr == null) {
                LOG.warn("database={}, table={} failed to parse retention condition: {}",
                        db.getFullName(), olapTable.getName(), ttlCondition);
                return Lists.newArrayList();
            }
        } catch (Exception e) {
            throw new SemanticException("Failed to parse partition retention condition: " + ttlCondition);
        }

        // if isMockPartitionIds is true, we mock partition ids for input cells because the partition is not added into table
        // yet which may happen in mv's refresh `syncAddOrDropPartitions` process.
        // TODO: remove this mock partition ids logic if `PartitionPruner` can support to prune partitions by partition name
        //  rather than by ids.
        Map<Long, PCell> inputCellsMap = null;
        long initialPartitionId = 0;
        if (!CollectionUtils.sizeIsEmpty(inputCells)) {
            inputCellsMap = Maps.newHashMap();
            if (isMockPartitionIds) {
                for (Map.Entry<String, PCell> e : inputCells.entrySet()) {
                    inputCellsMap.put(initialPartitionId, e.getValue());
                    inputCellIdToNameMap.put(initialPartitionId, e.getKey());
                    initialPartitionId--;
                }
            } else {
                for (Map.Entry<String, PCell> e : inputCells.entrySet()) {
                    Partition partition = olapTable.getPartition(e.getKey());
                    inputCellsMap.put(partition.getId(), e.getValue());
                    inputCellIdToNameMap.put(partition.getId(), e.getKey());
                }
            }
        }
        return PartitionSelector.getPartitionIdsByExpr(context, tableName, olapTable,
                whereExpr, false, inputCellsMap, null);
    }

    private static List<String> getPartitionsByRetentionCondition(Database db,
                                                                  OlapTable olapTable,
                                                                  String ttlCondition,
                                                                  Map<String, PCell> inputCells,
                                                                  boolean isMockPartitionIds,
                                                                  Predicate<Pair<Set<Long>, Long>> pred) {

        Map<Long, String> inputCellIdToNameMap = Maps.newHashMap();
        List<Long> retentionPartitionIds = getPartitionsByRetentionCondition(db, olapTable, ttlCondition, inputCells,
                inputCellIdToNameMap, isMockPartitionIds);
        if (retentionPartitionIds == null) {
            return null;
        }
        Set<Long> retentionPartitionSet = Sets.newHashSet(retentionPartitionIds);
        List<String> result = olapTable.getVisiblePartitions().stream()
                .filter(p -> pred.apply(Pair.create(retentionPartitionSet, p.getId())))
                .map(Partition::getName)
                .collect(Collectors.toList());
        // if input cells are not empty, filter out the partitions which are expired too.
        if (!CollectionUtils.sizeIsEmpty(inputCells)) {
            Preconditions.checkArgument(inputCellIdToNameMap != null);
            inputCellIdToNameMap.entrySet().stream()
                    .filter(e -> pred.apply(Pair.create(retentionPartitionSet, e.getKey())))
                    .forEach(e -> result.add(e.getValue()));
        }
        return result;
    }

    public static List<String> getReservedPartitionsByRetentionCondition(Database db,
                                                                         OlapTable olapTable,
                                                                         String ttlCondition,
                                                                         Map<String, PCell> inputCells,
                                                                         boolean isMockPartitionIds) {
        return getPartitionsByRetentionCondition(db, olapTable, ttlCondition, inputCells, isMockPartitionIds,
                (pair) -> pair.first.contains(pair.second));
    }

    /**
     * Get expired partitions by retention condition.
     */
    public static List<String> getExpiredPartitionsByRetentionCondition(Database db,
                                                                        OlapTable olapTable,
                                                                        String ttlCondition) {
        return getExpiredPartitionsByRetentionCondition(db, olapTable, ttlCondition, null, false);
    }

    /**
     * Get expired partitions by retention condition.
     * inputCells and isMockPartitionIds are used for mv refresh since the partition is not added into table yet, but
     * we need to check whether inputCells are expired or created for mv refresh.
     */
    public static List<String> getExpiredPartitionsByRetentionCondition(Database db,
                                                                        OlapTable olapTable,
                                                                        String ttlCondition,
                                                                        Map<String, PCell> inputCells,
                                                                        boolean isMockPartitionIds) {
        return getPartitionsByRetentionCondition(db, olapTable, ttlCondition, inputCells, isMockPartitionIds,
                (pair) -> !pair.first.contains(pair.second));
    }

    private static Map<ColumnRefOperator, ScalarOperator> buildReplaceMap(Map<ColumnRefOperator, Integer> colRefIdxMap,
                                                                          List<LiteralExpr> values) {
        // columnref -> literal
        Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Integer> entry : colRefIdxMap.entrySet()) {
            ColumnRefOperator colRef = entry.getKey();
            ConstantOperator replace =
                    (ConstantOperator) SqlToScalarOperatorTranslator.translate(values.get(entry.getValue()));
            replaceMap.put(colRef, replace);
        }
        return replaceMap;
    }

    private static Map<ColumnRefOperator, ScalarOperator> buildReplaceMapWithCell(Map<ColumnRefOperator, Integer> colRefIdxMap,
                                                                                  List<String> values) {
        // columnref -> literal
        Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Integer> entry : colRefIdxMap.entrySet()) {
            ColumnRefOperator colRef = entry.getKey();
            try {
                LiteralExpr literalExpr = LiteralExpr.create(values.get(entry.getValue()), colRef.getType());
                ConstantOperator replace = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                replaceMap.put(colRef, replace);
            } catch (Exception e) {
                LOG.warn("Failed to create literal expr for value: {}", values.get(entry.getValue()));
                return null;
            }
        }
        return replaceMap;
    }

    private static List<Long> getRangePartitionIdsByExpr(OlapTable olapTable,
                                                         RangePartitionInfo rangePartitionInfo,
                                                         ScalarOperator predicate,
                                                         Map<Column, ColumnRefOperator> columnRefOperatorMap,
                                                         boolean isDropPartitionCondition,
                                                         Map<Long, PCell> inputCells) {
        // clone it to avoid changing the original map
        Map<Long, Range<PartitionKey>> keyRangeById = Maps.newHashMap(rangePartitionInfo.getIdToRange(false));
        if (!CollectionUtils.sizeIsEmpty(inputCells)) {
            // mock partition ids since input cells has not been added into olapTable yet.
            inputCells.entrySet().stream()
                    .forEach(e -> {
                        PRangeCell pRangeCell = (PRangeCell) e.getValue();
                        keyRangeById.put(e.getKey(), pRangeCell.getRange());
                    });

        }
        // since partition pruning is false positive which means it may not prune some partitions which should be pruned
        // but it will not prune partitions which should not be pruned.
        ScalarOperator fpPredicate = predicate;
        if (isDropPartitionCondition) {
            // use not condition to prune partitions because it's not safe to prune partitions with where expression directly,
            ScalarOperator notWhereExpr =
                    new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, predicate);
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            fpPredicate = rewriter.rewrite(notWhereExpr, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        }

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(fpPredicate);
        ImmutableMap<String, PartitionColumnFilter> columnFilters = ImmutableMap.copyOf(
                ColumnFilterConverter.convertColumnFilter(conjuncts, olapTable));
        PartitionPruner partitionPruner = new RangePartitionPruner(keyRangeById,
                rangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn()), columnFilters);
        List<Long> selectedPartitionIds;
        try {
            selectedPartitionIds = partitionPruner.prune();
            // do more prune if necessary
            if (isNeedFurtherPrune(olapTable, selectedPartitionIds, fpPredicate, rangePartitionInfo, Maps.newHashMap())) {
                List<Range<PartitionKey>> candidateRanges = selectedPartitionIds.stream()
                        .map(keyRangeById::get)
                        .filter(range -> range != null)
                        .collect(Collectors.toList());
                selectedPartitionIds = doFurtherPartitionPrune(olapTable, fpPredicate,
                        columnRefOperatorMap, selectedPartitionIds, candidateRanges);
            }
            if (selectedPartitionIds == null) {
                throw new SemanticException("Failed to prune partitions with partition condition: " + predicate.toString());
            }
        } catch (Exception e) {
            throw new SemanticException("Failed to prune partitions with partition condition: " + e.getMessage());
        }
        // check if all used columns are partition columns
        if (isDropPartitionCondition) {
            // for recycling, we need to return the partitions which should not be pruned.
            Set<Long> notMatchedPartitionIds = Sets.newHashSet(selectedPartitionIds);
            return keyRangeById.keySet().stream()
                    .filter(id -> !notMatchedPartitionIds.contains(id))
                    .collect(Collectors.toList());
        } else {
            return selectedPartitionIds;
        }
    }

    private static List<Long> getListPartitionIdsByExpr(String dbName, OlapTable olapTable,
                                                        ListPartitionInfo listPartitionInfo,
                                                        Expr whereExpr,
                                                        ScalarOperator scalarOperator,
                                                        Map<Expr, Integer> exprToColumnIdxes,
                                                        boolean isDropPartitionCondition,
                                                        Map<Long, PCell> inputCells) {

        List<Long> result = null;
        // try to prune partitions by FE's constant evaluation ability
        try {
            result = getPartitionsByFEConstantEvaluation(olapTable, listPartitionInfo, scalarOperator,
                    isDropPartitionCondition, inputCells);
            if (result != null) {
                return result;
            }
        } catch (Exception e) {
            LOG.warn("Failed to prune partitions by FE's constant evaluation, transform to partitions_meta instead",
                    e);
        }

        String sql = "";
        try {
            sql = buildPartitionSelectQuery(dbName, olapTable, whereExpr, exprToColumnIdxes);
        } catch (Exception e) {
            LOG.warn("Failed to build partition select query: " + e.getMessage());
            throw new SemanticException("Build partition query from information_schema.partition_meta failed: "
                    + e.getMessage());
        }
        if (Strings.isNullOrEmpty(sql)) {
            throw new SemanticException("Build partition query from information_schema.partition_meta failed: " + sql);
        }

        try {
            // TODO: support to prune extra inputCells.
            return getPartitionsByQuerying(olapTable, listPartitionInfo, sql);
        } catch (Exception e) {
            LOG.warn("Get partitions from information_schema.partition_meta querying failed: " + e.getMessage());
            throw new SemanticException("Get partitions from information_schema.partition_meta querying failed:\n " + sql);
        }
    }

    static class Recorder {
        // used to record the state of eval result
        private boolean isConstTrue = false;

        public Recorder() {
        }

        public boolean isConstTrue() {
            return isConstTrue;
        }
    }

    /**
     * Check if the eval result is satisfied and short-circuit the evaluation.
     // TODO: refactor and move it to a common place
     * @param rewriter : rewriter to rewrite the partition condition scalar operator by constant fold and so on
     * @param replaceColumnRefRewriter: replace columnRef with literal
     * @param scalarOperator : partition condition scalar operator
     * @param isDropPartitionCondition : true for recycling/dropping condition, false for retention condition.
     * @param record : used to record the state of eval result
     * @return : true if the eval result is satisfied, false if not satisfied, null if the eval result is not set.
     */
    private static Optional<Boolean> isEvalResultSatisfied(ScalarOperatorRewriter rewriter,
                                                           ReplaceColumnRefRewriter replaceColumnRefRewriter,
                                                           ScalarOperator scalarOperator,
                                                           boolean isDropPartitionCondition,
                                                           Recorder record) {
        ScalarOperator result = replaceColumnRefRewriter.rewrite(scalarOperator);
        result = rewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        if (!result.isConstant()) {
            return Optional.empty();
        }
        if (isDropPartitionCondition) {
            // for recycling condition, keep partitions as less as possible
            // T1, partitions:
            //  p1: [1, 2]
            //  p2: [2, 3]
            // eg: alter table T1 drop partitions where p > 1, only choose p1 when all values are satisfied.
            // p1: [1, 2] is not satisfied, so we need to return p2: [2, 3]
            if (result.isConstantFalse()) {
                record.isConstTrue = false;
                return Optional.of(true);
            } else if (result.isConstantTrue()) {
                record.isConstTrue = true;
            } else {
                return Optional.empty();
            }
        } else {
            // for retention condition, keep partitions as much as possible
            // T1, partitions:
            //  p1: [1, 2]
            //  p2: [2, 3]
            // eg: partition_retention_condition = 'p > 1', choose partition if it once is satisfied.
            // p1: [1, 2]/[2, 3] are all satisfied, so we need to return p1/p2 both
            if (result.isConstantFalse()) {
                record.isConstTrue = false;
            } else if (result.isConstantTrue()) {
                record.isConstTrue = true;
                return Optional.of(true);
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(false);
    }

    /**
     * Fetch selected partition ids by using FE's constant evaluation ability.
     */
    private static List<Long> getPartitionsByFEConstantEvaluation(OlapTable olapTable,
                                                                  ListPartitionInfo listPartitionInfo,
                                                                  ScalarOperator scalarOperator,
                                                                  boolean isDropPartitionCondition,
                                                                  Map<Long, PCell> inputCells) {
        // eval for each conjunct
        Map<ColumnRefOperator, Integer> colRefIdxMap = Maps.newHashMap();
        List<String> partitionColNames = olapTable.getPartitionColumns().stream()
                .map(Column::getName).collect(Collectors.toList());
        List<ColumnRefOperator> usedPartitionColumnRefs = Lists.newArrayList();
        scalarOperator.getColumnRefs(usedPartitionColumnRefs);
        for (ColumnRefOperator colRef : usedPartitionColumnRefs) {
            Preconditions.checkArgument(partitionColNames.contains(colRef.getName()));
            colRefIdxMap.put(colRef, partitionColNames.indexOf(colRef.getName()));
        }

        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<Long> selectedPartitionIds = Lists.newArrayList();
        // single partition column
        Map<Long, List<LiteralExpr>> listPartitions = listPartitionInfo.getLiteralExprValues();
        final Recorder recorder = new Recorder();
        for (Map.Entry<Long, List<LiteralExpr>> e : listPartitions.entrySet()) {
            for (LiteralExpr literalExpr : e.getValue()) {
                Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
                ConstantOperator replace = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                replaceMap.put(usedPartitionColumnRefs.get(0), replace);

                // replace columnRef with literal
                final ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                // check for each partition value
                final Optional<Boolean> evalResultSatisfied = isEvalResultSatisfied(rewriter, replaceColumnRefRewriter,
                        scalarOperator, isDropPartitionCondition, recorder);
                // if eval result is not set, return it directly
                if (evalResultSatisfied == null || evalResultSatisfied.isEmpty()) {
                    return null;
                }
                // if eval result is satisfied, break the loop
                if (evalResultSatisfied.get()) {
                    break;
                }
            }
            if (recorder.isConstTrue()) {
                selectedPartitionIds.add(e.getKey());
            }
        }
        // multi partition columns
        Map<Long, List<List<LiteralExpr>>> multiListPartitions = listPartitionInfo.getMultiLiteralExprValues();
        for (Map.Entry<Long, List<List<LiteralExpr>>> e : multiListPartitions.entrySet()) {
            for (List<LiteralExpr> values : e.getValue()) {
                final Map<ColumnRefOperator, ScalarOperator> replaceMap = buildReplaceMap(colRefIdxMap, values);
                final ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);

                // check for each partition value
                final Optional<Boolean> evalResultSatisfied = isEvalResultSatisfied(rewriter, replaceColumnRefRewriter,
                        scalarOperator, isDropPartitionCondition, recorder);
                // if eval result is not set, return it directly
                if (evalResultSatisfied == null || evalResultSatisfied.isEmpty()) {
                    return null;
                }
                // if eval result is satisfied, break the loop
                if (evalResultSatisfied.get()) {
                    break;
                }
            }
            if (recorder.isConstTrue()) {
                selectedPartitionIds.add(e.getKey());
            }
        }
        if (!CollectionUtils.sizeIsEmpty(inputCells)) {
            for (Map.Entry<Long, PCell> e : inputCells.entrySet()) {
                PListCell pListCell = (PListCell) e.getValue();
                for (List<String> values : pListCell.getPartitionItems()) {
                    final Map<ColumnRefOperator, ScalarOperator> replaceMap = buildReplaceMapWithCell(colRefIdxMap, values);
                    if (replaceMap == null) {
                        return null;
                    }
                    final ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);

                    // check for each partition value
                    final Optional<Boolean> evalResultSatisfied = isEvalResultSatisfied(rewriter, replaceColumnRefRewriter,
                            scalarOperator, isDropPartitionCondition, recorder);
                    // if eval result is not set, return it directly
                    if (evalResultSatisfied == null || evalResultSatisfied.isEmpty()) {
                        return null;
                    }
                    // if eval result is satisfied, break the loop
                    if (evalResultSatisfied.get()) {
                        break;
                    }
                }
                if (recorder.isConstTrue()) {
                    selectedPartitionIds.add(e.getKey());
                }
            }
        }
        return selectedPartitionIds;
    }

    /**
     * Use `_statistics_.external_column_statistics` to get partition statistics for an external table
     * such as Iceberg/Hudi by its table UUID.
     */
    public static Map<String, Pair<Long, Long>> getExternalTablePartitionStats(Table table, Set<String> needPartitionNames) {
        String sql = String.format(EXTERNAL_TABLE_PARTITION_META_TEMPLATE, table.getUUID(),
                String.join(",", needPartitionNames));
        LOG.info("Get external table partition stats by sql: {}", sql);
        List<TResultBatch> batch = SimpleExecutor.getRepoExecutor().executeDQL(sql);
        return deserializeExternalStatisticsResult(batch);
    }

    private static Map<String, Pair<Long, Long>> deserializeExternalStatisticsResult(
            List<TResultBatch> batches) {
        Map<String, Pair<Long, Long>> result = new HashMap<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(StandardCharsets.UTF_8);
                List<String> data = MetaFunctions.LookupRecord.fromJson(jsonString).data;

                if (data != null && data.size() >= 3) {
                    String partitionName = data.get(0);
                    long rowCount = Long.parseLong(data.get(1));
                    long dataSize = Long.parseLong(data.get(2));
                    result.put(partitionName, Pair.create(rowCount, dataSize));
                }
            }
        }
        return result;
    }

    private static String buildPartitionSelectQuery(String dbName,
                                                    OlapTable olapTable,
                                                    Expr whereExpr,
                                                    Map<Expr, Integer> exprToColumnIdxes) {
        Database infoSchemaDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(InfoSchemaDb.DATABASE_NAME);
        if (infoSchemaDb == null) {
            return null;
        }
        Table partitionsMetaTbl = infoSchemaDb.getTable(PartitionsMetaSystemTable.NAME);
        if (partitionsMetaTbl == null) {
            return null;
        }
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        List<Column> partitionCols = olapTable.getPartitionColumns();
        for (Map.Entry<Expr, Integer> e : exprToColumnIdxes.entrySet()) {
            Expr alias = buildJsonQuery(partitionCols, e.getValue());
            aliasMap.put(e.getKey(), alias);
        }
        Expr newExpr = whereExpr.substitute(aliasMap);
        String newWhereSql = newExpr.toSql();
        String sql = String.format(PARTITIONS_META_TEMPLATE, dbName, olapTable.getName(), newWhereSql);
        LOG.info("Get partition ids by sql: {}", sql);
        return sql;
    }

    /**
     * Use `information_schema.partitions_meta` to filter selected partition names by using whereExpr.
     */
    private static List<Long> getPartitionsByQuerying(OlapTable olapTable,
                                                      ListPartitionInfo listPartitionInfo,
                                                      String sql) {
        List<TResultBatch> batch = SimpleExecutor.getRepoExecutor().executeDQL(sql);
        List<String> partitionNames = deserializeLookupResult(batch);
        // multi items in the single partition is not supported yet since `JSON_QUERY_TEMPLATE` is constructed the first element.
        Set<Long> excludedPartitionIds = Sets.newHashSet();
        listPartitionInfo.getLiteralExprValues().entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .forEach(excludedPartitionIds::add);
        listPartitionInfo.getMultiLiteralExprValues().entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .forEach(excludedPartitionIds::add);
        // ensure all partition names are existed
        return partitionNames.stream()
                .map(olapTable::getPartition)
                .map(Partition::getId)
                .filter(id -> !excludedPartitionIds.contains(id))
                .collect(Collectors.toList());
    }

    private static List<String> deserializeLookupResult(List<TResultBatch> batches) {
        List<String> result = Lists.newArrayList();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                String jsonString = copied.toString(Charset.defaultCharset());
                List<String> data = MetaFunctions.LookupRecord.fromJson(jsonString).data;
                if (CollectionUtils.isNotEmpty(data)) {
                    result.add(data.get(0));
                }
            }
        }
        return result;
    }

    private static Expr buildJsonQuery(List<Column> partitionCols,
                                       int partitionColumnIndex) {
        Preconditions.checkArgument(partitionColumnIndex >= 0 && partitionColumnIndex < partitionCols.size());
        Column partitionCol = partitionCols.get(partitionColumnIndex);
        String partitionColType = partitionCol.getType().toSql();
        String jsonQuery = String.format(JSON_QUERY_TEMPLATE, "PARTITION_VALUE", partitionColumnIndex, partitionColType);
        Expr expr = SqlParser.parseSqlToExpr(jsonQuery, SqlModeHelper.MODE_DEFAULT);
        return expr;
    }

    public static void validateRetentionConditionPredicate(OlapTable olapTable,
                                                           ScalarOperator predicate) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.isListPartition()) {
            // support common partition expressions for list partition tables
        } else if (partitionInfo.isRangePartition()) {
            Pair<Boolean, String> result = OperatorFunctionChecker.onlyContainMonotonicFunctions(predicate);
            if (!result.first) {
                throw new SemanticException("Retention condition must only contain monotonic functions for range partition " +
                        "tables but contains: " + result.second);
            }
        } else {
            throw new SemanticException("Unsupported partition type: " + partitionInfo.getType() + " for retention condition");
        }

        // extra check for materialized view
        if (olapTable instanceof MaterializedView) {
            Pair<Boolean, String> result = OperatorFunctionChecker.onlyContainFEConstantFunctions(predicate);
            if (!result.first) {
                throw new SemanticException("Retention condition must only contain FE constant functions for materialized" +
                        " view but contains: " + result.second);
            }
        }
    }
}