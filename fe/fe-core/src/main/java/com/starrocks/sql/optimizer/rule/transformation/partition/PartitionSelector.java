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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
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
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import jline.internal.Log;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner.doFurtherPartitionPrune;
import static com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner.isNeedFurtherPrune;

public class PartitionSelector {

    private static final Logger LOG = LogManager.getLogger(PartitionSelector.class);
    // why not use `PARTITION_ID` here? because partition_id in partitions_meta is physical partition id which may be confused.
    private static final String PARTITIONS_META_TEMPLATE = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS_META " +
            "WHERE DB_NAME ='%s' and TABLE_NAME='%s' AND %s;";
    private static final String JSON_QUERY_TEMPLATE = "CAST(JSON_QUERY(%s, '$[0].[%d]') AS %s)";

    /**
     * Return filtered partition names by whereExpr.
     * @isRecyclingOrRetention, true for recycling/dropping condition, false for retention condition.
     */
    public static List<String> getPartitionNamesByExpr(ConnectContext context,
                                                       TableName tableName,
                                                       OlapTable olapTable,
                                                       Expr whereExpr,
                                                       boolean isRecyclingCondition) {
        List<Long> selectedPartitionIds = getPartitionIdsByExpr(context, tableName, olapTable, whereExpr,
                isRecyclingCondition);
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
                                                   boolean isRecyclingCondition) {
        return getPartitionIdsByExpr(context, tableName, olapTable, whereExpr, isRecyclingCondition, null);
    }

    public static List<Long> getPartitionIdsByExpr(ConnectContext context,
                                                   TableName tableName,
                                                   OlapTable olapTable,
                                                   Expr whereExpr,
                                                   boolean isRecyclingCondition,
                                                   Map<Expr, Expr> partitionByExprMap) {
        return getPartitionIdsByExpr(context, tableName, olapTable, whereExpr, isRecyclingCondition,
                null, partitionByExprMap);
    }

    /**
     * Return filtered partition ids by whereExpr with extra input cells which are used for mv refresh.
     */
    private static List<Long> getPartitionIdsByExpr(ConnectContext context,
                                                    TableName tableName,
                                                    OlapTable olapTable,
                                                    Expr whereExpr,
                                                    boolean isRecyclingCondition,
                                                    Map<Long, PCell> inputCells,
                                                    Map<Expr, Expr> partitionByExprMap) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (!partitionInfo.isPartitioned()) {
            throw new SemanticException("Can't drop partitions with where expression since it is not partitioned");
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
        if (scalarOperator == null) {
            throw new SemanticException("Failed to translate where expression to scalar operator:" + whereExpr.toSql());
        }
        // validate scalar operator
        validateRetentionConditionPredicate(olapTable, scalarOperator);

        List<ColumnRefOperator> usedPartitionColumnRefs = Lists.newArrayList();
        scalarOperator.getColumnRefs(usedPartitionColumnRefs);
        if (CollectionUtils.isEmpty(usedPartitionColumnRefs)) {
            throw new SemanticException("No partition column is used in where clause for drop partition: " + whereExpr.toSql());
        }
        // check if all used columns are partition columns
        for (ColumnRefOperator colRef : usedPartitionColumnRefs) {
            if (!partitionColNames.contains(colRef.getName())) {
                throw new SemanticException("Column is not a partition column which can not" +
                        " be used in where clause for drop partition: " + colRef.getName());
            }
        }
        List<Long> selectedPartitionIds;
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            selectedPartitionIds = getRangePartitionIdsByExpr(olapTable, rangePartitionInfo, scalarOperator,
                    columnRefOperatorMap, isRecyclingCondition, inputCells);
        } else if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            selectedPartitionIds = getListPartitionIdsByExpr(tableName.getDb(), olapTable, listPartitionInfo,
                    whereExpr, scalarOperator, exprToColumnIdxes, inputCells);
        } else {
            throw new SemanticException("Unsupported partition type: " + partitionInfo.getType());
        }
        if (selectedPartitionIds == null) {
            throw new SemanticException("Failed to prune partitions with where expression: " + whereExpr.toSql());
        }
        return selectedPartitionIds;
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
            throw new SemanticException("Failed to parse retention condition: " + ttlCondition);
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
                                                         boolean isRecyclingCondition,
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
        if (isRecyclingCondition) {
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
                throw new SemanticException("Failed to prune partitions with where expression: " + predicate.toString());
            }
        } catch (Exception e) {
            throw new SemanticException("Failed to prune partitions with where expression: " + e.getMessage());
        }
        // check if all used columns are partition columns
        if (isRecyclingCondition) {
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
                                                        Map<Long, PCell> inputCells) {

        List<Long> result = null;
        // try to prune partitions by FE's constant evaluation ability
        try {
            result = getListPartitionIdsByExprV1(olapTable, listPartitionInfo, scalarOperator, inputCells);
            if (result != null) {
                return result;
            }
        } catch (Exception e1) {
            Log.info("Failed to prune partitions by FE's constant evaluation, transform to partitions_meta instead.");
        }

        try {
            // TODO: support to prune extra inputCells.
            return getListPartitionIdsByExprV2(dbName, olapTable, listPartitionInfo, whereExpr, exprToColumnIdxes);
        } catch (Exception e2) {
            LOG.warn("Failed to prune partitions with where expression(v2): " + e2.getMessage());
            throw new SemanticException("Failed to prune partitions with where expression: " + whereExpr.toSql());
        }
    }

    /**
     * Fetch selected partition ids by using FE's constant evaluation ability.
     */
    private static List<Long> getListPartitionIdsByExprV1(OlapTable olapTable,
                                                          ListPartitionInfo listPartitionInfo,
                                                          ScalarOperator scalarOperator,
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
        for (Map.Entry<Long, List<LiteralExpr>> e : listPartitions.entrySet()) {
            boolean isConstTrue = false;
            for (LiteralExpr literalExpr : e.getValue()) {
                Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
                ConstantOperator replace = (ConstantOperator) SqlToScalarOperatorTranslator.translate(literalExpr);
                replaceMap.put(usedPartitionColumnRefs.get(0), replace);

                // replace columnRef with literal
                ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                ScalarOperator result = replaceColumnRefRewriter.rewrite(scalarOperator);
                result = rewriter.rewrite(result, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);

                if (!result.isConstant()) {
                    return null;
                }
                if (result.isConstantFalse()) {
                    isConstTrue = false;
                    break;
                } else if (result.isConstantTrue()) {
                    isConstTrue = true;
                } else {
                    return null;
                }
            }
            if (isConstTrue) {
                selectedPartitionIds.add(e.getKey());
            }
        }
        // multi partition columns
        Map<Long, List<List<LiteralExpr>>> multiListPartitions = listPartitionInfo.getMultiLiteralExprValues();
        for (Map.Entry<Long, List<List<LiteralExpr>>> e : multiListPartitions.entrySet()) {
            boolean isConstTrue = false;
            for (List<LiteralExpr> values : e.getValue()) {
                Map<ColumnRefOperator, ScalarOperator> replaceMap = buildReplaceMap(colRefIdxMap, values);
                ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                ScalarOperator result = replaceColumnRefRewriter.rewrite(scalarOperator);
                result = rewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                if (!result.isConstant()) {
                    return null;
                }
                if (result.isConstantFalse()) {
                    isConstTrue = false;
                    break;
                } else if (result.isConstantTrue()) {
                    isConstTrue = true;
                } else {
                    return null;
                }
            }
            if (isConstTrue) {
                selectedPartitionIds.add(e.getKey());
            }
        }
        if (!CollectionUtils.sizeIsEmpty(inputCells)) {
            for (Map.Entry<Long, PCell> e : inputCells.entrySet()) {
                boolean isConstTrue = false;
                PListCell pListCell = (PListCell) e.getValue();
                for (List<String> values : pListCell.getPartitionItems()) {
                    Map<ColumnRefOperator, ScalarOperator> replaceMap = buildReplaceMapWithCell(colRefIdxMap, values);
                    if (replaceMap == null) {
                        return null;
                    }
                    ReplaceColumnRefRewriter replaceColumnRefRewriter = new ReplaceColumnRefRewriter(replaceMap);
                    ScalarOperator result = replaceColumnRefRewriter.rewrite(scalarOperator);
                    result = rewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                    if (!result.isConstant()) {
                        return null;
                    }
                    if (result.isConstantFalse()) {
                        isConstTrue = false;
                        break;
                    } else if (result.isConstantTrue()) {
                        isConstTrue = true;
                    } else {
                        return null;
                    }
                }
                if (isConstTrue) {
                    selectedPartitionIds.add(e.getKey());
                }
            }
        }
        return selectedPartitionIds;
    }

    /**
     * Use `information_schema.partitions_meta` to filter selected partition names by using whereExpr.
     */
    private static List<Long> getListPartitionIdsByExprV2(String dbName,
                                                          OlapTable olapTable,
                                                          ListPartitionInfo listPartitionInfo,
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