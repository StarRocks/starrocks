// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;

public class RemoteScanPartitionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(RemoteScanPartitionPruneRule.class);

    public static final RemoteScanPartitionPruneRule HIVE_SCAN =
            new RemoteScanPartitionPruneRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final RemoteScanPartitionPruneRule HUDI_SCAN =
            new RemoteScanPartitionPruneRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final RemoteScanPartitionPruneRule ICEBERG_SCAN =
            new RemoteScanPartitionPruneRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final RemoteScanPartitionPruneRule DELTALAKE_SCAN =
            new RemoteScanPartitionPruneRule(OperatorType.LOGICAL_DELTALAKE_SCAN);

    public static final RemoteScanPartitionPruneRule FILE_SCAN =
            new RemoteScanPartitionPruneRule(OperatorType.LOGICAL_FILE_SCAN);

    public RemoteScanPartitionPruneRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator operator = (LogicalScanOperator) input.getOp();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        // no null partitions in this map, used by ListPartitionPruner
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // Store partitions with null partition values separately, used by ListPartitionPruner
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();

        try {
            initPartitionInfo(operator, context, columnToPartitionValuesMap, columnToNullPartitions);
            classifyConjuncts(operator, columnToPartitionValuesMap);
            computePartitionInfo(operator, columnToPartitionValuesMap, columnToNullPartitions);
        } catch (Exception e) {
            LOG.warn("HMS table partition prune failed : " + e);
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
        }
        return Collections.emptyList();
    }

    private void initPartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                   Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                   Map<ColumnRefOperator, Set<Long>> columnToNullPartitions)
            throws AnalysisException {
        Table table = operator.getTable();
        // RemoteScanPartitionPruneRule may be run multiple times, such like after MaterializedViewRewriter rewrite，
        // the predicates of scan operator may changed, it need to re-compute the ScanOperatorPredicates.
        operator.getScanOperatorPredicates().clear();
        if (table instanceof HiveMetaStoreTable) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            List<Column> partitionColumns = hmsTable.getPartitionColumns();
            List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
                columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
                partitionColumnRefOperators.add(partitionColumnRefOperator);
            }

            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
            context.getDumpInfo().getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(),
                    hmsTable.getTableName()).setPartitionNames(new ArrayList<>());

            Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
            if (!hmsTable.isUnPartitioned()) {
                long partitionId = 0;
                for (String partName : partitionNames) {
                    List<String> values = toPartitionValues(partName);
                    PartitionKey partitionKey = createPartitionKey(values, partitionColumns, table.getType());
                    partitionKeys.put(partitionKey, partitionId++);
                }
            } else {
                partitionKeys.put(new PartitionKey(), 0L);
            }

            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                PartitionKey key = entry.getKey();
                long partitionId = entry.getValue();
                List<LiteralExpr> literals = key.getKeys();
                for (int i = 0; i < literals.size(); i++) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(i);
                    LiteralExpr literal = literals.get(i);
                    if (Expr.IS_NULL_LITERAL.apply(literal)) {
                        columnToNullPartitions.get(columnRefOperator).add(partitionId);
                        continue;
                    }

                    Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                            .computeIfAbsent(literal, k -> Sets.newHashSet());
                    partitions.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);
            }
        }
        LOG.debug("Table: {}, partition values map: {}, null partition map: {}",
                table.getName(), columnToPartitionValuesMap, columnToNullPartitions);
    }

    private void classifyConjuncts(LogicalScanOperator operator,
                                   Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
            throws AnalysisException {
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
            if (!columnRefOperatorList.retainAll(columnToPartitionValuesMap.keySet())) {
                operator.getScanOperatorPredicates().getPartitionConjuncts().add(scalarOperator);
            } else {
                operator.getScanOperatorPredicates().getNonPartitionConjuncts().add(scalarOperator);
            }
        }
    }

    private void computePartitionInfo(LogicalScanOperator operator,
                                      Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                      Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        if (table instanceof HiveMetaStoreTable) {
            ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts());
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
            }
            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
        }
    }
}
