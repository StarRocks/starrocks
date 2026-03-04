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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.ScanOptimizeOption;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class RewriteSimpleAggToHDFSScanRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(RewriteSimpleAggToHDFSScanRule.class);

    private static final Set<OperatorType> SUPPORTED = Set.of(OperatorType.LOGICAL_HIVE_SCAN,
            OperatorType.LOGICAL_ICEBERG_SCAN,
            OperatorType.LOGICAL_FILE_SCAN
    );

    public static final RewriteSimpleAggToHDFSScanRule SCAN_NO_PROJECT =
            new RewriteSimpleAggToHDFSScanRule(false);

    public static final RewriteSimpleAggToHDFSScanRule SCAN_AND_PROJECT =
            new RewriteSimpleAggToHDFSScanRule();

    private final boolean hasProjectOperator;

    private RewriteSimpleAggToHDFSScanRule(boolean /* unused */ noProject) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(MultiOpPattern.of(SUPPORTED)));
        hasProjectOperator = false;
    }

    private RewriteSimpleAggToHDFSScanRule() {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT).addChildren(MultiOpPattern.of(SUPPORTED))));
        hasProjectOperator = true;
    }

    private OptExpression buildAggScanOperator(LogicalAggregationOperator aggregationOperator,
                                               LogicalScanOperator scanOperator,
                                               LogicalProjectOperator projectOperator,
                                               OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        // only need to handle count(*)
        Map<ColumnRefOperator, CallOperator> aggs = aggregationOperator.getAggregations();
        Preconditions.checkArgument(aggs.entrySet().size() == 1);
        ColumnRefOperator aggColumnRef = aggs.entrySet().iterator().next().getKey();
        CallOperator aggCall = aggs.entrySet().iterator().next().getValue();
        Preconditions.checkArgument(aggCall.getFnName().equals(FunctionSet.COUNT) && !aggCall.isDistinct());

        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        // select out partition columns.
        int tableRelationId = -1;
        for (ColumnRefOperator c : scanOperator.getColRefToColumnMetaMap().keySet()) {
            int relationId = columnRefFactory.getRelationId(c.getId());
            if (tableRelationId == -1) {
                tableRelationId = relationId;
            } else if (tableRelationId != relationId) {
                LOG.warn("Table relationIds are different in columns, tableRelationId = {}, relationId = {}",
                        tableRelationId, relationId);
                return null;
            }
            if (scanOperator.getPartitionColumns().contains(c.getName())) {
                newScanColumnRefs.put(c, scanOperator.getColRefToColumnMetaMap().get(c));
            }
        }

        if (tableRelationId == -1) {
            LOG.warn("Can not find table relation id in scan operator");
            return null;
        }

        ColumnRefOperator sumOutputColumnRef =
                columnRefFactory.create("sum_" + aggCall.getFnName(), aggCall.getType(), aggCall.isNullable());
        ColumnRefOperator placeholderColumn;
        {
            // generate a placeholder column for scan node.
            // ___count___ must be the column name for backend code.
            String metaColumnName = "___" + aggCall.getFnName() + "___";
            Column c = new Column(metaColumnName, Type.NULL);
            c.setIsAllowNull(true);
            placeholderColumn =
                    columnRefFactory.create(metaColumnName, aggCall.getType(), aggCall.isNullable());
            columnRefFactory.updateColumnToRelationIds(placeholderColumn.getId(), tableRelationId);
            columnRefFactory.updateColumnRefToColumns(placeholderColumn, c, scanOperator.getTable());
            newScanColumnRefs.put(placeholderColumn, c);

            CallOperator sumCall = new CallOperator(FunctionSet.SUM, Type.BIGINT,
                    Collections.singletonList(placeholderColumn),
                    Expr.getBuiltinFunction(FunctionSet.SUM, new Type[] {Type.BIGINT}, Function.CompareMode.IS_IDENTICAL));
            newAggCalls.put(sumOutputColumnRef, sumCall);
        }

        Map<Column, ColumnRefOperator> newScanColumnMeta = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, Column> c : newScanColumnRefs.entrySet()) {
            newScanColumnMeta.put(c.getValue(), c.getKey());
        }

        LogicalScanOperator newMetaScan = null;

        if (scanOperator instanceof LogicalHiveScanOperator) {
            newMetaScan = new LogicalHiveScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else if (scanOperator instanceof LogicalIcebergScanOperator) {
            newMetaScan = new LogicalIcebergScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate(),
                    scanOperator.getTableVersionRange());
        } else if (scanOperator instanceof LogicalFileScanOperator) {
            newMetaScan = new LogicalFileScanOperator(scanOperator.getTable(),
                    newScanColumnRefs, newScanColumnMeta, scanOperator.getLimit(), scanOperator.getPredicate());
        } else {
            LOG.warn("Unexpected scan operator: " + scanOperator);
            return null;
        }
        ScanOptimizeOption newOpt = scanOperator.getScanOptimizeOption().copy();
        newOpt.setCanUseCountOpt(true);
        newMetaScan.setScanOptimizeOption(newOpt);
        try {
            newMetaScan.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());
        } catch (AnalysisException e) {
            LOG.warn("Exception caught when set scan operator predicates", e);
            return null;
        }

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(aggregationOperator.getType(),
                aggregationOperator.getGroupingKeys(), newAggCalls);
        newAggOperator.setProjection(aggregationOperator.getProjection());

        // ifnull(sum(__count__)), 0) to avoid null result
        CallOperator ifNullCall = new CallOperator(FunctionSet.IFNULL, Type.BIGINT,
                Lists.newArrayList(sumOutputColumnRef, ConstantOperator.createBigint(0)),
                Expr.getBuiltinFunction(FunctionSet.IFNULL, new Type[] {Type.BIGINT, Type.BIGINT},
                        Function.CompareMode.IS_IDENTICAL));
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        newProjectMap.putAll(newAggOperator.getColumnRefMap());
        newProjectMap.remove(sumOutputColumnRef);
        newProjectMap.put(aggColumnRef, ifNullCall);
        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(newProjectMap);

        // For SCAN_AND_PROJECT: the AGG's grouping keys reference the original PROJECT's output
        // column refs (e.g. v1 = date_trunc('day', ds)).  The new SCAN only emits the underlying
        // partition column refs, so we insert a thin inner PROJECT that reconstructs the grouping
        // key expressions from the partition columns so that the AGG can still group by them.
        //   outer-project(ifnull) -> agg(sum(__count__)) -> inner-project(gk exprs) -> new-scan
        if (projectOperator != null) {
            Map<ColumnRefOperator, ScalarOperator> innerProjMap = Maps.newHashMap();
            // Carry over grouping-key mappings from the original PROJECT
            for (ColumnRefOperator gk : aggregationOperator.getGroupingKeys()) {
                ScalarOperator expr = projectOperator.getColumnRefMap().get(gk);
                if (expr != null) {
                    innerProjMap.put(gk, expr);
                }
            }
            // Pass through the ___count___ placeholder so the AGG's SUM can consume it
            innerProjMap.put(placeholderColumn, placeholderColumn);
            LogicalProjectOperator innerProjectOperator = new LogicalProjectOperator(innerProjMap);

            // outer-project -> agg -> inner-project -> scan
            OptExpression optExpression = OptExpression.create(newProjectOperator);
            OptExpression aggExpression = OptExpression.create(newAggOperator);
            OptExpression innerProjExpression = OptExpression.create(innerProjectOperator);
            optExpression.getInputs().add(aggExpression);
            aggExpression.getInputs().add(innerProjExpression);
            innerProjExpression.getInputs().add(OptExpression.create(newMetaScan));
            return optExpression;
        }

        // project(ifnull) -> agg(sum(__count__)) -> scan  (SCAN_NO_PROJECT path)
        OptExpression optExpression = OptExpression.create(newProjectOperator);
        OptExpression aggExpression = OptExpression.create(newAggOperator);
        optExpression.getInputs().add(aggExpression);
        aggExpression.getInputs().add(OptExpression.create(newMetaScan));
        return optExpression;
    }

    private LogicalScanOperator getScanOperator(final OptExpression input) {
        LogicalScanOperator scanOperator = null;
        if (hasProjectOperator) {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        } else {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getOp();
        }
        return scanOperator;
    }

    private LogicalProjectOperator getProjectOperator(final OptExpression input) {
        if (!hasProjectOperator) {
            return null;
        }
        return (LogicalProjectOperator) input.getInputs().get(0).getOp();
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan()) {
            LOG.info("RewriteSimpleAggToHDFSScanRule [{}]: skipped - session variable disabled",
                    hasProjectOperator ? "SCAN_AND_PROJECT" : "SCAN_NO_PROJECT");
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);

        LOG.debug("RewriteSimpleAggToHDFSScanRule [{}]: checking table={}, partitionColumns={}, scanPredicate={}",
                hasProjectOperator ? "SCAN_AND_PROJECT" : "SCAN_NO_PROJECT",
                scanOperator.getTable().getName(),
                scanOperator.getPartitionColumns(),
                scanOperator.getPredicate());

        // For Iceberg tables the optimization is only correct when every partition field in the
        // current spec uses an identity transform.
        //
        // Non-identity transforms (day(ts), bucket(n,col), truncate(n,col), etc.) store a
        // *derived* value in the partition metadata, not the raw column value.  Two problems arise:
        //
        //   1. GROUP BY correctness: getPartitionColumns() returns the *source* column (e.g. "ts"
        //      for day(ts)), so the grouping-key check lets a "GROUP BY ts" query through.  The
        //      synthetic task emits the epoch-day integer as the "ts" value, which is wrong.
        //
        //   2. Predicate correctness: a filter on the source column (e.g. WHERE ts >= '...') does
        //      not select whole partitions; record_count reflects all rows in the file, not the
        //      filtered subset.
        //
        // For identity-partitioned tables neither issue exists: the partition value equals the
        // column value, and a predicate on the partition column selects whole partitions.
        //
        // Files written under old, unpartitioned specs are handled by aggregateTasksByPartition(),
        // which returns null for such files, causing the optimisation to fall back gracefully.
        if (scanOperator instanceof LogicalIcebergScanOperator) {
            IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
            if (!icebergTable.isAllPartitionColumnsAlwaysIdentity()) {
                LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - table={} has non-identity partition " +
                        "transforms, partition values would not match source column values",
                        icebergTable.getName());
                return false;
            }
        }

        // scan must not have a pushed-down limit (DEFAULT_LIMIT = -1 means no limit)
        if (scanOperator.getLimit() != -1) {
            LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - scan has limit={}", scanOperator.getLimit());
            return false;
        }

        // no materialized column in predicate of scan
        if (hasMaterializedColumnInPredicate(scanOperator, scanOperator.getPredicate())) {
            LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - materialized column in scan predicate={}",
                    scanOperator.getPredicate());
            return false;
        }

        // all group by keys are partition keys.
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        LogicalProjectOperator projectOperator = getProjectOperator(input);
        LOG.debug("RewriteSimpleAggToHDFSScanRule: groupingKeys={}, hasProjectOperator={}",
                groupingKeys, hasProjectOperator);
        if (projectOperator != null) {
            // SCAN_AND_PROJECT: grouping keys reference PROJECT output column refs.
            // Resolve each grouping key through the PROJECT and verify every referenced
            // column used in the expression is a partition column of the scan.
            Map<ColumnRefOperator, ScalarOperator> projMap = projectOperator.getColumnRefMap();
            LOG.debug("RewriteSimpleAggToHDFSScanRule: projMap keys={}", projMap.keySet());
            for (ColumnRefOperator gk : groupingKeys) {
                ScalarOperator projExpr = projMap.get(gk);
                if (projExpr == null) {
                    LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - groupingKey={} not found in projMap={}",
                            gk, projMap.keySet());
                    return false;
                }
                LOG.debug("RewriteSimpleAggToHDFSScanRule: gk={} -> projExpr={} (columnRefs={})",
                        gk, projExpr, projExpr.getColumnRefs());
                // Every column ref in the projection expression must be a partition column
                for (ColumnRefOperator ref : projExpr.getColumnRefs()) {
                    if (!scanOperator.getPartitionColumns().contains(ref.getName())) {
                        LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - ref={} not in partitionColumns={}",
                                ref.getName(), scanOperator.getPartitionColumns());
                        return false;
                    }
                }
            }
        } else {
            // SCAN_NO_PROJECT: grouping keys must directly be partition column refs
            List<String> gkNames = groupingKeys.stream().map(x -> x.getName()).collect(Collectors.toList());
            if (!scanOperator.getPartitionColumns().containsAll(gkNames)) {
                LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - groupingKey names={} not all in partitionColumns={}",
                        gkNames, scanOperator.getPartitionColumns());
                return false;
            }
        }

        // no materialized column in predicate of aggregation
        if (hasMaterializedColumnInPredicate(scanOperator, aggregationOperator.getPredicate())) {
            LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - materialized column in agg predicate={}",
                    aggregationOperator.getPredicate());
            return false;
        }

        // not applicable if there is no aggregation functions, like `distinct x`.
        if (aggregationOperator.getAggregations().isEmpty()) {
            LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - no aggregation functions");
            return false;
        }

        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();
                    ColumnRefSet usedColumns = aggregator.getUsedColumns();
                    LOG.debug("RewriteSimpleAggToHDFSScanRule: agg fn={}, distinct={}, usedColumns={}, args={}",
                            functionName, aggregator.isDistinct(), usedColumns, aggregator.getArguments());

                    if (functionName.equals(FunctionSet.COUNT) && !aggregator.isDistinct() && usedColumns.isEmpty()) {
                        List<ScalarOperator> arguments = aggregator.getArguments();
                        if (arguments.isEmpty()) {
                            // count()/count(*)
                            return true;
                        } else if (arguments.size() == 1 && !arguments.get(0).isConstantNull()) {
                            // count(non-null constant)
                            return true;
                        }
                        LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - COUNT with unexpected args={}", arguments);
                        return false;
                    }
                    LOG.info("RewriteSimpleAggToHDFSScanRule: skipped - unsupported agg fn={} distinct={} usedColumns={}",
                            functionName, aggregator.isDistinct(), usedColumns);
                    return false;
                }
        );
        LOG.debug("RewriteSimpleAggToHDFSScanRule: check result={}", allValid);
        return allValid;
    }

    private static boolean hasMaterializedColumnInPredicate(LogicalScanOperator scanOperator, ScalarOperator predicate) {
        if (predicate == null) {
            return false;
        }
        List<ColumnRefOperator> columnRefOperators = predicate.getColumnRefs();
        Set<String> partitionColumns = scanOperator.getPartitionColumns();
        for (ColumnRefOperator c : columnRefOperators) {
            if (!partitionColumns.contains(c.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Try to compute COUNT(*) directly from Iceberg manifest metadata.
     * This is O(manifests) with no file I/O — much faster than the full planFiles() pipeline.
     * For no-predicate queries, uses snapshot summary (zero I/O) when available.
     *
     * Returns the total row count if all conditions are met, empty otherwise.
     * Conditions: Iceberg table, snapshot exists, no delete manifests, row counts available
     * in all data manifests, no deleted rows in any manifest.
     */
    private OptionalLong tryManifestCountShortCircuit(LogicalScanOperator scanOperator) {
        IcebergTable icebergTable = (IcebergTable) scanOperator.getTable();
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        Snapshot snapshot = nativeTable.currentSnapshot();

        if (snapshot == null) {
            LOG.info("Manifest count short-circuit: empty table (no snapshot), returning 0");
            return OptionalLong.of(0L);
        }

        // Fast path: for no-predicate queries, read total-records from snapshot summary (zero I/O)
        if (scanOperator.getPredicate() == null) {
            Map<String, String> summary = snapshot.summary();
            String totalDeleteFiles = summary.get("total-delete-files");
            if ("0".equals(totalDeleteFiles) || totalDeleteFiles == null) {
                String totalRecords = summary.get("total-records");
                if (totalRecords != null) {
                    try {
                        long count = Long.parseLong(totalRecords);
                        LOG.info("Manifest count short-circuit: count={} from snapshot summary", count);
                        return OptionalLong.of(count);
                    } catch (NumberFormatException e) {
                        // fall through to manifest-based counting
                    }
                }
            } else {
                LOG.info("Manifest count short-circuit: skipped - snapshot has delete files");
                return OptionalLong.empty();
            }
        }

        org.apache.iceberg.io.FileIO io = nativeTable.io();

        List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);
        if (!deleteManifests.isEmpty()) {
            LOG.info("Manifest count short-circuit: skipped - {} delete manifests", deleteManifests.size());
            return OptionalLong.empty();
        }

        List<ManifestFile> dataManifests = snapshot.dataManifests(io);

        if (scanOperator.getPredicate() != null) {
            ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();
            ScalarOperatorToIcebergExpr.IcebergContext icebergContext =
                    new ScalarOperatorToIcebergExpr.IcebergContext(nativeTable.schema().asStruct());
            Expression filterExpr = converter.convert(
                    Lists.newArrayList(scanOperator.getPredicate()), icebergContext);

            if (filterExpr == Expressions.alwaysTrue()) {
                LOG.info("Manifest count short-circuit: skipped - predicate could not be converted");
                return OptionalLong.empty();
            }

            int beforeCount = dataManifests.size();
            dataManifests = IcebergApiConverter.filterManifests(dataManifests, nativeTable, filterExpr);

            if (dataManifests.isEmpty() && beforeCount > 0) {
                LOG.info("Manifest count short-circuit: skipped - filter removed all {} manifests", beforeCount);
                return OptionalLong.empty();
            }
        }

        long totalCount = 0;
        for (ManifestFile manifest : dataManifests) {
            Long addedRows = manifest.addedRowsCount();
            Long existingRows = manifest.existingRowsCount();

            if (addedRows == null || existingRows == null) {
                LOG.info("Manifest count short-circuit: skipped - manifest missing row counts");
                return OptionalLong.empty();
            }

            Long deletedRows = manifest.deletedRowsCount();
            if (deletedRows != null && deletedRows > 0) {
                LOG.info("Manifest count short-circuit: skipped - manifest has {} deleted rows", deletedRows);
                return OptionalLong.empty();
            }

            totalCount += addedRows + existingRows;
        }

        LOG.info("Manifest count short-circuit: count={} from {} manifests", totalCount, dataManifests.size());
        return OptionalLong.of(totalCount);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);
        LogicalProjectOperator projectOperator = getProjectOperator(input);
        LOG.debug("RewriteSimpleAggToHDFSScanRule: transform() scanOperator.limit={} scanOperator.predicate={}",
                scanOperator.getLimit(), scanOperator.getPredicate());

        // Try manifest count short-circuit (Iceberg only, no GROUP BY)
        if (scanOperator instanceof LogicalIcebergScanOperator
                && aggregationOperator.getGroupingKeys().isEmpty()) {
            OptionalLong manifestCount = tryManifestCountShortCircuit(scanOperator);
            if (manifestCount.isPresent()) {
                ColumnRefOperator aggColumnRef =
                        aggregationOperator.getAggregations().entrySet().iterator().next().getKey();
                List<ScalarOperator> row = Lists.newArrayList(
                        ConstantOperator.createBigint(manifestCount.getAsLong()));
                List<List<ScalarOperator>> rows = Lists.newArrayList();
                rows.add(row);
                LogicalValuesOperator valuesOp = new LogicalValuesOperator(
                        Lists.newArrayList(aggColumnRef), rows);
                return Lists.newArrayList(OptExpression.create(valuesOp));
            }
        }

        OptExpression result = buildAggScanOperator(aggregationOperator, scanOperator, projectOperator, context);
        if (result == null) {
            // Fail to rewrite
            return Lists.newArrayList(input);
        }
        return Lists.newArrayList(result);
    }
}
