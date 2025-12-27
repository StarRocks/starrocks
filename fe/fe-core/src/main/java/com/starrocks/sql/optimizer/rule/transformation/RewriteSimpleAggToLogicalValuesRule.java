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

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class RewriteSimpleAggToLogicalValuesRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(RewriteSimpleAggToLogicalValuesRule.class);


    public static final RewriteSimpleAggToLogicalValuesRule PAIMON_SCAN_NO_PROJECT =
            new RewriteSimpleAggToLogicalValuesRule(OperatorType.LOGICAL_PAIMON_SCAN, false);

    public static final RewriteSimpleAggToLogicalValuesRule PAIMON_SCAN =
            new RewriteSimpleAggToLogicalValuesRule(OperatorType.LOGICAL_PAIMON_SCAN);

    final OperatorType scanOperatorType;
    final boolean hasProjectOperator;

    private RewriteSimpleAggToLogicalValuesRule(OperatorType logicalOperatorType, boolean /* unused */ noProject) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(logicalOperatorType)));
        hasProjectOperator = false;
        scanOperatorType = logicalOperatorType;
    }

    private RewriteSimpleAggToLogicalValuesRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_REWRITE_SIMPLE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, logicalOperatorType)));
        hasProjectOperator = true;
        scanOperatorType = logicalOperatorType;
    }

    private LogicalScanOperator getScanOperator(final OptExpression input) {
        LogicalScanOperator scanOperator;
        if (hasProjectOperator) {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        } else {
            scanOperator = (LogicalScanOperator) input.getInputs().get(0).getOp();
        }
        return scanOperator;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteSimpleAggToPaimonMetaScan()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);

        // no limit
        if (scanOperator.getLimit() != -1) {
            return false;
        }

        // filter only involved with partition keys.
        if (scanOperator.getPredicate() != null) {
            if (!new HashSet<>(scanOperator.getTable().getPartitionColumnNames())
                    .containsAll(scanOperator.getPredicate().getColumnRefs().stream().map(ColumnRefOperator::getName).toList())) {
                return false;
            }
        }
        // no group by keys on agg operator
        List<ColumnRefOperator> groupingKeys = aggregationOperator.getGroupingKeys();
        if (groupingKeys != null && !groupingKeys.isEmpty()) {
            return false;
        }

        // no predicate on agg operator
        if (aggregationOperator.getPredicate() != null) {
            return false;
        }

        // not applicable if there is no aggregation functions, like `distinct x`.
        if (aggregationOperator.getAggregations().isEmpty()) {
            return false;
        }

        return aggregationOperator.getAggregations().values().stream().allMatch(
                aggregator -> {
                    AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
                    String functionName = aggregateFunction.functionName();
                    ColumnRefSet usedColumns = aggregator.getUsedColumns();

                    if (functionName.equals(FunctionSet.COUNT) && !aggregator.isDistinct() && usedColumns.isEmpty()) {
                        List<ScalarOperator> arguments = aggregator.getArguments();
                        // count(non-null constant)
                        if (arguments.isEmpty()) {
                            // count()/count(*)
                            return true;
                        } else {
                            return arguments.size() == 1 && !arguments.get(0).isConstantNull();
                        }
                    }
                    return false;
                }
        );

    }

    private long calculateCount(LogicalScanOperator scanOperator) {
        long count = 0;
        Table table = scanOperator.getTable();
        List<String> fieldNames = scanOperator.getColRefToColumnMetaMap().keySet().stream()
                .map(ColumnRefOperator::getName)
                .toList();
        TvrVersionRange tvrVersionRange = scanOperator.getTvrVersionRange();
        if (tvrVersionRange == null) {
            tvrVersionRange = TvrTableSnapshot.empty();
        }
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPredicate(scanOperator.getPredicate())
                .setFieldNames(fieldNames)
                .setTableVersionRange(tvrVersionRange)
                .setLimit(-1)
                .build();
        List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(
                table, params);

        if (fileInfos.isEmpty()) {
            return -1;
        }

        if (scanOperatorType == OperatorType.LOGICAL_PAIMON_SCAN) {
            // Process all RemoteFileInfo entries to handle multiple files
            boolean hasValidFileInfo = false;
            boolean hasValidSplit = false;
            for (RemoteFileInfo fileInfo : fileInfos) {
                if (fileInfo.getFiles() == null || fileInfo.getFiles().isEmpty()) {
                    continue;
                }
                hasValidFileInfo = true;
                for (RemoteFileDesc file : fileInfo.getFiles()) {
                    if (!(file instanceof PaimonRemoteFileDesc remoteFileDesc)) {
                        LOG.warn("Unexpected file type in PaimonRemoteFileDesc: {}", file.getClass().getName());
                        return -1;
                    }
                    List<Split> splits = remoteFileDesc.getPaimonSplitsInfo().getPaimonSplits();
                    if (splits.isEmpty()) {
                        continue;
                    }
                    for (Split split : splits) {
                        if (!(split instanceof DataSplit dataSplit)) {
                            LOG.warn("Non-DataSplit found in Paimon splits, cannot calculate count");
                            return -1;
                        }
                        if (!dataSplit.mergedRowCountAvailable()) {
                            LOG.debug("DataSplit mergedRowCount not available, cannot calculate count");
                            return -1;
                        }
                        hasValidSplit = true;
                        count += dataSplit.mergedRowCount();
                    }
                }
            }
            // If no valid fileInfo was processed or no valid split was found, return -1 to indicate count cannot be calculated
            if (!hasValidFileInfo || !hasValidSplit) {
                return -1;
            }
            return count;
        } else {
            throw new UnsupportedOperationException("RewriteSimpleAggToLogicalValuesRule only supports Paimon Table");
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        LogicalScanOperator scanOperator = getScanOperator(input);
        long count = calculateCount(scanOperator);

        if (count == -1) {
            // Unable to calculate count through metadata, continue with scan operator
            LOG.debug("Unable to calculate count through metadata for table {}, continue with scan operator",
                    scanOperator.getTable().getName());
            return Lists.newArrayList(input);
        }

        LOG.debug("Rewrite simple agg to logical values for table {}, calculated count: {}",
                scanOperator.getTable().getName(), count);

        List<ColumnRefOperator> columns = new ArrayList<>(aggregationOperator.getAggregations().keySet());
        // Create a value for each aggregation column, all with the same count value
        List<ScalarOperator> valueRow = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            valueRow.add(ConstantOperator.createBigint(count));
        }
        LogicalValuesOperator logicalValuesOperator = new LogicalValuesOperator.Builder()
                .setRows(List.of(valueRow))
                .setColumnRefSet(columns)
                .build();
        return Lists.newArrayList(OptExpression.create(logicalValuesOperator));
    }
}
