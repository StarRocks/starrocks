// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.external.elasticsearch.EsShardPartitions;
import com.starrocks.external.elasticsearch.EsTablePartitions;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EsScanPartitionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(EsScanPartitionPruneRule.class);

    public EsScanPartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_ES_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalEsScanOperator operator = (LogicalEsScanOperator) input.getOp();
        EsTablePartitions esTablePartitions = operator.getEsTablePartitions();

        Collection<Long> partitionIds = null;
        try {
            partitionIds = partitionPrune(esTablePartitions.getPartitionInfo(), operator.getColumnFilters());
        } catch (AnalysisException e) {
            LOG.warn("Es Table partition prune failed. " + e);
        }

        ArrayList<String> unPartitionedIndices = Lists.newArrayList();
        ArrayList<String> partitionedIndices = Lists.newArrayList();
        for (EsShardPartitions esShardPartitions : esTablePartitions.getUnPartitionedIndexStates().values()) {
            operator.getSelectedIndex().add(esShardPartitions);
            unPartitionedIndices.add(esShardPartitions.getIndexName());
        }
        if (partitionIds != null) {
            for (Long partitionId : partitionIds) {
                EsShardPartitions indexState = esTablePartitions.getEsShardPartitions(partitionId);
                operator.getSelectedIndex().add(indexState);
                partitionedIndices.add(indexState.getIndexName());
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("partition prune finished, unpartitioned index [{}], "
                            + "partitioned index [{}]",
                    String.join(",", unPartitionedIndices),
                    String.join(",", partitionedIndices));
        }
        return Collections.emptyList();
    }

    /**
     * if the index name is an alias or index pattern, then the es table is related
     * with one or more indices some indices could be pruned by using partition info
     * in index settings currently only support range partition setting
     *
     * @param partitionInfo
     * @return
     * @throws AnalysisException
     */
    private Collection<Long> partitionPrune(PartitionInfo partitionInfo,
                                            Map<String, PartitionColumnFilter> columnFilters) throws AnalysisException {
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner = null;
        switch (partitionInfo.getType()) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
                partitionPruner = new RangePartitionPruner(keyRangeById, rangePartitionInfo.getPartitionColumns(),
                        columnFilters);
                return partitionPruner.prune();
            }
            default: {
                return null;
            }
        }
    }
}
