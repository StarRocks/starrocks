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

import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.TopNIndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Recognizes vector TopN shapes and attaches a planning request to the scan.
 *
 * <p>This rule deliberately keeps the original TopN, score expression, projection, and predicate.
 * Connector-specific candidate execution and score replacement are introduced by a later PR.
 */
public class ApplyTopNIndexRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(ApplyTopNIndexRule.class);

    public static final ApplyTopNIndexRule PAIMON_SCAN =
            new ApplyTopNIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    private final Function<LogicalScanOperator, ConnectorIndexMetadata> indexMetadataLoader;

    private ApplyTopNIndexRule(OperatorType scanType) {
        this(scanType, ApplyTopNIndexRule::loadIndexMetadata);
    }

    ApplyTopNIndexRule(OperatorType scanType,
                       Function<LogicalScanOperator, ConnectorIndexMetadata> indexMetadataLoader) {
        super(RuleType.TF_APPLY_CONNECTOR_INDEX,
                Pattern.create(OperatorType.LOGICAL_TOPN).addChildren(Pattern.create(scanType)));
        this.indexMetadataLoader = indexMetadataLoader;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator scan = (LogicalScanOperator) input.inputAt(0).getOp();
        return scan.getIndexCondition() == null && topN.hasLimit() && topN.getLimit() > 0
                && topN.getOrderByElements().size() == 1 && scan.getProjection() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = (LogicalTopNOperator) input.getOp();
        LogicalScanOperator scan = (LogicalScanOperator) input.inputAt(0).getOp();
        Ordering ordering = topN.getOrderByElements().get(0);
        ScalarOperator scoreExpression = scan.getProjection().getColumnRefMap().get(ordering.getColumnRef());
        if (scoreExpression == null) {
            return List.of();
        }

        ConnectorIndexMetadata indexMetadata;
        try {
            indexMetadata = indexMetadataLoader.apply(scan);
        } catch (RuntimeException e) {
            LOG.debug("Failed to load connector index metadata for {}", scan.getTable().getName(), e);
            return List.of();
        }

        IndexAnalyzer analyzer = new IndexAnalyzer(indexMetadata);
        if (!analyzer.supportsVectorTopN(scoreExpression, ordering.isAscending())) {
            return List.of();
        }

        ScalarOperator indexPredicate = analyzer.getIndexPredicate(scan.getPredicate());
        TopNIndexCondition condition = new TopNIndexCondition(indexPredicate, scoreExpression,
                topN.getLimit(), topN.getOffset(), ordering.isAscending());
        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scan);
        LogicalScanOperator newScan = (LogicalScanOperator) builder.withOperator(scan)
                .setIndexCondition(condition)
                .build();

        return List.of(OptExpression.create(topN, OptExpression.create(newScan, input.inputAt(0).getInputs())));
    }

    private static ConnectorIndexMetadata loadIndexMetadata(LogicalScanOperator scan) {
        Optional<ConnectorMetadata> connectorMetadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(scan.getTable().getCatalogName());
        return connectorMetadata.map(metadata -> metadata.getIndexMetadata(scan.getTable()))
                .orElseGet(ConnectorIndexMetadata::empty);
    }
}
