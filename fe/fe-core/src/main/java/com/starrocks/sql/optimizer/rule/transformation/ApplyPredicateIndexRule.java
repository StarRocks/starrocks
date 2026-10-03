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
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** Annotates a connector scan with its indexable scalar conjuncts. */
public class ApplyPredicateIndexRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(ApplyPredicateIndexRule.class);

    public static final ApplyPredicateIndexRule PAIMON_SCAN =
            new ApplyPredicateIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    private final Function<LogicalScanOperator, ConnectorIndexMetadata> indexMetadataLoader;

    private ApplyPredicateIndexRule(OperatorType scanType) {
        this(scanType, ApplyPredicateIndexRule::loadIndexMetadata);
    }

    ApplyPredicateIndexRule(OperatorType scanType,
                            Function<LogicalScanOperator, ConnectorIndexMetadata> indexMetadataLoader) {
        super(RuleType.TF_APPLY_CONNECTOR_INDEX, Pattern.create(scanType));
        this.indexMetadataLoader = indexMetadataLoader;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scan = (LogicalScanOperator) input.getOp();
        return scan.getIndexCondition() == null && scan.getPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scan = (LogicalScanOperator) input.getOp();
        ConnectorIndexMetadata indexMetadata;
        try {
            indexMetadata = indexMetadataLoader.apply(scan);
        } catch (RuntimeException e) {
            LOG.debug("Failed to load connector index metadata for {}", scan.getTable().getName(), e);
            return List.of();
        }

        IndexCondition indexCondition = new IndexAnalyzer(indexMetadata).getIndexCondition(scan.getPredicate());
        if (indexCondition == null) {
            return List.of();
        }

        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scan);
        LogicalScanOperator newScan = (LogicalScanOperator) builder.withOperator(scan)
                .setIndexCondition(indexCondition)
                .build();
        return List.of(OptExpression.create(newScan, input.getInputs()));
    }

    private static ConnectorIndexMetadata loadIndexMetadata(LogicalScanOperator scan) {
        Optional<ConnectorMetadata> connectorMetadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(scan.getTable().getCatalogName());
        return connectorMetadata.map(metadata -> metadata.getIndexMetadata(scan.getTable()))
                .orElseGet(ConnectorIndexMetadata::empty);
    }
}
