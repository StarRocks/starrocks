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

import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.IndexAnalyzer;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Function;

/** Annotates a Paimon scan using connector-declared index capabilities. */
public class ApplyPredicateIndexRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(ApplyPredicateIndexRule.class);

    public static final ApplyPredicateIndexRule PAIMON_SCAN =
            new ApplyPredicateIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    private final Function<LogicalPaimonScanOperator, ConnectorIndexMetadata> indexMetadataLoader;

    private ApplyPredicateIndexRule(OperatorType scanType) {
        this(scanType, ApplyPredicateIndexRule::loadIndexMetadata);
    }

    ApplyPredicateIndexRule(OperatorType scanType,
                            Function<LogicalPaimonScanOperator, ConnectorIndexMetadata> indexMetadataLoader) {
        super(RuleType.TF_APPLY_PREDICATE_CONNECTOR_INDEX, Pattern.create(scanType));
        this.indexMetadataLoader = indexMetadataLoader;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!(input.getOp() instanceof LogicalPaimonScanOperator)) {
            return false;
        }
        LogicalPaimonScanOperator scan = (LogicalPaimonScanOperator) input.getOp();
        return scan.getIndexCondition() == null
                && IndexAnalyzer.hasIndexablePredicateShape(scan);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (!check(input, context)) {
            return List.of();
        }
        LogicalPaimonScanOperator scan = (LogicalPaimonScanOperator) input.getOp();
        ConnectorIndexMetadata indexMetadata;
        try {
            indexMetadata = indexMetadataLoader.apply(scan);
        } catch (RuntimeException e) {
            LOG.warn("Failed to load connector index metadata for {}", scan.getTable().getName(), e);
            return List.of();
        }

        IndexCondition indexCondition = IndexAnalyzer.forScan(indexMetadata, scan)
                .getIndexCondition(scan.getPredicate());
        if (indexCondition == null) {
            return List.of();
        }

        LogicalPaimonScanOperator newScan = new LogicalPaimonScanOperator.Builder().withOperator(scan)
                .setIndexCondition(indexCondition)
                .build();
        return List.of(OptExpression.create(newScan, input.getInputs()));
    }

    private static ConnectorIndexMetadata loadIndexMetadata(LogicalPaimonScanOperator scan) {
        return GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getIndexMetadata(scan.getTable(), scan.getTvrVersionRange());
    }
}
