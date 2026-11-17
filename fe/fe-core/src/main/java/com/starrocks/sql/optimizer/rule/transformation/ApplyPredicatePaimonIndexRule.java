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

import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.paimon.index.PaimonIndexAnalyzer;
import com.starrocks.connector.paimon.index.PaimonIndexCondition;
import com.starrocks.connector.paimon.index.PaimonIndexMetadata;
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

/** Annotates a Paimon scan using the index capabilities of the table version. */
public class ApplyPredicatePaimonIndexRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(ApplyPredicatePaimonIndexRule.class);

    public static final ApplyPredicatePaimonIndexRule PAIMON_SCAN =
            new ApplyPredicatePaimonIndexRule(OperatorType.LOGICAL_PAIMON_SCAN);

    private final Function<LogicalPaimonScanOperator, PaimonIndexMetadata> indexMetadataLoader;

    private ApplyPredicatePaimonIndexRule(OperatorType scanType) {
        this(scanType, ApplyPredicatePaimonIndexRule::loadIndexMetadata);
    }

    ApplyPredicatePaimonIndexRule(OperatorType scanType,
                                 Function<LogicalPaimonScanOperator, PaimonIndexMetadata> indexMetadataLoader) {
        super(RuleType.TF_APPLY_PREDICATE_PAIMON_INDEX, Pattern.create(scanType));
        this.indexMetadataLoader = indexMetadataLoader;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!(input.getOp() instanceof LogicalPaimonScanOperator)) {
            return false;
        }
        LogicalPaimonScanOperator scan = (LogicalPaimonScanOperator) input.getOp();
        return scan.getIndexCondition() == null
                && PaimonIndexAnalyzer.hasIndexablePredicateShape(scan);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (!check(input, context)) {
            return List.of();
        }
        LogicalPaimonScanOperator scan = (LogicalPaimonScanOperator) input.getOp();
        PaimonIndexMetadata indexMetadata;
        try {
            indexMetadata = indexMetadataLoader.apply(scan);
        } catch (RuntimeException e) {
            LOG.warn("Failed to load Paimon index metadata for {}", scan.getTable().getName(), e);
            return List.of();
        }

        PaimonIndexCondition indexCondition = PaimonIndexAnalyzer.forScan(indexMetadata, scan)
                .getIndexCondition(scan.getPredicate());
        if (indexCondition == null) {
            return List.of();
        }

        LogicalPaimonScanOperator newScan = new LogicalPaimonScanOperator.Builder().withOperator(scan)
                .setIndexCondition(indexCondition)
                .build();
        return List.of(OptExpression.create(newScan, input.getInputs()));
    }

    private static PaimonIndexMetadata loadIndexMetadata(LogicalPaimonScanOperator scan) {
        return PaimonMetadata.resolve(scan.getTable())
                .map(metadata -> metadata.getIndexMetadata(scan.getTable(), scan.getTvrVersionRange()))
                .orElseGet(PaimonIndexMetadata::empty);
    }
}
