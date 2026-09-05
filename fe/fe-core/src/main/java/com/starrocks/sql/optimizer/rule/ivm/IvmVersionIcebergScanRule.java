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

package com.starrocks.sql.optimizer.rule.ivm;

import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator.VersionRefType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Optional;

/**
 * Resolves a {@link LogicalVersionOperator} wrapping a {@link LogicalIcebergScanOperator} by binding
 * the scan to a specific Iceberg snapshot.
 *
 * <p>Pattern: {@code LogicalVersionOperator -> LogicalIcebergScanOperator}
 *
 * <p>Based on {@link VersionRefType}:
 * <ul>
 *   <li>{@code FROM_VERSION} — binds to the from-snapshot (start of the delta range)</li>
 *   <li>{@code TO_VERSION} — binds to the to-snapshot (end of the delta range)</li>
 * </ul>
 */
public class IvmVersionIcebergScanRule extends TransformationRule {
    public IvmVersionIcebergScanRule() {
        super(RuleType.TF_IVM_VERSION_ICEBERG_SCAN,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalIcebergScanOperator scan = (LogicalIcebergScanOperator) input.inputAt(0).getOp();
        Optional<TvrTableDeltaTrait> trait = scan.getTvrTableDeltaTrait();
        return trait.isPresent();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        LogicalIcebergScanOperator scan = (LogicalIcebergScanOperator) input.inputAt(0).getOp();

        TvrTableDeltaTrait trait = scan.getTvrTableDeltaTrait().get();
        TvrTableDelta tvrDelta = trait.getTvrDelta();

        // Resolve the snapshot based on version ref type
        TvrTableSnapshot resolvedSnapshot;
        if (version.getVersionRefType() == VersionRefType.FROM_VERSION) {
            resolvedSnapshot = tvrDelta.fromSnapshot();
        } else {
            resolvedSnapshot = tvrDelta.toSnapshot();
        }

        // Build a new scan bound to the resolved snapshot
        LogicalScanOperator.Builder builder =
                (LogicalScanOperator.Builder) OperatorBuilderFactory.build(scan);
        LogicalIcebergScanOperator newScan = (LogicalIcebergScanOperator) builder
                .withOperator(scan)
                .setTableVersionRange(resolvedSnapshot)
                .build();

        return List.of(OptExpression.create(newScan));
    }
}
