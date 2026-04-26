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
// limitations under the License

package com.starrocks.sql.optimizer.rule.tvr;

import com.google.common.base.Preconditions;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.sql.analyzer.mv.IVMAnalyzer;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrLazyOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptExpression;
import com.starrocks.sql.optimizer.rule.tvr.common.TvrOptMeta;
import org.apache.hadoop.util.Lists;

import java.util.List;

public class TvrTableScanRule extends TvrTransformationRule {

    public TvrTableScanRule() {
        super(RuleType.TF_TVR_TABLE_SCAN, Pattern.create(OperatorType.PATTERN_SCAN));
    }

    private boolean isSupportedTvr(LogicalScanOperator scanOperator) {
        // check whether this table is supported for ivm.
        if (!IVMAnalyzer.isTableTypeIVMSupported(scanOperator.getTable().getType())) {
            return false;
        }
        return scanOperator.getTvrTableDeltaTrait().isPresent()
                && scanOperator.getTvrTableDeltaTrait().get().isAppendOnly();
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        if (scanOperators.stream().anyMatch(scan -> !isSupportedTvr(scan))) {
            return false;
        }
        return input.getTvrMeta() == null;
    }

    private LogicalScanOperator withTvrVersionRange(LogicalScanOperator scanOperator,
                                                    TvrTableSnapshot tvrVersionRange) {
        Operator.Builder builder = OperatorBuilderFactory.build(scanOperator);
        LogicalScanOperator.Builder scanBuilder = (LogicalScanOperator.Builder) builder;
        return scanBuilder
                .withOperator(scanOperator)
                .setTableVersionRange(tvrVersionRange)
                .build();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        Preconditions.checkState(scanOperator.getTvrTableDeltaTrait().isPresent(),
                "TvrDeltaTrait should be present for scan operator: %s", scanOperator);

        TvrTableDeltaTrait tvrDeltaTrait = scanOperator.getTvrTableDeltaTrait().get();
        TvrTableDelta tvrTableDelta = tvrDeltaTrait.getTvrDelta();
        TvrTableSnapshot fromSnapshot = tvrTableDelta.fromSnapshot();
        TvrTableSnapshot toSnapshot = tvrTableDelta.toSnapshot();

        if (!IVMAnalyzer.isTableTypeIVMSupported(scanOperator.getTable().getType())) {
            throw new IllegalStateException(
                    "Unsupported table type for TVR table scan: " + scanOperator.getTable().getType());
        }
        // from snapshot
        LogicalScanOperator fromOperator = withTvrVersionRange(scanOperator, fromSnapshot);
        OptExpression fromOpt = OptExpression.create(fromOperator);

        // to snapshot
        LogicalScanOperator toOperator = withTvrVersionRange(scanOperator, toSnapshot);
        OptExpression toOpt = OptExpression.create(toOperator);

        // create TvrOptExpression for both from and to snapshots
        TvrOptMeta tvrOptMeta = new TvrOptMeta(
                tvrDeltaTrait,
                TvrLazyOptExpression.of(() -> new TvrOptExpression(fromSnapshot, fromOpt)),
                TvrLazyOptExpression.of(() -> new TvrOptExpression(toSnapshot, toOpt))
        );
        OptExpression newOptExpression = OptExpression.create(scanOperator, tvrOptMeta);
        return Lists.newArrayList(newOptExpression);
    }
}
