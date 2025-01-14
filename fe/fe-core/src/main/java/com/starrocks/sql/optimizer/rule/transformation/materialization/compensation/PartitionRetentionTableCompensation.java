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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class PartitionRetentionTableCompensation extends TableCompensation {
    private ScalarOperator compensateOperator;

    public PartitionRetentionTableCompensation(Table refBaseTable, ScalarOperator compensationExpr) {
        super(refBaseTable, MVTransparentState.COMPENSATE);
        this.compensateOperator = compensationExpr;
    }

    public LogicalScanOperator compensate(OptimizerContext optimizerContext,
                                          MaterializedView mv,
                                          LogicalScanOperator scanOperator) {
        Preconditions.checkArgument(compensateOperator != null);
        final Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns == null || !refBaseTablePartitionColumns.containsKey(refBaseTable)) {
            return scanOperator;
        }
        Map<String, Column> columnMap = refBaseTable.getBaseSchema()
                .stream()
                .collect(Collectors.toMap(Column::getName, c -> c));
        List<ColumnRefOperator> origColumnRefs = compensateOperator.getColumnRefs();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        for (ColumnRefOperator origColRef : origColumnRefs) {
            Preconditions.checkArgument(columnMap.containsKey(origColRef.getName()));
            ColumnRefOperator newColRef = scanOperator.getColumnReference(columnMap.get(origColRef.getName()));
            columnRefMap.put(origColRef, newColRef);
        }
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnRefMap);
        ScalarOperator scalarOperator = rewriter.rewrite(compensateOperator);
        if (scalarOperator == null) {
            return scanOperator;
        }
        // build final predicate
        ScalarOperator compensate =  NegateFilterShuttle.getInstance().negateFilter(scalarOperator);
        compensate.setRedundant(true);
        ScalarOperator finalPredicate = Utils.compoundAnd(scanOperator.getPredicate(), compensate);
        // build new scan operator
        final LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
        builder.withOperator(scanOperator)
                .setPredicate(finalPredicate);
        return builder.build();
    }

    @Override
    public String toString() {
        ScalarOperator compensate =  NegateFilterShuttle.getInstance().negateFilter(compensateOperator);
        return String.format("%)", compensate.debugString());
    }

    public static TableCompensation build(Table refBaseTable,
                                          MvUpdateInfo mvUpdateInfo,
                                          Optional<LogicalScanOperator> scanOperatorOpt) {
        Preconditions.checkArgument(mvUpdateInfo.getQueryRewriteConsistencyMode() ==
                TableProperty.QueryRewriteConsistencyMode.FORCE_MV);
        MaterializedView mv = mvUpdateInfo.getMv();
        try {
            Optional<ScalarOperator> retentionConditionExpr = mv.getRetentionConditionScalarOp();
            if (retentionConditionExpr.isEmpty()) {
                return TableCompensation.unknown();
            }
            return new PartitionRetentionTableCompensation(refBaseTable, retentionConditionExpr.get());
        } catch (Exception e) {
            return TableCompensation.unknown();
        }
    }
}