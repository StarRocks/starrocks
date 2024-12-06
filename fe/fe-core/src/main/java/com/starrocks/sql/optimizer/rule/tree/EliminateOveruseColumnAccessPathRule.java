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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class EliminateOveruseColumnAccessPathRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        Visitor.processTopdown(root, ColumnRefSet.of());
        return root;
    }

    private static final class Visitor extends OptExpressionVisitor<Optional<ColumnRefSet>, ColumnRefSet> {
        private static final Visitor INSTANCE = new Visitor();

        @Override
        public Optional<ColumnRefSet> visit(OptExpression optExpression, ColumnRefSet context) {
            if (optExpression.getInputs().stream().noneMatch(input -> input.getOp() instanceof PhysicalScanOperator)) {
                return Optional.empty();
            }
            ColumnRefSet columnRefSet = ColumnRefSet.of();
            optExpression.getRowOutputInfo().getColumnRefMap().values()
                    .forEach(scalarOperator -> columnRefSet.union(scalarOperator.getColumnRefs()));
            return Optional.of(columnRefSet);
        }

        @Override
        public Optional<ColumnRefSet> visitPhysicalScan(OptExpression optExpression,
                                                        ColumnRefSet parentUsedColumnRefs) {
            Preconditions.checkState(parentUsedColumnRefs != null);
            PhysicalScanOperator scan = optExpression.getOp().cast();
            if (parentUsedColumnRefs.isEmpty() || scan.getColumnAccessPaths() == null ||
                    scan.getColumnAccessPaths().isEmpty()) {
                return Optional.empty();
            }

            Predicate<ColumnAccessPath> isSubfieldPrunedProjecting = accessPath ->
                    !accessPath.isFromPredicate() && !accessPath.onlyRoot();

            Map<Boolean, List<ColumnAccessPath>> accessPathGroups = scan.getColumnAccessPaths()
                    .stream()
                    .collect(Collectors.partitioningBy(isSubfieldPrunedProjecting));

            List<ColumnAccessPath> nonSubfieldPruningProjectings = accessPathGroups.get(false);
            List<ColumnAccessPath> subfieldPruningProjectings = accessPathGroups.get(true);

            if (subfieldPruningProjectings.isEmpty()) {
                return Optional.empty();
            }

            Map<String, ColumnRefOperator> columnNameToIdMap = scan.getColRefToColumnMetaMap().entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getValue().getName(), Map.Entry::getKey));

            Predicate<ColumnAccessPath> isOveruseProjecting = accessPath ->
                    parentUsedColumnRefs.contains(Objects.requireNonNull(columnNameToIdMap.get(accessPath.getPath())));

            Map<Boolean, List<ColumnAccessPath>> subfieldPruningProjectingGroups = subfieldPruningProjectings
                    .stream()
                    .collect(Collectors.partitioningBy(isOveruseProjecting));

            List<ColumnAccessPath> nonOveruseProjectings = subfieldPruningProjectingGroups.get(false);
            List<ColumnAccessPath> overuseProjectings = subfieldPruningProjectingGroups.get(true);
            if (overuseProjectings.isEmpty()) {
                return Optional.empty();
            }

            ImmutableList<ColumnAccessPath> retentionAccessPaths = ImmutableList.<ColumnAccessPath>builder()
                    .addAll(nonSubfieldPruningProjectings)
                    .addAll(nonOveruseProjectings)
                    .build();

            scan.setColumnAccessPaths(retentionAccessPaths);
            return Optional.empty();
        }

        public static void processTopdown(OptExpression optExpression, ColumnRefSet parentColumnRefSet) {
            ColumnRefSet columnRefSet =
                    optExpression.getOp().accept(INSTANCE, optExpression, parentColumnRefSet).orElse(null);
            optExpression.getInputs().forEach(input -> processTopdown(input, columnRefSet));
        }
    }
}
