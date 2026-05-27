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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.lake.bookmark.BookmarkScopedTableResolver;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator.VersionRefType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

/**
 * Resolves a {@link LogicalVersionOperator} wrapping a {@link LogicalOlapScanOperator}
 * over a cloud-native table into a read-only scan over a bookmark-scoped
 * {@code OlapTable}. Used when the IVM JOIN rewrite splits a delta query into
 * "delta ⋈ static" — the static side gets wrapped in {@code LogicalVersionOperator}
 * with either {@code FROM_VERSION} (= base bookmark) or {@code TO_VERSION}
 * (= head bookmark).
 *
 * <p>Pattern: {@code LogicalVersionOperator -> LogicalOlapScanOperator}
 *
 * <p>Mirrors {@code IvmVersionIcebergScanRule}. Iceberg stamps a snapshot id on
 * the scan; the cloud-native rule additionally swaps the table to a scoped read-only {@code OlapTable}
 * built by {@link BookmarkScopedTableResolver#resolveById}, which strips partitions
 * added after the bookmark and pins per-partition versions.
 */
public class IvmVersionOlapScanRule extends TransformationRule {
    public IvmVersionOlapScanRule() {
        super(RuleType.TF_IVM_VERSION_OLAP_SCAN,
                Pattern.create(OperatorType.LOGICAL_VERSION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        return scan.getTable().isCloudNativeTableOrMaterializedView()
                && scan.getTvrTableDeltaTrait().isPresent();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalVersionOperator version = (LogicalVersionOperator) input.getOp();
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        OlapTable liveTable = (OlapTable) scan.getTable();
        TvrTableDelta tvrDelta = (TvrTableDelta) scan.getTvrVersionRange();

        long bookmarkId = (version.getVersionRefType() == VersionRefType.FROM_VERSION)
                ? tvrDelta.from().getVersion()
                : tvrDelta.to().getVersion();

        // resolveById throws SemanticException if the live table has drifted
        // from the bookmark (partition dropped, index replaced, tablet resharded).
        OlapTable scopedTable = BookmarkScopedTableResolver.resolveById(liveTable, bookmarkId);

        // Version state has been consumed (bookmark resolved into the scoped
        // table); reset to the empty sentinel — null would NPE in downstream
        // consumers like PiecesPlanTransformer that call .isEmpty() unchecked.
        LogicalScanOperator.Builder builder =
                (LogicalScanOperator.Builder) OperatorBuilderFactory.build(scan);
        LogicalOlapScanOperator newScan = (LogicalOlapScanOperator) builder
                .withOperator(scan)
                .setTable(scopedTable)
                .setTableVersionRange(TvrTableSnapshot.empty())
                .build();

        return List.of(OptExpression.create(newScan));
    }
}
