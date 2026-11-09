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

package com.starrocks.connector.hive;

import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.FunctionPreimage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plain {@code column op constant} predicates implied by the function predicates of a Hive scan
 * ({@code year(dt) = 2024} implies {@code dt >= 2024-01-01 and dt < 2025-01-01}), see {@link FunctionPreimage}.
 * <p>
 * They are classified with the scan's conjuncts: on a partition column they prune partitions, on other columns they
 * go to the BE, which prunes row groups on a bare column only. A disjunction (the ranges of an IN list) is kept only
 * on partition columns alone, see {@link FunctionPreimage#deriveRangePredicates}. Each is a superset of the conjunct
 * it came from, and none is added to the scan's predicate, so the selectivity of a conjunct is not counted twice.
 * Only DATE and DATETIME columns are handled; Hive timestamps are local times, as the constants are.
 */
public final class HivePreimagePredicates {
    private HivePreimagePredicates() {
    }

    public static boolean isEnabled() {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connectContext.getSessionVariable().isEnableHiveFunctionPredicatePruning();
    }

    /**
     * @param partitionColumns the scan's partition columns: a disjunction on them alone prunes partitions and is kept
     */
    public static List<ScalarOperator> derive(List<ScalarOperator> conjuncts,
                                              Map<ColumnRefOperator, Column> colRefToColumn,
                                              Set<ColumnRefOperator> partitionColumns) {
        if (conjuncts.isEmpty() || !isEnabled()) {
            return List.of();
        }
        List<ScalarOperator> result = new ArrayList<>();
        for (ScalarOperator predicate : FunctionPreimage.deriveColumnPredicates(conjuncts, (operand, kind) ->
                kind == FunctionPreimage.Kind.TEMPORAL
                        && operand instanceof ColumnRefOperator ref && colRefToColumn.containsKey(ref))) {
            if (!FunctionPreimage.isDisjunction(predicate)
                    || partitionColumns.containsAll(Utils.extractColumnRef(predicate))) {
                result.add(predicate);
            }
        }
        return result;
    }
}
