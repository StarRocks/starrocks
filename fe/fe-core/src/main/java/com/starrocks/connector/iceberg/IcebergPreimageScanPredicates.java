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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.FunctionPreimage;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;

/**
 * Plain {@code column op constant} predicates implied by the function predicates of an Iceberg scan, for the BE to
 * prune row groups (Parquet statistics and page index) with. The BE only prunes on predicates over a bare column, so
 * {@code year(c) = 2024} prunes Iceberg files (through {@link FunctionPreimage}) but no row group inside them;
 * adding {@code c >= 2024-01-01 and c < 2025-01-01} next to it does.
 * <p>
 * Each derived predicate is a superset of the conjunct it came from, so adding it to the scan conjuncts keeps the
 * result. They are added to the BE conjuncts only, never to the optimizer's predicate, so the selectivity of a
 * conjunct is not counted twice. Only DATE and DATETIME columns are handled. The bounds are local times, which is
 * also how the BE reads a timestamptz column.
 */
public final class IcebergPreimageScanPredicates {
    private IcebergPreimageScanPredicates() {
    }

    /**
     * @param conjuncts the scan predicate's conjuncts
     * @param schema the Iceberg schema the scan reads
     * @param colRefToColumn the scan's column refs
     * @return derived predicates not already among {@code conjuncts}, without disjunctions (see
     *         {@link FunctionPreimage#deriveRangePredicates})
     */
    public static List<ScalarOperator> derive(List<ScalarOperator> conjuncts, Types.StructType schema,
                                              Map<ColumnRefOperator, Column> colRefToColumn) {
        if (!ScalarOperatorToIcebergExpr.isPreimageEnabled()) {
            return List.of();
        }
        FunctionPreimage.ColumnResolver iceberg = ScalarOperatorToIcebergExpr.preimageResolver(schema);
        FunctionPreimage.ColumnResolver resolver = (operand, kind) -> kind == FunctionPreimage.Kind.TEMPORAL
                && operand instanceof ColumnRefOperator ref && colRefToColumn.containsKey(ref)
                && iceberg.accepts(operand, kind);
        return FunctionPreimage.deriveRangePredicates(conjuncts, resolver);
    }
}
