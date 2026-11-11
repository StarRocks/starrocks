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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ExternalScanPredicates;
import com.starrocks.sql.optimizer.rewrite.FunctionPreimage;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.List;
import java.util.Map;

/**
 * Plain {@code column op constant} predicates implied by the function predicates of a Delta Lake scan
 * ({@code year(c) = 2024} implies {@code c >= 2024-01-01 and c < 2025-01-01}), see {@link FunctionPreimage}. The
 * switch, the disjunction policy and error handling are in {@link ExternalScanPredicates}; this class only maps
 * the columns of the kernel schema.
 * <p>
 * Each derived predicate is a superset of the conjunct it came from. They are used in two places, never in the
 * optimizer's predicate, so the selectivity of a conjunct is not counted twice:
 * <ul>
 *   <li>next to the predicate pushed to the Delta kernel, to skip files on partition values and file statistics;
 *   a local time naming no single instant on a TIMESTAMP column is widened there (see
 *   {@link com.starrocks.connector.LocalTimeBounds});</li>
 *   <li>in the BE scan conjuncts, to prune row groups, which the BE only does on a bare column; without
 *   disjunctions, see {@link FunctionPreimage#deriveRangePredicates}.</li>
 * </ul>
 */
public final class DeltaLakePreimagePredicates {
    private DeltaLakePreimagePredicates() {
    }

    /**
     * Predicates to push to the Delta kernel next to {@code conjuncts}, on top-level DATE, TIMESTAMP, TIMESTAMP_NTZ
     * and STRING columns of {@code schema}.
     */
    public static List<ScalarOperator> forFileSkipping(List<ScalarOperator> conjuncts, StructType schema) {
        if (schema == null) {
            return List.of();
        }
        return ExternalScanPredicates.forFileSkipping(ExternalScanPredicates.Connector.DELTA_LAKE, conjuncts,
                (operand, kind) -> operand instanceof ColumnRefOperator ref && accepts(schema, ref.getName(), kind));
    }

    /**
     * Predicates to add to the BE scan conjuncts, on the scan's DATE and DATETIME columns.
     */
    public static List<ScalarOperator> forScan(List<ScalarOperator> conjuncts,
                                               Map<ColumnRefOperator, Column> colRefToColumn) {
        return ExternalScanPredicates.forBeConjuncts(ExternalScanPredicates.Connector.DELTA_LAKE, conjuncts,
                ExternalScanPredicates.scanTemporalColumns(colRefToColumn));
    }

    private static boolean accepts(StructType schema, String name, FunctionPreimage.Kind kind) {
        if (schema.indexOf(name) < 0) {
            return false;
        }
        StructField field = schema.get(name);
        DataType type = field == null ? null : field.getDataType();
        if (kind == FunctionPreimage.Kind.TEMPORAL) {
            return type instanceof DateType || type instanceof TimestampType || type instanceof TimestampNTZType;
        }
        return type instanceof StringType;
    }
}
