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

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

/**
 * Shared admission checks and helpers for the JDBC push-down rules (aggregate fold / join merge /
 * project push-down). These live in the optimizer layer and decide/assemble what each scan
 * contributes to the remote pushdown SQL.
 */
public class JDBCPushDownRuleUtils {
    private JDBCPushDownRuleUtils() {
    }

    /**
     * True when {@code projection} carries no expression to evaluate downstream: it is either
     * absent (null) or a pure column-pruning identity that maps every output ref to itself, so the
     * scan's projection can be folded into pushdown SQL rather than blocking the merge/push-down.
     */
    public static boolean isColumnPruningOnly(Projection projection) {
        return projection == null || projection.getColumnRefMap().entrySet().stream()
                .allMatch(e -> e.getKey().equals(e.getValue()));
    }

    /**
     * Build the {@link Column} a pushed-down scan exposes for {@code outputColumnRef}. When the
     * output is the scan column itself (identity reference), the original scan column is reused;
     * otherwise a fresh column is synthesized under {@code columnName} — the caller picks the name
     * (e.g. an aggregate alias vs. the ref's own name), since that choice is rule-specific.
     */
    public static Column createOutputColumn(ColumnRefOperator outputColumnRef, ScalarOperator outputExpr,
                                            LogicalJDBCScanOperator scan, String columnName) {
        Column scanColumn = scan.getColRefToColumnMetaMap().get(outputColumnRef);
        if (scanColumn != null && outputExpr.equals(outputColumnRef)) {
            return scanColumn;
        }
        return new Column(columnName, outputColumnRef.getType(), outputColumnRef.isNullable());
    }
}
