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

package com.starrocks.planner;

import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.Type;

/**
 * Builder for generating filter expressions from MIN/MAX runtime filter.
 *
 * For a given column and MIN/MAX values, generates expressions like:
 * - Range filter: col >= MIN AND col <= MAX
 * - Or exclusion filter: col < MIN OR col > MAX (for pruning)
 *
 * Example:
 * - MIN = '2024-01-01', MAX = '2024-01-31'
 * - Generated: created_at >= '2024-01-01' AND created_at <= '2024-01-31'
 * - Or: created_at < '2024-01-01' OR created_at > '2024-01-31' (for zone map pruning)
 */
public class MinMaxFilterExprBuilder {

    /**
     * Build a range filter expression: col >= min AND col <= max
     *
     * @param slotRef the column reference
     * @param minValue the minimum value (inclusive)
     * @param maxValue the maximum value (inclusive)
     * @return the filter expression, or null if values are invalid
     */
    public static Expr buildRangeFilter(SlotRef slotRef, String minValue, String maxValue) {
        if (minValue == null || maxValue == null) {
            return null;
        }

        Type colType = slotRef.getType();

        try {
            // Build: col >= min
            LiteralExpr minLiteral = LiteralExprFactory.create(minValue, colType);
            BinaryPredicate gePredicate = new BinaryPredicate(
                    BinaryType.GE, slotRef.clone(), minLiteral);

            // Build: col <= max
            LiteralExpr maxLiteral = LiteralExprFactory.create(maxValue, colType);
            BinaryPredicate lePredicate = new BinaryPredicate(
                    BinaryType.LE, slotRef.clone(), maxLiteral);

            // Build: col >= min AND col <= max
            return new CompoundPredicate(CompoundPredicate.Operator.AND, gePredicate, lePredicate);

        } catch (Exception e) {
            // Failed to create literal, return null
            return null;
        }
    }

    /**
     * Build an exclusion filter expression: col < min OR col > max
     * This is useful for zone map pruning - rows matching this can be skipped.
     *
     * @param slotRef the column reference
     * @param minValue the minimum value (inclusive)
     * @param maxValue the maximum value (inclusive)
     * @return the exclusion expression, or null if values are invalid
     */
    public static Expr buildExclusionFilter(SlotRef slotRef, String minValue, String maxValue) {
        if (minValue == null || maxValue == null) {
            return null;
        }

        Type colType = slotRef.getType();

        try {
            // Build: col < min
            LiteralExpr minLiteral = LiteralExprFactory.create(minValue, colType);
            BinaryPredicate ltPredicate = new BinaryPredicate(
                    BinaryType.LT, slotRef.clone(), minLiteral);

            // Build: col > max
            LiteralExpr maxLiteral = LiteralExprFactory.create(maxValue, colType);
            BinaryPredicate gtPredicate = new BinaryPredicate(
                    BinaryType.GT, slotRef.clone(), maxLiteral);

            // Build: col < min OR col > max
            return new CompoundPredicate(CompoundPredicate.Operator.OR, ltPredicate, gtPredicate);

        } catch (Exception e) {
            // Failed to create literal, return null
            return null;
        }
    }

    /**
     * Build a lower bound filter: col >= min
     * For MIN aggregation only.
     */
    public static Expr buildLowerBoundFilter(SlotRef slotRef, String minValue) {
        if (minValue == null) {
            return null;
        }

        try {
            LiteralExpr minLiteral = LiteralExprFactory.create(minValue, slotRef.getType());
            return new BinaryPredicate(BinaryType.GE, slotRef.clone(), minLiteral);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Build an upper bound filter: col <= max
     * For MAX aggregation only.
     */
    public static Expr buildUpperBoundFilter(SlotRef slotRef, String maxValue) {
        if (maxValue == null) {
            return null;
        }

        try {
            LiteralExpr maxLiteral = LiteralExprFactory.create(maxValue, slotRef.getType());
            return new BinaryPredicate(BinaryType.LE, slotRef.clone(), maxLiteral);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Convert the filter expression to SQL string for debugging/explain.
     */
    public static String toSql(Expr filterExpr) {
        if (filterExpr == null) {
            return "NULL";
        }
        return ExprToSql.toSql(filterExpr);
    }
}
