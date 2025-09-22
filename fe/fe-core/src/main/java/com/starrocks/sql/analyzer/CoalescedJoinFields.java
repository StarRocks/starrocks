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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles column resolution for JOIN USING clauses per SQL standard.
 * 
 * In JOIN USING, each USING column appears only once in the result (coalesced),
 * unlike JOIN ON where both L.col and R.col are visible. For unqualified 
 * references to USING columns, this class resolves to the appropriate table's
 * field based on JOIN type (RIGHT JOIN prefers right table values).
 * 
 * Examples:
 * - SELECT id FROM t1 JOIN t2 USING(id)     -> returns coalesced id column
 * - SELECT t1.id FROM t1 JOIN t2 USING(id)  -> returns original t1.id column
 */
public class CoalescedJoinFields extends RelationFields {
    private final Set<String> usingColumns;
    private final JoinOperator joinOperator;

    public CoalescedJoinFields(List<Field> fields, List<String> usingColNames, JoinOperator joinOperator) {
        super(fields);
        this.usingColumns = usingColNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        this.joinOperator = joinOperator;
    }

    @Override
    public List<Field> resolveFields(SlotRef name) {
        String columnName = name.getColumnName().toLowerCase();
        
        // For unqualified USING columns, return single coalesced field per SQL standard
        // This implements the "appears only once" semantics of JOIN USING
        if (name.getTblNameWithoutAnalyzed() == null && usingColumns.contains(columnName)) {
            List<Field> allMatches = super.resolveFields(name);
            if (!allMatches.isEmpty()) {
                Field selectedField = selectCoalescedField(allMatches);
                return ImmutableList.of(selectedField);  // Single field, not both L.col and R.col
            }
        }
        
        // For qualified references (table.column) or non-USING columns, return all matches
        // This allows access to original table columns when explicitly qualified
        return super.resolveFields(name);
    }

    /**
     * Selects which table's field to use for the coalesced USING column.
     * This implements COALESCE semantics: 
     * - RIGHT JOIN: COALESCE(L.col, R.col) -> prefer non-null R.col
     * - FULL OUTER JOIN: COALESCE(L.col, R.col) -> actual COALESCE function expression
     * - Other JOINs: COALESCE(L.col, R.col) -> prefer non-null L.col
     */
    private Field selectCoalescedField(List<Field> allMatches) {
        if (joinOperator != null && joinOperator.isRightJoin()) {
            // RIGHT JOIN: right table is primary, prefer its field for coalesced column
            return allMatches.get(allMatches.size() - 1);
        } else if (joinOperator != null && joinOperator.isFullOuterJoin()) {
            // FULL OUTER JOIN: create actual COALESCE(left.col, right.col) expression
            return createCoalescedField(allMatches);
        } else {
            // Other JOINs: left table is primary, prefer its field for coalesced column
            return allMatches.get(0);
        }
    }

    /**
     * Creates a field with COALESCE(left.col, right.col) expression for FULL OUTER JOIN USING.
     * This implements the SQL standard requirement that USING columns in FULL OUTER JOIN
     * should return non-null values from either table.
     */
    private Field createCoalescedField(List<Field> allMatches) {
        if (allMatches.size() < 2) {
            // Fallback to left table field if we don't have both left and right
            return allMatches.get(0);
        }

        Field leftField = allMatches.get(0);
        Field rightField = allMatches.get(allMatches.size() - 1);

        // Create SlotRef expressions for both fields
        SlotRef leftSlotRef = new SlotRef(leftField.getRelationAlias(), leftField.getName());
        SlotRef rightSlotRef = new SlotRef(rightField.getRelationAlias(), rightField.getName());

        // Create COALESCE(left.col, right.col) function call
        FunctionCallExpr coalesceExpr = new FunctionCallExpr(
                FunctionSet.COALESCE, Arrays.asList(leftField.getOriginExpression(), rightField.getOriginExpression()));

        // Determine the result type (should be compatible with both input types)
        Type resultType = Type.getAssignmentCompatibleType(leftField.getType(), rightField.getType(), true);
        if (resultType == null || resultType.isInvalid()) {
            // Fallback to left field type if types are incompatible
            resultType = leftField.getType();
        }

        // Create a new field with the COALESCE expression
        // Use the column name from the left field (USING column name)
        // Set relationAlias to null since this is a computed column spanning both tables
        return new Field(
            leftField.getName(),
            resultType,
            null, // No specific table alias for coalesced column
            coalesceExpr,
            true, // visible
            leftField.isNullable() && rightField.isNullable() // nullable only if both are nullable
        );
    }
}
