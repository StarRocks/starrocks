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
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles column resolution for JOIN USING clauses per SQL standard.
 * 
 * In JOIN USING, each USING column appears only once in the result (coalesced),
 * unlike JOIN ON where both L.col and R.col are visible. For unqualified 
 * references to USING columns, this class resolves to the appropriate table's
 * field based on JOIN type and implements proper COALESCE semantics.
 * 
 * JOIN Type Handling:
 * - INNER/LEFT JOIN: prefer left table's field for USING columns
 * - RIGHT JOIN: prefer right table's field for USING columns  
 * - FULL OUTER JOIN: create COALESCE(left.col, right.col) expressions for USING columns
 * 
 * FULL OUTER JOIN USING Support:
 * For FULL OUTER JOIN USING, this class creates actual COALESCE function expressions
 * to handle NULL values from either side properly, ensuring correct SQL semantics
 * where unqualified USING column references resolve to COALESCE(L.col, R.col).
 * 
 * Examples:
 * - SELECT id FROM t1 INNER JOIN t2 USING(id)      -> returns left table's id field
 * - SELECT id FROM t1 RIGHT JOIN t2 USING(id)      -> returns right table's id field
 * - SELECT id FROM t1 FULL OUTER JOIN t2 USING(id) -> returns COALESCE(t1.id, t2.id)
 * - SELECT t1.id FROM t1 JOIN t2 USING(id)         -> returns original t1.id column
 */
public class CoalescedJoinFields extends RelationFields {
    private final Set<String> usingColumns;
    private final JoinOperator joinOperator;
    private final Map<String, Field> coalescedFields; // For FULL OUTER JOIN USING

    public CoalescedJoinFields(List<Field> fields, List<String> usingColNames, JoinOperator joinOperator) {
        super(fields);
        this.usingColumns = usingColNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        this.joinOperator = joinOperator;
        this.coalescedFields = new HashMap<>();
    }

    public CoalescedJoinFields(List<Field> fields, List<String> usingColNames, JoinOperator joinOperator,
                               List<Field> leftFields, List<Field> rightFields) {
        super(buildFieldsWithCoalescedFields(fields, usingColNames, joinOperator, leftFields, rightFields));
        this.usingColumns = usingColNames.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        this.joinOperator = joinOperator;
        this.coalescedFields = extractCoalescedFields(usingColNames);
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
     * For FULL OUTER JOIN, returns the pre-built COALESCE field from allFields.
     * For other JOINs, implements preference semantics.
     */
    private Field selectCoalescedField(List<Field> allMatches) {
        if (joinOperator != null && joinOperator.isFullOuterJoin()) {
            // FULL OUTER JOIN: return pre-built COALESCE field from coalescedFields map
            String columnName = allMatches.get(0).getName().toLowerCase();
            Field coalescedField = coalescedFields.get(columnName);
            if (coalescedField != null) {
                return coalescedField;
            }
        }
        
        if (joinOperator != null && joinOperator.isRightJoin()) {
            // RIGHT JOIN: right table is primary, prefer its field for coalesced column
            return allMatches.get(allMatches.size() - 1);
        } else {
            // Other JOINs: left table is primary, prefer its field for coalesced column
            return allMatches.get(0);
        }
    }

    /**
     * For FULL OUTER JOIN USING, we add COALESCE fields to allFields to ensure proper indexing.
     * This allows indexOf to return real indices instead of mock values.
     */
    private static List<Field> buildFieldsWithCoalescedFields(List<Field> originalFields, List<String> usingColNames,
                                                              JoinOperator joinOperator, List<Field> leftFields,
                                                              List<Field> rightFields) {
        if (joinOperator == null || !joinOperator.isFullOuterJoin()) {
            // For non-FULL OUTER JOIN, keep original fields unchanged
            return originalFields;
        }
        
        // For FULL OUTER JOIN USING, we need to add COALESCE fields to allFields
        List<Field> result = new ArrayList<>(originalFields);
        Set<String> usingLower = usingColNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
        
        // Build COALESCE fields and add them to the result
        Map<String, Field> leftFieldMap = new HashMap<>();
        Map<String, Field> rightFieldMap = new HashMap<>();
        
        for (Field lf : leftFields) {
            if (lf.getName() != null) {
                leftFieldMap.put(lf.getName().toLowerCase(), lf);
            }
        }
        
        for (Field rf : rightFields) {
            if (rf.getName() != null) {
                rightFieldMap.put(rf.getName().toLowerCase(), rf);
            }
        }
        
        for (String usingCol : usingColNames) {
            String lowerCol = usingCol.toLowerCase();
            if (usingLower.contains(lowerCol)) {
                Field lf = leftFieldMap.get(lowerCol);
                Field rf = rightFieldMap.get(lowerCol);
                if (lf != null && rf != null) {
                    Field coalescedField = buildCoalescedField(lf, rf);
                    result.add(coalescedField);
                }
            }
        }
        
        return result;
    }

    /**
     * Extracts COALESCE fields from allFields for FULL OUTER JOIN USING.
     * Since COALESCE fields are now added to allFields, we can find them there.
     */
    private Map<String, Field> extractCoalescedFields(List<String> usingColNames) {
        Map<String, Field> result = new HashMap<>();
        List<Field> allFields = super.getAllFields();
        Set<String> usingLower = usingColNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
        
        for (Field field : allFields) {
            if (field.getName() != null && usingLower.contains(field.getName().toLowerCase()) &&
                    field.getOriginExpression() instanceof FunctionCallExpr funcExpr) {
                if (FunctionSet.COALESCE.equalsIgnoreCase(funcExpr.getFnName().getFunction())) {
                    result.put(field.getName().toLowerCase(), field);
                }
            }
        }
        
        return result;
    }

    private static Field buildCoalescedField(Field leftField, Field rightField) {
        TableName leftTable = leftField.getRelationAlias() != null ?
            new TableName(leftField.getRelationAlias().getCatalog(), 
                         leftField.getRelationAlias().getDb(), 
                         leftField.getRelationAlias().getTbl()) : null;
        SlotRef leftSlot = new SlotRef(leftTable, leftField.getName());
        
        TableName rightTable = rightField.getRelationAlias() != null ?
            new TableName(rightField.getRelationAlias().getCatalog(),
                         rightField.getRelationAlias().getDb(),
                         rightField.getRelationAlias().getTbl()) : null;
        SlotRef rightSlot = new SlotRef(rightTable, rightField.getName());
        
        List<Expr> args = Lists.newArrayList(leftSlot, rightSlot);
        FunctionCallExpr coalesceExpr = new FunctionCallExpr(FunctionSet.COALESCE, args);
        
        Type resultType = TypeManager.getCommonSuperType(leftField.getType(), rightField.getType());
        if (resultType == null) {
            resultType = leftField.getType();
        }
        
        return new Field(leftField.getName(), resultType, null, coalesceExpr, true, true);
    }
}

