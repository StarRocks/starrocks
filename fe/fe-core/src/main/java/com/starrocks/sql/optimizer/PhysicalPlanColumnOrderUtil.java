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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to fix the output column order of PhysicalValuesOperator.
 * The order must strictly follow the expected output column list.
 *
 * We match columns by ColumnRefOperator.id (unique identifier),
 * ignoring alias names, since aliases are only for SQL syntax and
 * may not exist in the optimized plan.
 */
public class PhysicalPlanColumnOrderUtil {
    private static final Logger LOG = LogManager.getLogger(PhysicalPlanColumnOrderUtil.class);

    // column reference operator
    private final List<ColumnRefOperator> expectedOutputColumns;

    public PhysicalPlanColumnOrderUtil(List<ColumnRefOperator> expectedOutputColumns) {
        this.expectedOutputColumns = expectedOutputColumns;
    }

    /**
     * Fix the column order issue in the physics plan
     * @param optExpr
     * @param context
     */
    public void fixPhysicalPlan(OptExpression optExpr, ExecPlan context) {
        // There are no fields to output
        if (expectedOutputColumns.isEmpty()) {
            return;
        }
        visit(optExpr, context);
    }

    /**
     * Visit OptExpression and fix the column order
     * @param optExpr
     * @param context
     */
    private void visit(OptExpression optExpr, ExecPlan context) {
        // Visit the child nodes first
        for (OptExpression input : optExpr.getInputs()) {
            visit(input, context);
        }

        // Fix the current node
        Operator op = optExpr.getOp();
        if (op instanceof PhysicalValuesOperator) {
            PhysicalValuesOperator operator = (PhysicalValuesOperator) op;
            // There is no result to be output
            if (operator.getRows().isEmpty()) {
                return;
            }
            boolean isAllConstants = true;
            if (operator.getProjection() != null) {
                isAllConstants = operator.getProjection().getColumnRefMap().values().stream()
                        .allMatch(ScalarOperator::isConstantRef);
            } else if (CollectionUtils.isNotEmpty(operator.getRows())) {
                isAllConstants = operator.getRows().stream().allMatch(row ->
                        row.stream().allMatch(ScalarOperator::isConstantRef));
            }
            if (!isAllConstants) {
                return;
            }
            fixPhysicalValuesOperator(optExpr, operator, context);
        }
    }

    /**
     * Fix the column order of PhysicalValuesOperator
     * @param optExpr
     * @param physicalValuesOperator
     * @param context
     */
    public void fixPhysicalValuesOperator(OptExpression optExpr, PhysicalValuesOperator physicalValuesOperator, ExecPlan context) {
        List<ColumnRefOperator> columnRefOperatorList = physicalValuesOperator.getColumnRefSet();
        // Check if repair is needed
        if (!needsOrderFix(columnRefOperatorList)) {
            return;
        }

        // Create a sort mapping
        List<Integer> sortList = new ArrayList<>();
        List<ColumnRefOperator> newSortColumnRefList = new ArrayList<>();

        for (ColumnRefOperator expectedColumn : expectedOutputColumns) {
            boolean matched = false;
            for (int i = 0; i < columnRefOperatorList.size(); i++) {
                ColumnRefOperator columnRefOperator = columnRefOperatorList.get(i);
                if (columnRefOperator.getId() == expectedColumn.getId()) {
                    sortList.add(i);
                    newSortColumnRefList.add(columnRefOperator);
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                LOG.warn("fixPhysicalValuesOperator: cannot match expected column {} in {}",
                        expectedColumn, columnRefOperatorList);
            }
        }
        // output column name complete different with ColumnRefOperator
        if (newSortColumnRefList.isEmpty() || newSortColumnRefList.size() != columnRefOperatorList.size()) {
            return;
        }

        // If the order remains unchanged, return directly
        if (sortList.size() == columnRefOperatorList.size() && isAlreadyInOrder(sortList)) {
            return;
        }

        // Reorder the row data
        List<List<ScalarOperator>> newSortRowsList = new ArrayList<>();
        for (List<ScalarOperator> row : physicalValuesOperator.getRows()) {
            List<ScalarOperator> curRows = new ArrayList<>();
            for (int index : sortList) {
                curRows.add(row.get(index));
            }
            newSortRowsList.add(curRows);
        }

        // Create a new PhysicalValuesOperator
        PhysicalValuesOperator newSortPhysicalValueOperator = new PhysicalValuesOperator(
                newSortColumnRefList,
                newSortRowsList,
                physicalValuesOperator.getLimit(),
                physicalValuesOperator.getPredicate(),
                physicalValuesOperator.getProjection()
        );

        // Replacement operator
        optExpr.setOp(newSortPhysicalValueOperator);
        if (context.getPhysicalPlan() != null && context.getPhysicalPlan().getOp() == physicalValuesOperator) {
            context.getPhysicalPlan().setOp(newSortPhysicalValueOperator);
        }
    }


    /**
     * Check if it is already in order
     * @param sortList
     * @return
     */
    public boolean isAlreadyInOrder(List<Integer> sortList) {
        for (int i = 0; i < sortList.size(); i++) {
            if (sortList.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if the sequence needs to be repaired
     * @param currentOrder
     * @return
     */
    public boolean needsOrderFix(List<ColumnRefOperator> currentOrder) {
        if (currentOrder.size() != expectedOutputColumns.size()) {
            return false;
        }
        for (int i = 0; i < currentOrder.size(); i++) {
            // The field has no name and cannot be sorted
            if (currentOrder.get(i).getName().isEmpty()) {
                return false;
            }
            String realOutputName = currentOrder.get(i).getName();
            String realColumnName = expectedOutputColumns.get(i).getName();
            if (!realOutputName.equalsIgnoreCase(realColumnName)) {
                return true;
            }
        }
        return false;
    }
}
