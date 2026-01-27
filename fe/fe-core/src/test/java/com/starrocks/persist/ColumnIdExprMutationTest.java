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

package com.starrocks.persist;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for ColumnIdExpr mutation bug.
 *
 * This test demonstrates the bug where convertToColumnNameExpr() mutates the internal
 * SlotRef state instead of returning a new expression with the updated column name.
 *
 * Bug scenario:
 * 1. MV1 is created with partition expression containing SlotRef with columnName="event_date"
 * 2. MV1 goes INACTIVE -> ACTIVE
 * 3. During reactivation, convertToColumnNameExpr() is called which mutates the SlotRef
 * 4. The SlotRef's columnName changes from "event_date" to something else (e.g., "mv_event_date")
 * 5. When MV2 (nested MV based on MV1) tries to validate, it fails because the column names don't match
 *
 * See: https://github.com/StarRocks/starrocks/issues/XXXXX
 */
public class ColumnIdExprMutationTest {

    /**
     * Test that demonstrates the mutation bug in convertToColumnNameExpr.
     *
     * When convertToColumnNameExpr is called with a different idToColumn mapping,
     * the original SlotRef's columnName should NOT be modified.
     */
    @Test
    public void testConvertToColumnNameExprMutatesOriginal() {
        // Setup: Create a SlotRef with original column name "event_date"
        String originalColumnName = "event_date";
        SlotRef slotRef = new SlotRef(null, originalColumnName);
        slotRef.setColumnId(ColumnId.create(originalColumnName));

        // Create ColumnIdExpr
        // Note: We need to use the create method that sets column ID by column name
        Expr originalExpr = slotRef;
        ColumnIdExpr columnIdExpr = ColumnIdExpr.create(originalExpr);

        // Verify initial state
        Assertions.assertEquals(originalColumnName, slotRef.getColumnName(),
                "Initial column name should be 'event_date'");

        // Create an idToColumn map that maps the columnId to a DIFFERENT column name
        // This simulates what happens when MV1 is reactivated - the idToColumn map
        // may resolve the columnId to a different column name (e.g., from the MV's internal schema)
        String mutatedColumnName = "mv_event_date";
        Column mutatedColumn = new Column(mutatedColumnName, DateType.DATE);
        mutatedColumn.setColumnId(ColumnId.create(originalColumnName)); // Same column ID, different name

        Map<ColumnId, Column> idToColumn = new HashMap<>();
        idToColumn.put(ColumnId.create(originalColumnName), mutatedColumn);

        // Call convertToColumnNameExpr - this is where the mutation bug occurs
        Expr resultExpr = columnIdExpr.convertToColumnNameExpr(idToColumn);

        // BUG: The original SlotRef has been mutated!
        // This test will FAIL after the fix is applied (which is the correct behavior)
        // Before fix: originalSlotRef.getColumnName() == "mv_event_date" (MUTATED!)
        // After fix: originalSlotRef.getColumnName() == "event_date" (UNCHANGED)

        // Get the SlotRef from the result
        SlotRef resultSlotRef = (SlotRef) resultExpr;

        // The result should have the new column name
        Assertions.assertEquals(mutatedColumnName, resultSlotRef.getColumnName(),
                "Result expression should have the new column name");

        // THE BUG: This assertion will FAIL because the original is mutated
        // After the fix, this assertion should PASS
        Assertions.assertEquals(originalColumnName, slotRef.getColumnName(),
                "BUG: Original SlotRef should NOT be mutated, but it was changed from '"
                + originalColumnName + "' to '" + slotRef.getColumnName() + "'");
    }

    /**
     * Test that calling convertToColumnNameExpr multiple times with different mappings
     * produces consistent results.
     *
     * This test demonstrates that the mutation bug causes inconsistent behavior
     * when the same ColumnIdExpr is used multiple times.
     */
    @Test
    public void testMultipleConversionsProduceConsistentResults() {
        // Setup
        String originalColumnName = "event_date";
        SlotRef slotRef = new SlotRef(null, originalColumnName);
        slotRef.setColumnId(ColumnId.create(originalColumnName));

        ColumnIdExpr columnIdExpr = ColumnIdExpr.create((Expr) slotRef);

        // First conversion with mapping to "column_a"
        Column columnA = new Column("column_a", DateType.DATE);
        columnA.setColumnId(ColumnId.create(originalColumnName));
        Map<ColumnId, Column> idToColumnA = new HashMap<>();
        idToColumnA.put(ColumnId.create(originalColumnName), columnA);

        Expr resultA = columnIdExpr.convertToColumnNameExpr(idToColumnA);
        String resultAColumnName = ((SlotRef) resultA).getColumnName();

        // Second conversion with mapping to "column_b"
        Column columnB = new Column("column_b", DateType.DATE);
        columnB.setColumnId(ColumnId.create(originalColumnName));
        Map<ColumnId, Column> idToColumnB = new HashMap<>();
        idToColumnB.put(ColumnId.create(originalColumnName), columnB);

        Expr resultB = columnIdExpr.convertToColumnNameExpr(idToColumnB);
        String resultBColumnName = ((SlotRef) resultB).getColumnName();

        // The results should be different
        Assertions.assertEquals("column_a", resultAColumnName,
                "First conversion should produce 'column_a'");

        // BUG: Due to mutation, the second conversion may see the already-mutated state
        // After the fix, this should correctly produce "column_b"
        Assertions.assertEquals("column_b", resultBColumnName,
                "Second conversion should produce 'column_b', but due to mutation bug, " +
                "the internal state may have been corrupted by the first conversion");
    }

    /**
     * Test that simulates the nested MV scenario.
     *
     * This test demonstrates the exact bug scenario:
     * 1. MV1 has a partition expression with columnName="event_date"
     * 2. MV1 goes through INACTIVE -> ACTIVE cycle (simulated by calling convertToColumnNameExpr)
     * 3. MV2's validation checks if its partition column matches MV1's partition column
     * 4. Due to mutation, the match fails
     */
    @Test
    public void testNestedMVPartitionValidationFailure() {
        // Simulate MV1's partition expression
        String baseTableColumnName = "event_date";
        SlotRef mv1PartitionSlotRef = new SlotRef(null, baseTableColumnName);
        mv1PartitionSlotRef.setColumnId(ColumnId.create(baseTableColumnName));

        ColumnIdExpr mv1PartitionExpr = ColumnIdExpr.create((Expr) mv1PartitionSlotRef);

        // Save the original column name that MV2 expects to match
        String expectedPartitionColumn = mv1PartitionSlotRef.getColumnName();

        // Simulate MV2's partition expression that references the same column
        String mv2PartitionColumnName = "event_date"; // MV2 references MV1's event_date

        // MV1 goes INACTIVE -> ACTIVE
        // During reactivation, getPartitionColumns() calls convertToColumnNameExpr
        // This simulates the idToColumn mapping in MV1's reactivation
        Column mv1InternalColumn = new Column("mv_internal_event_date", DateType.DATE);
        mv1InternalColumn.setColumnId(ColumnId.create(baseTableColumnName));

        Map<ColumnId, Column> mv1IdToColumn = new HashMap<>();
        mv1IdToColumn.put(ColumnId.create(baseTableColumnName), mv1InternalColumn);

        // This call mutates mv1PartitionSlotRef's columnName (BUG!)
        mv1PartitionExpr.convertToColumnNameExpr(mv1IdToColumn);

        // Now MV2 tries to validate its partition column against MV1's partition column
        // MV2 checks: mv1PartitionColumn.equalsIgnoreCase(mv2PartitionColumnName)
        String actualMv1PartitionColumn = mv1PartitionSlotRef.getColumnName();

        // BUG: This validation fails because mv1PartitionSlotRef was mutated
        // Before fix: actualMv1PartitionColumn == "mv_internal_event_date" (MUTATED!)
        // After fix: actualMv1PartitionColumn == "event_date" (UNCHANGED)

        boolean validationPasses = actualMv1PartitionColumn.equalsIgnoreCase(mv2PartitionColumnName);

        Assertions.assertTrue(validationPasses,
                "BUG: MV2 partition validation should pass, but fails because MV1's partition " +
                "SlotRef was mutated from '" + expectedPartitionColumn + "' to '" +
                actualMv1PartitionColumn + "' during MV1's INACTIVE->ACTIVE cycle. " +
                "This causes the error: 'Materialized view partition column in partition exp " +
                "must be base table partition column'");
    }
}
