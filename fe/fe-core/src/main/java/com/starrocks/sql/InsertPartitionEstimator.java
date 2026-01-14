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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Estimator for partition count based on query predicates.
 *
 * This class analyzes SQL predicates to estimate how many partitions will be affected
 * by an INSERT operation. It handles:
 *
 * 1. AND-connected predicates (conjuncts)
 * 2. OR predicates on the same partition column (e.g., col = 'x' OR col = 'y')
 * 3. IN predicates with constant values
 * 4. Range predicates with NDV-based estimation
 * 5. Column mapping from SELECT output to target partition columns (including SELECT *)
 *
 * The estimator supports column mapping from SELECT output columns to target
 * table partition columns, allowing proper estimation even when source and target
 * tables have different partition column names.
 *
 * Estimation logic:
 * 1. Equality predicates (col = value): typically selects 1 value per column
 * 2. OR predicates (col = 'x' OR col = 'y'): count the number of distinct values
 * 3. IN predicates (col IN (...)): count the number of values in the list
 * 4. Range predicates (col > value): estimate using NDV statistics (10% selectivity)
 *
 * Examples:
 * - dt = '2024-01-01' AND country = 'US'  -> 1 partition
 * - dt = '2024-01-01' AND (country = 'US' OR country = 'CA')  -> 2 partitions
 * - dt IN ('2024-01-01', '2024-01-02') AND country = 'US'  -> 2 partitions
 * - dt > '2024-01-01'  -> estimated based on NDV (e.g., 10% of total partitions)
 */
public class InsertPartitionEstimator {
    private static final Logger LOG = LogManager.getLogger(InsertPartitionEstimator.class);

    private final Set<String> partitionColNames;
    private final Map<String, String> selectColToTargetPartitionCol;
    private final Map<String, Long> columnDistinctValues;
    private final Map<String, Double> partitionNdv;

    /**
     * Create a new partition estimator with pre-built column mapping.
     *
     * @param partitionColNames set of target table partition column names (lowercase)
     * @param selectColToTargetPartitionCol mapping from SELECT output column names to target partition column names
     * @param partitionNdv map from partition column name to NDV statistics
     */
    public InsertPartitionEstimator(Set<String> partitionColNames,
                                     Map<String, String> selectColToTargetPartitionCol,
                                     Map<String, Double> partitionNdv) {
        this.partitionColNames = partitionColNames;
        this.selectColToTargetPartitionCol = selectColToTargetPartitionCol;
        this.columnDistinctValues = new HashMap<>();
        this.partitionNdv = partitionNdv;
    }

    /**
     * Create a new partition estimator by building column mapping from INSERT statement.
     * This factory method handles SELECT * and column alias scenarios.
     *
     * @param insertStmt the INSERT statement
     * @param icebergTable the target Iceberg table
     * @param selectRelation the SELECT relation
     * @param partitionNdv map from partition column name to NDV statistics
     * @return a new InsertPartitionEstimator instance
     */
    public static InsertPartitionEstimator create(InsertStmt insertStmt,
                                                   IcebergTable icebergTable,
                                                   SelectRelation selectRelation,
                                                   Map<String, Double> partitionNdv) {
        Set<String> partitionColNames = icebergTable.getPartitionColumns().stream()
                .map(col -> col.getName().toLowerCase())
                .collect(Collectors.toSet());

        Map<String, String> selectColToTargetPartitionCol =
                buildSelectToTargetPartitionMapping(insertStmt, icebergTable, selectRelation);

        return new InsertPartitionEstimator(partitionColNames, selectColToTargetPartitionCol, partitionNdv);
    }

    /**
     * Estimate partition count based on the given predicate.
     *
     * @param predicate the WHERE clause predicate
     * @return estimated partition count, or null if unable to estimate
     */
    public Long estimate(Expr predicate) {
        // Split compound predicate into conjuncts (AND-connected)
        List<Expr> conjuncts = extractConjuncts(predicate);

        for (Expr conjunct : conjuncts) {
            if (!processPredicate(conjunct)) {
                // If we encounter a predicate we can't handle, return null to indicate
                // we cannot reliably estimate
                return null;
            }
        }

        // If we have equality predicates for all partition columns, return product
        if (columnDistinctValues.size() == partitionColNames.size()) {
            long totalPartitions = 1;
            for (long count : columnDistinctValues.values()) {
                totalPartitions = multiplyWithOverflowCheck(totalPartitions, count);
            }
            return totalPartitions;
        }

        // Partial match - if we have at least one equality predicate on partition column
        // we can provide some estimate, but be conservative
        if (!columnDistinctValues.isEmpty()) {
            long estimate = columnDistinctValues.values().stream()
                    .reduce(1L, InsertPartitionEstimator::multiplyWithOverflowCheck);
            return estimate;
        }

        // No predicate-derived estimate; fall back to NDV from statistics if available
        if (partitionNdv != null && !partitionNdv.isEmpty()) {
            long estimate = 1;
            boolean hasNdv = false;
            for (double ndv : partitionNdv.values()) {
                if (Double.isNaN(ndv) || ndv <= 0) {
                    continue;
                }
                hasNdv = true;
                estimate = multiplyWithOverflowCheck(estimate, (long) Math.ceil(ndv));
            }
            if (hasNdv) {
                return estimate;
            }
        }

        return null;
    }

    /**
     * Build mapping from SELECT output column names to target partition column names.
     * This handles SELECT *, column aliases, and different column names between source and target tables.
     *
     * For example:
     * - INSERT INTO target (id, dt) SELECT id, src_dt FROM src WHERE src_dt = '...'
     *   Creates mapping: {"src_dt" -> "dt"}
     * - INSERT INTO target (id, dt) SELECT * FROM src WHERE src_dt = '...'
     *   Creates mapping: {"src_dt" -> "dt"} (extracts src_dt from source relation fields)
     * - INSERT INTO target (id, dt) SELECT *, 123 FROM src WHERE src_dt = '...'
     *   Creates mapping: {"src_dt" -> "dt"} (handles mixed SELECT * with literals)
     *
     * @param insertStmt the INSERT statement
     * @param icebergTable the target Iceberg table
     * @param selectRelation the SELECT relation
     * @return mapping from SELECT column names to target partition column names
     */
    private static Map<String, String> buildSelectToTargetPartitionMapping(InsertStmt insertStmt,
                                                                           IcebergTable icebergTable,
                                                                           SelectRelation selectRelation) {
        Map<String, String> mapping = new HashMap<>();
        Map<String, String> targetPartitionColMap = icebergTable.getPartitionColumns().stream()
                .collect(Collectors.toMap(
                        col -> col.getName().toLowerCase(),
                        Column::getName,
                        (a, b) -> a));
        if (targetPartitionColMap.isEmpty()) {
            return mapping;
        }

        // Get SELECT list items to extract source column names
        SelectList selectList = selectRelation.getSelectList();
        if (selectList == null) {
            return mapping;
        }

        List<SelectListItem> selectItems = selectList.getItems();
        if (selectItems == null || selectItems.isEmpty()) {
            return mapping;
        }

        List<String> targetColNames = insertStmt.getTargetColumnNames();

        // Check if SELECT list contains any star items
        boolean hasStar = selectItems.stream().anyMatch(item -> item != null && item.isStar());

        // Determine loop limit: for SELECT *, use target schema size; otherwise use select items size
        int loopLimit = hasStar ? icebergTable.getFullSchema().size()
                                : Math.min(selectItems.size(), icebergTable.getFullSchema().size());

        for (int i = 0; i < loopLimit; i++) {
            // Determine which target column this position maps to
            String targetColName;
            if (targetColNames != null && i < targetColNames.size()) {
                targetColName = targetColNames.get(i);
            } else {
                Column targetCol = icebergTable.getFullSchema().get(i);
                if (targetCol == null) {
                    continue;
                }
                targetColName = targetCol.getName();
            }

            // Check if this target column is a partition column using O(1) map lookup
            String targetPartColName = targetPartitionColMap.get(targetColName.toLowerCase());
            if (targetPartColName == null) {
                continue;  // Not a partition column, skip
            }

            // Extract source column name
            String sourceColName;
            if (hasStar) {
                // For SELECT *, source column at position i comes from source relation fields
                sourceColName = getSourceColumnNameForStarSelect(selectRelation, i);
            } else {
                // For explicit column list, extract from the SELECT expression
                SelectListItem selectItem = selectItems.get(i);
                if (selectItem == null) {
                    continue;
                }
                sourceColName = extractSourceColumnName(selectItem, selectRelation, i);
            }

            if (sourceColName != null) {
                // Map source column name -> target partition column name
                mapping.put(sourceColName.toLowerCase(), targetPartColName.toLowerCase());
            }
        }

        return mapping;
    }

    /**
     * Extract the source column name from a SelectListItem.
     *
     * For "SELECT *", extracts the column name from the source relation's fields.
     * For "src_dt AS dt", returns "src_dt" (the source column).
     * For "dt" (no alias), returns "dt".
     * For "col1 + col2", returns null (not a simple column reference).
     *
     * @param selectItem the SELECT list item
     * @param selectRelation the SELECT relation (for SELECT * support)
     * @param position the position in the SELECT list (for SELECT * support)
     * @return the source column name, or null if not a simple column reference
     */
    private static String extractSourceColumnName(SelectListItem selectItem,
                                                   SelectRelation selectRelation,
                                                   int position) {
        if (selectItem.isStar()) {
            // For SELECT *, get the source column name from the relation fields
            return getSourceColumnNameForStarSelect(selectRelation, position);
        }

        Expr expr = selectItem.getExpr();
        if (expr == null) {
            return null;
        }

        // If the expression is a SlotRef (direct column reference), use it
        if (expr instanceof SlotRef) {
            return ((SlotRef) expr).getColumnName();
        }

        // Try to extract SlotRef from implicit cast
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            if (cast.isImplicit() && cast.getChild(0) instanceof SlotRef) {
                return ((SlotRef) cast.getChild(0)).getColumnName();
            }
        }

        // For more complex expressions, return null
        // We only handle simple column references for partition pruning
        return null;
    }

    /**
     * Get the source column name for SELECT * at a given position.
     *
     * For "INSERT INTO target_table (id, dt) SELECT * FROM src_table WHERE ...",
     * where src_table has columns (id, src_dt):
     * - Position 0: maps to source column "id"
     * - Position 1: maps to source column "src_dt"
     *
     * @param selectRelation the SELECT relation
     * @param position the position in the SELECT list
     * @return the source column name, or null if unable to determine
     */
    private static String getSourceColumnNameForStarSelect(SelectRelation selectRelation, int position) {
        try {
            // Get the source relation's fields to find the column name at this position
            Relation relation = selectRelation.getRelation();
            if (relation == null) {
                return null;
            }

            // Check if the analyzer has resolved the relation fields
            // This should be available after analysis phase
            if (relation.getScope() != null) {
                RelationFields relationFields = relation.getRelationFields();
                if (relationFields != null) {
                    List<Field> allFields = relationFields.getAllFields();
                    if (position >= 0 && position < allFields.size()) {
                        Field field = allFields.get(position);
                        if (field != null) {
                            return field.getName();
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to get source column name for SELECT * at position {}: {}",
                    position, e.getMessage());
        }
        return null;
    }

    /**
     * Recursively extract AND-connected predicates.
     * For OR predicates, try to extract values if they're on the same partition column.
     */
    private List<Expr> extractConjuncts(Expr expr) {
        List<Expr> result = new ArrayList<>();
        extractConjunctsRecursive(expr, result);
        return result;
    }

    private void extractConjunctsRecursive(Expr expr, List<Expr> result) {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) expr;
            if (compound.getOp() == CompoundPredicate.Operator.AND) {
                extractConjunctsRecursive(expr.getChild(0), result);
                extractConjunctsRecursive(expr.getChild(1), result);
                return;
            }
            // For OR predicates, keep them as is - will be processed in processPredicate
        }
        result.add(expr);
    }

    private boolean processPredicate(Expr predicate) {
        // Handle CompoundPredicate (OR) - extract values for same column
        if (predicate instanceof CompoundPredicate) {
            return processCompoundPredicate((CompoundPredicate) predicate);
        }

        // Handle BinaryPredicate (equality, range comparisons)
        if (predicate instanceof BinaryPredicate) {
            return processBinaryPredicate((BinaryPredicate) predicate);
        }

        // Handle InPredicate
        if (predicate instanceof InPredicate) {
            return processInPredicate((InPredicate) predicate);
        }

        // For other predicate types we can't analyze (like LIKE), return true to skip
        return true;
    }

    /**
     * Process compound predicates (OR).
     * Extract partition column values from OR predicates like:
     * - col = 'x' OR col = 'y'  (2 values)
     * - col IN ('x', 'y', 'z')  (handled by processInPredicate)
     *
     * Only handles OR predicates on the same partition column.
     * Returns false for complex OR predicates that can't be analyzed.
     */
    private boolean processCompoundPredicate(CompoundPredicate compound) {
        // Only handle OR predicates
        if (compound.getOp() != CompoundPredicate.Operator.OR) {
            return true; // Not an OR predicate, skip
        }

        // Try to extract partition column value counts from OR expression
        Map<String, Long> orValueCounts = new HashMap<>();

        // Recursively extract OR value counts
        if (!extractOrValueCounts(compound, orValueCounts)) {
            // Failed to extract, can't estimate
            return false;
        }

        // If we found partition columns with OR-ed values, use them for estimation
        for (Map.Entry<String, Long> entry : orValueCounts.entrySet()) {
            String targetPartColName = entry.getKey();
            long valueCount = entry.getValue();
            // Use the count of distinct OR values as the estimate
            columnDistinctValues.put(targetPartColName, valueCount);
        }

        return true;
    }

    /**
     * Recursively extract OR value counts from a predicate tree.
     * Returns false if the predicate is too complex to analyze.
     *
     * @param expr the expression to analyze
     * @param orValueCounts map from target partition column name to value count
     * @return true if successful, false if too complex
     */
    private boolean extractOrValueCounts(Expr expr, Map<String, Long> orValueCounts) {
        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compound = (CompoundPredicate) expr;

            if (compound.getOp() == CompoundPredicate.Operator.OR) {
                // Continue recursively
                boolean leftOk = extractOrValueCounts(compound.getChild(0), orValueCounts);
                boolean rightOk = extractOrValueCounts(compound.getChild(1), orValueCounts);

                // Merge results from both sides
                // If both sides have the same column, add the counts
                // If different columns, we can't handle (too complex)
                return leftOk && rightOk;
            } else if (compound.getOp() == CompoundPredicate.Operator.AND) {
                // AND inside OR - too complex for our simple analysis
                // For example: (a = 1 AND b = 2) OR (a = 3 AND b = 4)
                return false;
            }
        }

        // Try to extract equality predicate info
        EqualityPredInfo equalityInfo = extractEqualityPredicateInfo(expr);
        if (equalityInfo != null) {
            // Check if it's a partition column
            String selectColName = equalityInfo.columnName;
            if (isPartitionColumn(selectColName)) {
                String targetPartColName = getTargetPartitionColumnName(selectColName);

                // Increment the count for this partition column
                orValueCounts.merge(targetPartColName, 1L, Long::sum);
            }
            return true;
        }

        // For IN predicate inside OR, handle it
        if (expr instanceof InPredicate) {
            InPredicate inPred = (InPredicate) expr;
            if (!inPred.isNotIn() && inPred.isConstantValues()) {
                SlotRef slotRef = extractSlotRef(inPred.getChild(0));
                if (slotRef != null) {
                    String colName = slotRef.getColumnName().toLowerCase();
                    if (isPartitionColumn(colName)) {
                        String targetPartColName = getTargetPartitionColumnName(colName);
                        // Add the IN predicate value count
                        long count = inPred.getInElementNum();
                        orValueCounts.merge(targetPartColName, count, Long::sum);
                    }
                }
            }
            return true;
        }

        // Unknown predicate type in OR - can't reliably estimate
        return false;
    }

    /**
     * Check if a SELECT column name maps to a target table partition column.
     * First tries the mapping, then falls back to direct name match.
     */
    private boolean isPartitionColumn(String selectColName) {
        // First check if we have a mapping for this SELECT column
        if (selectColToTargetPartitionCol != null) {
            String targetPartCol = selectColToTargetPartitionCol.get(selectColName);
            if (targetPartCol != null && partitionColNames.contains(targetPartCol)) {
                return true;
            }
        }
        // Fallback to direct name match (for backward compatibility)
        return partitionColNames.contains(selectColName);
    }

    /**
     * Get the target partition column name for a SELECT column name.
     * Returns the mapped partition column name, or the original name if no mapping exists.
     */
    private String getTargetPartitionColumnName(String selectColName) {
        if (selectColToTargetPartitionCol != null) {
            String targetPartCol = selectColToTargetPartitionCol.get(selectColName);
            if (targetPartCol != null) {
                return targetPartCol;
            }
        }
        return selectColName;
    }

    /**
     * Helper class to hold equality predicate information
     */
    private static class EqualityPredInfo {
        final String columnName;

        EqualityPredInfo(String columnName) {
            this.columnName = columnName;
        }
    }

    /**
     * Extract column name from an equality predicate (col = value).
     * Returns EqualityPredInfo if this is a simple equality predicate, null otherwise.
     */
    private EqualityPredInfo extractEqualityPredicateInfo(Expr expr) {
        if (!(expr instanceof BinaryPredicate)) {
            return null;
        }

        BinaryPredicate binary = (BinaryPredicate) expr;
        if (binary.getOp() != BinaryType.EQ) {
            return null;
        }

        // Check if one side is a column reference
        SlotRef slotRef = extractSlotRef(binary.getChild(0));
        if (slotRef == null) {
            slotRef = extractSlotRef(binary.getChild(1));
        }

        if (slotRef == null) {
            return null;
        }

        return new EqualityPredInfo(slotRef.getColumnName().toLowerCase());
    }

    private boolean processBinaryPredicate(BinaryPredicate binary) {
        // Check if this is an equality predicate
        boolean isEquality = binary.getOp() == BinaryType.EQ;
        boolean isRange = !isEquality && (
                binary.getOp() == BinaryType.LT ||
                binary.getOp() == BinaryType.LE ||
                binary.getOp() == BinaryType.GT ||
                binary.getOp() == BinaryType.GE);

        // Check if one side is a partition column reference
        SlotRef slotRef = extractSlotRef(binary.getChild(0));
        if (slotRef == null) {
            slotRef = extractSlotRef(binary.getChild(1));
        }

        if (slotRef == null) {
            return true; // Neither side is a simple column reference
        }

        String selectColName = slotRef.getColumnName().toLowerCase();
        // Check if this SELECT column maps to a target table partition column
        if (!isPartitionColumn(selectColName)) {
            return true; // Not a partition column
        }

        // Get the target partition column name for NDV lookup and storing results
        String targetPartColName = getTargetPartitionColumnName(selectColName);

        if (isEquality) {
            // Equality predicate: typically selects 1 value per column
            columnDistinctValues.put(targetPartColName, 1L);
        } else if (isRange) {
            // Range predicate: use NDV statistics to estimate selectivity
            double ndv = partitionNdv != null && partitionNdv.containsKey(targetPartColName)
                    ? partitionNdv.get(targetPartColName) : -1;
            if (ndv > 0) {
                // Estimate using fixed selectivity (10% of partitions)
                // Assumes 10% of partitions are selected by range predicates (e.g., dt > '2024-01-01').
                // This is conservative; actual selectivity depends on range bounds
                long estimated = (long) Math.ceil(ndv * 0.1);
                // Ensure at least 1 partition is estimated
                columnDistinctValues.put(targetPartColName, Math.max(1L, estimated));
            }
            // If no NDV statistics available, we can't estimate - leave empty to trigger fallback
        }
        return true;
    }

    private boolean processInPredicate(InPredicate inPred) {
        // Only handle IN (not NOT IN) with constant values
        if (inPred.isNotIn()) {
            return true; // NOT IN doesn't help estimate partition count
        }

        if (!inPred.isConstantValues()) {
            return true; // Non-constant values, can't estimate
        }

        // Check if the left side is a partition column reference
        SlotRef slotRef = extractSlotRef(inPred.getChild(0));
        if (slotRef == null) {
            return true;
        }

        String selectColName = slotRef.getColumnName().toLowerCase();
        // Check if this SELECT column maps to a target table partition column
        if (!isPartitionColumn(selectColName)) {
            return true; // Not a partition column
        }

        // Get the target partition column name for storing results
        String targetPartColName = getTargetPartitionColumnName(selectColName);

        // Count distinct values in IN list
        long distinctCount = inPred.getInElementNum();
        columnDistinctValues.put(targetPartColName, distinctCount);
        return true;
    }

    private SlotRef extractSlotRef(Expr expr) {
        // Direct SlotRef
        if (expr instanceof SlotRef) {
            return (SlotRef) expr;
        }

        // Unwrap implicit cast
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            if (cast.isImplicit() && cast.getChild(0) instanceof SlotRef) {
                return (SlotRef) cast.getChild(0);
            }
        }

        return null;
    }

    /**
     * Multiply two long values with overflow check.
     * Returns the product, or -1 if overflow would occur.
     */
    public static long multiplyWithOverflowCheck(long base, long factor) {
        if (base == 0 || factor == 0) {
            return 0;
        }
        // Check for potential overflow before multiplying
        if (base > Long.MAX_VALUE / factor) {
            // Overflow risk too high, return -1 to indicate unable to estimate reliably
            return -1;
        }
        return base * factor;
    }
}
