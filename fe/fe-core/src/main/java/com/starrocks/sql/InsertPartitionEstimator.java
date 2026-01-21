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

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
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
 * This class provides static utility methods for estimating partition count
 * using multiple strategies:
 *
 * 1. Predicate-based estimation (WHERE clause analysis) - uses inner class PredicateEstimator
 * 2. Statistics-based estimation (NDV)
 * 3. Existing partition count from metadata
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

    // Private constructor to prevent instantiation - this is a utility class
    private InsertPartitionEstimator() {
    }

    /**
     * Estimate partition count for an INSERT operation.
     * This is the main entry point that fetches all necessary metadata and estimates partition count.
     *
     * @param insertStmt the INSERT statement
     * @param table the target table (can be any Table implementation: Iceberg, Hive, etc.)
     * @return estimated partition count, -1 means unable to estimate
     */
    public static long estimatePartitionCountForInsert(InsertStmt insertStmt, Table table) {
        long existingPartitionCount = 0;
        boolean metadataAvailable = false;
        try {
            // Try to get current partition count from table metadata
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .listPartitionNames(table.getCatalogName(),
                            table.getCatalogDBName(),
                            table.getCatalogTableName(),
                            new ConnectorMetadatRequestContext());
            existingPartitionCount = partitionNames.size();
            metadataAvailable = true;

            // Check if there's a WHERE predicate
            QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
            boolean hasWherePredicate = false;
            if (queryRelation instanceof SelectRelation) {
                SelectRelation selectRelation = (SelectRelation) queryRelation;
                hasWherePredicate = selectRelation.hasWhereClause() && selectRelation.getPredicate() != null;
            }

            // Get NDV statistics for partition columns
            Map<String, Double> partitionNdv = getPartitionColumnNdv(table);

            // Use estimatePartitionCount for the actual estimation
            return estimatePartitionCount(insertStmt, table, existingPartitionCount,
                    hasWherePredicate, partitionNdv);
        } catch (Exception e) {
            LOG.warn("Failed to get partition count for table {}.{}: {}",
                    table.getCatalogDBName(), table.getName(), e.getMessage());
        }

        // When metadata was unavailable (exception), return -1 to indicate unable to estimate
        return metadataAvailable ? existingPartitionCount : -1;
    }

    /**
     * Estimate partition count for an INSERT operation.
     * This is the main strategy selector that combines multiple estimation strategies.
     *
     * Estimation strategy (in order of priority):
     * 1. Static partition insert: returns 1
     * 2. Predicate-based estimation (WHERE clause analysis)
     * 3. Statistics-based estimation (NDV)
     * 4. Existing partition count
     *
     * @param insertStmt the INSERT statement
     * @param table the target table
     * @param existingPartitionCount current partition count from metadata
     * @param hasWherePredicate whether the INSERT has a WHERE clause
     * @param partitionNdv optional NDV statistics for partition columns (can be null/empty)
     * @return estimated partition count, -1 means unable to estimate
     */
    public static long estimatePartitionCount(InsertStmt insertStmt,
                                               Table table,
                                               long existingPartitionCount,
                                               boolean hasWherePredicate,
                                               Map<String, Double> partitionNdv) {
        // 1. For static partition INSERT, only writes to one partition
        if (insertStmt.isStaticKeyPartitionInsert()) {
            return 1;
        }

        // Use provided NDV or empty map if null
        Map<String, Double> ndv = (partitionNdv != null) ? partitionNdv : new HashMap<>();

        // 2. Try predicate-based estimation (only if there's a WHERE clause)
        if (hasWherePredicate) {
            return estimateFromPredicates(insertStmt, table, existingPartitionCount, ndv);
        }

        // 3. No WHERE clause, try statistics-based estimation for empty tables
        if (existingPartitionCount == 0 && ndv != null && !ndv.isEmpty()) {
            long statsEstimate = estimateFromStatistics(ndv);
            if (statsEstimate > 0) {
                return statsEstimate;
            }
        }

        // 4. Default to existing partition count
        return existingPartitionCount;
    }

    /**
     * Estimate partition count for an INSERT operation (without NDV).
     * This is a convenience method that calls the main method with null NDV.
     *
     * @param insertStmt the INSERT statement
     * @param table the target table
     * @param existingPartitionCount current partition count from metadata
     * @param hasWherePredicate whether the INSERT has a WHERE clause
     * @return estimated partition count, -1 means unable to estimate
     */
    public static long estimatePartitionCount(InsertStmt insertStmt,
                                               Table table,
                                               long existingPartitionCount,
                                               boolean hasWherePredicate) {
        return estimatePartitionCount(insertStmt, table, existingPartitionCount, hasWherePredicate, null);
    }

    /**
     * Estimate partition count using NDV statistics.
     * This is used as a fallback when partition metadata is unavailable.
     *
     * @param partitionNdv map from partition column name to NDV statistics
     * @return estimated partition count, -1 if unable to estimate
     */
    public static long estimateFromStatistics(Map<String, Double> partitionNdv) {
        if (partitionNdv == null || partitionNdv.isEmpty()) {
            return -1;
        }
        long estimate = 1;
        boolean hasValidNdv = false;
        for (double ndv : partitionNdv.values()) {
            if (Double.isNaN(ndv) || ndv <= 0) {
                return -1;  // Any invalid NDV makes the entire estimate unreliable
            }
            hasValidNdv = true;
            long result = multiplyWithOverflowCheck(estimate, (long) Math.ceil(ndv));
            if (result == -1) {
                // Overflow detected
                return -1;
            }
            estimate = result;
        }
        return hasValidNdv ? estimate : -1;
    }

    /**
     * Estimate partition count from WHERE clause predicates.
     *
     * @param insertStmt the INSERT statement
     * @param table the target table
     * @param existingPartitionCount current partition count from metadata
     * @param partitionNdv map from partition column name to NDV statistics
     * @return estimated partition count, -1 if unable to estimate
     */
    private static long estimateFromPredicates(InsertStmt insertStmt,
                                                Table table,
                                                long existingPartitionCount,
                                                Map<String, Double> partitionNdv) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();

        // Only support SelectRelation with WHERE clause
        if (!(queryRelation instanceof SelectRelation)) {
            return -1;
        }

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        if (!selectRelation.hasWhereClause()) {
            // No WHERE clause, return existing partition count
            return existingPartitionCount;
        }

        Expr predicate = selectRelation.getPredicate();
        if (predicate == null) {
            return existingPartitionCount;
        }

        if (table.getPartitionColumns().isEmpty()) {
            return -1;
        }

        // Build column mapping
        Map<String, String> selectColToTargetPartitionCol =
                buildSelectToTargetPartitionMapping(insertStmt, table, selectRelation);

        // Create PredicateEstimator to handle predicate-based estimation
        Set<String> partitionColNames = table.getPartitionColumns().stream()
                .map(col -> col.getName().toLowerCase())
                .collect(Collectors.toSet());

        PredicateEstimator estimator = new PredicateEstimator(
                partitionColNames, selectColToTargetPartitionCol, partitionNdv);
        Long estimatedCount = estimator.estimate(predicate);
        // If predicate estimation returns null (unable to estimate), return -1
        return estimatedCount != null ? estimatedCount : -1;
    }

    /**
     * Get NDV statistics for partition columns.
     *
     * @param table the target table
     * @return map from partition column name to NDV statistics
     */
    private static Map<String, Double> getPartitionColumnNdv(Table table) {
        Map<String, Double> partitionNdv = new HashMap<>();
        if (table == null || table.getPartitionColumns().isEmpty()) {
            return partitionNdv;
        }

        try {
            if (GlobalStateMgr.getCurrentState().getStatisticStorage() == null) {
                return partitionNdv;
            }
            for (Column partitionCol : table.getPartitionColumns()) {
                ColumnStatistic statistic = GlobalStateMgr.getCurrentState().getStatisticStorage()
                        .getColumnStatistic(table, partitionCol.getName());
                if (statistic == null || statistic.isUnknown()) {
                    continue;
                }
                double ndv = statistic.getDistinctValuesCount();
                if (!Double.isNaN(ndv) && ndv > 0) {
                    partitionNdv.put(partitionCol.getName().toLowerCase(), ndv);
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to fetch partition column NDV for table {}.{}: {}",
                    table.getCatalogDBName(), table.getName(), e.getMessage());
        }
        return partitionNdv;
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
     * @param table the target table (can be any Table implementation: Iceberg, Hive, etc.)
     * @param selectRelation the SELECT relation
     * @return mapping from SELECT column names to target partition column names
     */
    private static Map<String, String> buildSelectToTargetPartitionMapping(InsertStmt insertStmt,
                                                                           Table table,
                                                                           SelectRelation selectRelation) {
        Map<String, String> mapping = new HashMap<>();
        Map<String, String> targetPartitionColMap = table.getPartitionColumns().stream()
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
        int loopLimit = hasStar ? table.getFullSchema().size()
                                : Math.min(selectItems.size(), table.getFullSchema().size());

        for (int i = 0; i < loopLimit; i++) {
            // Determine which target column this position maps to
            String targetColName;
            if (targetColNames != null && i < targetColNames.size()) {
                targetColName = targetColNames.get(i);
            } else {
                Column targetCol = table.getFullSchema().get(i);
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

    /**
     * Inner class for predicate-based partition count estimation.
     * This class is only instantiated when predicate-based estimation is needed.
     */
    private static class PredicateEstimator {
        private final Set<String> partitionColNames;
        private final Map<String, String> selectColToTargetPartitionCol;
        private final Map<String, Long> columnDistinctValues;
        private final Map<String, Double> partitionNdv;

        /**
         * Create a new predicate estimator.
         *
         * @param partitionColNames set of target table partition column names (lowercase)
         * @param selectColToTargetPartitionCol mapping from SELECT output column names to target partition column names
         * @param partitionNdv map from partition column name to NDV statistics
         */
        PredicateEstimator(Set<String> partitionColNames,
                          Map<String, String> selectColToTargetPartitionCol,
                          Map<String, Double> partitionNdv) {
            this.partitionColNames = partitionColNames;
            this.selectColToTargetPartitionCol = selectColToTargetPartitionCol;
            this.columnDistinctValues = new HashMap<>();
            this.partitionNdv = partitionNdv;
        }

        /**
         * Estimate partition count based on the given predicate.
         *
         * @param predicate the WHERE clause predicate
         * @return estimated partition count, or null if unable to estimate
         */
        Long estimate(Expr predicate) {
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
                    totalPartitions = InsertPartitionEstimator.multiplyWithOverflowCheck(totalPartitions, count);
                    if (totalPartitions == -1) {
                        return null;
                    }
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
                    estimate = InsertPartitionEstimator.multiplyWithOverflowCheck(estimate, (long) Math.ceil(ndv));
                }
                if (hasNdv) {
                    return estimate;
                }
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
                    // Check if OR involves multiple different partition columns
                    if (orValueCounts.size() > 1) {
                        // Multiple partition columns in OR - too complex to estimate accurately
                        // For example: dt = '2024-01-01' OR country = 'US'
                        // could match many more partitions than just the value counts
                        return false;
                    }
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
    }
}
