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

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Finds the tablets (buckets) of a hash-distributed table that can hold rows matching the conditions of a
 * DELETE on a Duplicate, Aggregate or Unique Key table.
 *
 * <p>A row is stored in tablet {@code hash(distribution columns) % tablet count} of every index of its
 * physical partition. When the conditions bind every distribution column to a list of values with {@code =} or
 * {@code IN}, only the buckets of those values can hold a matching row. The buckets are computed by
 * {@link HashDistributionPruner}, the pruner that queries use, from the same kind of filters that the optimizer
 * builds for {@code col = v} and {@code col IN (...)}, with each value converted to the column type first, so
 * that the hash agrees with the one used to place the rows.
 *
 * <p>Pruning is skipped, and every tablet may hold matching rows, when the table is not in NORMAL state, is not
 * hash distributed, a distribution column has a type outside {@link #canPruneOn(Type)}, is not bound by exactly
 * one {@code =} or {@code IN} condition, or a value cannot be converted to the column type.
 */
public class DeleteBucketPruner {
    private static final Logger LOG = LogManager.getLogger(DeleteBucketPruner.class);

    private final List<ColumnId> distributionColumnIds;
    private final List<Column> distributionColumns;
    private final Map<String, PartitionColumnFilter> filters;

    private DeleteBucketPruner(List<ColumnId> distributionColumnIds, List<Column> distributionColumns,
                               Map<String, PartitionColumnFilter> filters) {
        this.distributionColumnIds = distributionColumnIds;
        this.distributionColumns = distributionColumns;
        this.filters = filters;
    }

    /**
     * Returns a pruner for the conditions of a DELETE on {@code table}, or null when the conditions cannot
     * exclude any bucket. The caller must hold the table lock, and {@code conditions} must have been checked by
     * {@link DeleteMgr}, which also normalizes the column names of their slot refs.
     */
    @Nullable
    public static DeleteBucketPruner create(OlapTable table, List<Predicate> conditions) {
        try {
            return createImpl(table, conditions);
        } catch (RuntimeException e) {
            // Pruning is an optimization: never fail the DELETE because of it.
            LOG.warn("Failed to prune the buckets of a DELETE on table {}, writing the delete predicate to every tablet",
                    table.getName(), e);
            return null;
        }
    }

    @Nullable
    private static DeleteBucketPruner createImpl(OlapTable table, List<Predicate> conditions) {
        // A table that is being altered or resharded may hold tablets whose bucket order differs from the
        // hash layout below; keep the existing behaviour for it.
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            return null;
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        if (distributionInfo.getType() != DistributionInfo.DistributionInfoType.HASH) {
            return null;
        }
        List<ColumnId> columnIds = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
        if (columnIds.isEmpty()) {
            return null;
        }
        List<Column> columns = MetaUtils.getColumnsByColumnIds(table, columnIds);
        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        for (Column column : columns) {
            if (!canPruneOn(column.getType())) {
                return null;
            }
            PartitionColumnFilter filter = null;
            for (Predicate condition : conditions) {
                if (!isOnColumn(condition, column)) {
                    continue;
                }
                PartitionColumnFilter conditionFilter = toFilter(condition, column);
                if (conditionFilter == null) {
                    // A condition such as `col > v` or `col NOT IN (...)` does not bind the column, but it only
                    // narrows the rows that another condition on the column selects.
                    continue;
                }
                if (filter != null) {
                    // Two conditions bind the same column. Rare, so do not bother intersecting them.
                    return null;
                }
                filter = conditionFilter;
            }
            if (filter == null) {
                return null;
            }
            filters.put(column.getName(), filter);
        }
        return new DeleteBucketPruner(columnIds, columns, filters);
    }

    /**
     * Returns the ids of the tablets of {@code indexes}, the visible indexes of one physical partition of
     * {@code partition}, that can hold rows matching the conditions, or null when every tablet can.
     */
    @Nullable
    public Set<Long> selectTablets(Partition partition, List<MaterializedIndex> indexes) {
        DistributionInfo distributionInfo = partition.getDistributionInfo();
        if (distributionInfo.getType() != DistributionInfo.DistributionInfoType.HASH
                || !((HashDistributionInfo) distributionInfo).getDistributionColumns().equals(distributionColumnIds)) {
            return null;
        }
        // Loads route a row to tablet `hash % tablet count` of every index, and rollups are built tablet by
        // tablet from their base index, so all indexes of a hash-distributed physical partition have the same
        // number of tablets. Do not prune a layout that does not follow this.
        int tabletNum = -1;
        for (MaterializedIndex index : indexes) {
            int num = index.getTablets().size();
            if (num == 0 || (tabletNum != -1 && num != tabletNum)) {
                return null;
            }
            tabletNum = num;
        }
        Set<Long> selected = Sets.newHashSet();
        try {
            for (MaterializedIndex index : indexes) {
                List<Long> tabletIds = index.getTabletIdsInOrder();
                selected.addAll(new HashDistributionPruner(tabletIds, distributionColumns, filters).prune());
            }
        } catch (RuntimeException e) {
            LOG.warn("Failed to prune the buckets of a DELETE in partition {}, writing the delete predicate to " +
                    "every tablet", partition.getName(), e);
            return null;
        }
        return selected;
    }

    /**
     * Types whose values hash the same way in FE and BE when converted with {@link LiteralExprFactory}. Kept
     * to the common bucketing column types. CHAR is left out because a loaded value is hashed before it is
     * padded, so a DELETE value that differs from the loaded one only in trailing characters could hash to
     * another bucket.
     */
    static boolean canPruneOn(Type type) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case LARGEINT:
            case DATE:
            case DATETIME:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    private static boolean isOnColumn(Predicate condition, Column column) {
        Expr child = condition.getChild(0);
        return child instanceof SlotRef && column.getName().equalsIgnoreCase(((SlotRef) child).getColumnName());
    }

    /**
     * Converts {@code col = v} or {@code col IN (v1, ...)} into the filter that the optimizer would build for it,
     * or returns null for any other condition.
     */
    @Nullable
    private static PartitionColumnFilter toFilter(Predicate condition, Column column) {
        if (condition instanceof BinaryPredicate && ((BinaryPredicate) condition).getOp() == BinaryType.EQ) {
            LiteralExpr value = toColumnType(condition.getChild(1), column);
            if (value == null) {
                return null;
            }
            PartitionColumnFilter filter = new PartitionColumnFilter();
            filter.setLowerBound(value, true);
            filter.setUpperBound(value, true);
            return filter;
        } else if (condition instanceof InPredicate && !((InPredicate) condition).isNotIn()) {
            List<LiteralExpr> values = Lists.newArrayList();
            for (int i = 1; i < condition.getChildren().size(); i++) {
                LiteralExpr value = toColumnType(condition.getChild(i), column);
                if (value == null) {
                    return null;
                }
                values.add(value);
            }
            if (values.isEmpty()) {
                return null;
            }
            PartitionColumnFilter filter = new PartitionColumnFilter();
            filter.setInPredicateLiterals(values);
            return filter;
        }
        return null;
    }

    /**
     * Converts a value to a literal of the column type, which is how BE parses the value of the delete
     * predicate. A literal of another type would hash differently, for example an INT literal for a BIGINT
     * column. Returns null for NULL or a value that cannot be converted.
     */
    @Nullable
    private static LiteralExpr toColumnType(Expr value, Column column) {
        if (!(value instanceof LiteralExpr) || value instanceof NullLiteral) {
            return null;
        }
        try {
            return LiteralExprFactory.create(((LiteralExpr) value).getStringValue(), column.getType());
        } catch (AnalysisException | RuntimeException e) {
            return null;
        }
    }
}
