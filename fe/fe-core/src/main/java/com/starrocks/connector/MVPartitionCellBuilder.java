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

package com.starrocks.connector;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.iceberg.IcebergPartitionKeyResolver;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.type.PrimitiveType;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds MV partition cells (range or list) from external base table partition names.
 * <p>
 * Pipeline: resolve partition keys (via {@link ExternalPartitionKeyResolver})
 *         → transform (date conversion, sorting)
 *         → build cells (range boundaries or list cells)
 *         → generate MV partition names
 * <p>
 * This class owns all MV partition cell construction logic that was previously
 * scattered across {@link PartitionUtil}.
 */
public class MVPartitionCellBuilder {

    private MVPartitionCellBuilder() {
    }

    // ========== Public entry points ==========

    /**
     * Build range cells [lower, upper) for non-JDBC external tables, or (lower, upper] for JDBC tables.
     * Resolves partition names to PartitionKeys, collects source name mapping, sorts, and builds cells.
     */
    public static BaseToMVPartitionMapping buildRangeCells(Table baseTable, Column baseTablePartitionColumn,
                                                 Collection<String> basePartitionNames,
                                                 Expr mvPartitionExpr) throws AnalysisException {
        ExternalPartitionMappingContext mappingContext =
                ExternalPartitionMappingContext.create(baseTable, baseTablePartitionColumn, mvPartitionExpr);
        ExternalPartitionKeyResolver partitionKeyResolver = getResolver(baseTable);
        PartitionUtil.DateTimeInterval basePartitionInterval =
                PartitionUtil.getDateTimeInterval(baseTable, baseTablePartitionColumn);

        // Resolve partition names to PartitionKeys, collect source name mapping, then sort by key value
        Map<String, PartitionKey> mvPartitionKeysByName = Maps.newHashMap();
        Map<String, Set<String>> sourceMapping = new HashMap<>();
        for (String basePartitionName : basePartitionNames) {
            PartitionKeyResolutionResult resolutionResult =
                    partitionKeyResolver.resolve(mappingContext, basePartitionName);
            PartitionKey mvPartitionKey = resolutionResult.getSingleKey();
            String mvPartitionName = generateMVPartitionName(mvPartitionKey);
            mvPartitionKeysByName.put(mvPartitionName, mvPartitionKey);
            sourceMapping.computeIfAbsent(mvPartitionName, k -> new java.util.HashSet<>()).add(basePartitionName);
        }
        LinkedHashMap<String, PartitionKey> sortedKeys = mvPartitionKeysByName.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        PCellSortedSet cells;
        if (baseTable.isJDBCTable()) {
            cells = buildOpenClosedRangeCells(sortedKeys, baseTablePartitionColumn, mvPartitionExpr);
        } else {
            cells = buildClosedOpenRangeCells(sortedKeys, baseTablePartitionColumn,
                    mvPartitionExpr, basePartitionInterval);
        }
        return BaseToMVPartitionMapping.of(cells, sourceMapping);
    }

    /**
     * Build list partition cells from external base table partition names.
     */
    public static BaseToMVPartitionMapping buildListCells(Table baseTable, List<Column> mvRefBasePartitionColumns,
                                                          Collection<String> basePartitionNames) throws AnalysisException {
        ExternalPartitionMappingContext mappingContext =
                ExternalPartitionMappingContext.create(baseTable, mvRefBasePartitionColumns);
        ExternalPartitionKeyResolver partitionKeyResolver = getResolver(baseTable);

        PCellSortedSet mvPartitionListMap = PCellSortedSet.of();
        Map<String, Set<String>> sourceMapping = new HashMap<>();
        for (String basePartitionName : basePartitionNames) {
            PartitionKeyResolutionResult resolutionResult =
                    partitionKeyResolver.resolve(mappingContext, basePartitionName);
            for (PartitionKey mvPartitionKey : resolutionResult.getKeys()) {
                String mvPartitionName = generateMVPartitionName(mvPartitionKey);
                List<List<String>> mvPartitionItems = generateMVPartitionList(mvPartitionKey);
                mvPartitionListMap.add(PCellWithName.of(mvPartitionName, new PListCell(mvPartitionItems)));
                sourceMapping.computeIfAbsent(mvPartitionName, k -> new java.util.HashSet<>()).add(basePartitionName);
            }
        }
        return BaseToMVPartitionMapping.of(mvPartitionListMap, sourceMapping);
    }

    /**
     * Get MV partition names for given base table partition names.
     * Dispatches to range or list cell building based on partition type.
     */
    public static Set<String> getMVPartitionNames(Table baseTable, Column baseTablePartitionColumn,
                                                  List<String> basePartitionNames,
                                                  boolean mvUsesListPartitioning,
                                                  Expr mvPartitionExpr) throws AnalysisException {
        if (mvUsesListPartitioning) {
            PCellSortedSet mvPartitionNamesWithList = buildListCells(
                    baseTable, ImmutableList.of(baseTablePartitionColumn), basePartitionNames).cells();
            return mvPartitionNamesWithList.getPartitionNames();
        } else {
            PCellSortedSet mvPartitionNamesWithRange = buildRangeCells(
                    baseTable, baseTablePartitionColumn, basePartitionNames, mvPartitionExpr).cells();
            return mvPartitionNamesWithRange.getPartitionNames();
        }
    }

    // ========== Unified entry points (OLAP + external tables) ==========

    /**
     * Get range partition cells for any table type (OLAP or external).
     * For OLAP tables, reads directly from OlapTable's range partition map.
     * For external tables, resolves partition names via the resolver pipeline.
     * @param pinnedVersionRange if non-null, partition enumeration uses this snapshot (Iceberg);
     *                           if null, uses the live snapshot (current behavior).
     */
    public static BaseToMVPartitionMapping getPartitionKeyRange(Table table, Column partitionColumn,
                                                                Expr partitionExpr,
                                                                TvrVersionRange pinnedVersionRange)
            throws AnalysisException {
        if (table.isNativeTableOrMaterializedView()) {
            return BaseToMVPartitionMapping.of(getOlapRangePartitionMap((OlapTable) table));
        }
        return buildRangeCells(table, partitionColumn, getPartitionNames(table, pinnedVersionRange), partitionExpr);
    }

    public static BaseToMVPartitionMapping getPartitionKeyRange(Table table, Column partitionColumn,
                                                                Expr partitionExpr) throws AnalysisException {
        return getPartitionKeyRange(table, partitionColumn, partitionExpr, null);
    }

    /**
     * Get list partition cells for any table type (OLAP or external).
     * For OLAP tables, reads directly from OlapTable's partition cells.
     * For external tables, resolves partition names via the resolver pipeline.
     * @param pinnedVersionRange if non-null, partition enumeration uses this snapshot (Iceberg);
     *                           if null, uses the live snapshot (current behavior).
     */
    public static BaseToMVPartitionMapping getPartitionCells(Table table, List<Column> partitionColumns,
                                                             TvrVersionRange pinnedVersionRange)
            throws AnalysisException {
        if (table.isNativeTableOrMaterializedView()) {
            return BaseToMVPartitionMapping.of(((OlapTable) table).getPartitionCells(Optional.of(partitionColumns)));
        }
        return buildListCells(table, partitionColumns, getPartitionNames(table, pinnedVersionRange));
    }

    public static BaseToMVPartitionMapping getPartitionCells(Table table, List<Column> partitionColumns)
            throws AnalysisException {
        return getPartitionCells(table, partitionColumns, null);
    }

    private static PCellSortedSet getOlapRangePartitionMap(OlapTable olapTable) {
        if (!olapTable.getPartitionInfo().isRangePartition()) {
            throw new IllegalArgumentException("Must be range partitioned table");
        }
        return olapTable.getRangePartitionMap();
    }

    /**
     * Get partition names, optionally at a pinned snapshot.
     * @param pinnedVersionRange if non-null, uses this snapshot for Iceberg; if null, uses live snapshot.
     */
    public static List<String> getPartitionNames(Table table, TvrVersionRange pinnedVersionRange) {
        return ConnectorPartitionTraits.build(table, pinnedVersionRange).getPartitionNames();
    }

    /**
     * Generate MV partition name from partition key.
     * Example: PartitionKey([2020-01-01]) → "p20200101"
     */
    public static String generateMVPartitionName(PartitionKey mvPartitionKey) {
        StringBuilder sb = new StringBuilder("p");
        List<String> mvPartitionNameFragments = mvPartitionKey.getKeys()
                .stream()
                .map(MVPartitionCellBuilder::generateLiteralPartitionName)
                .collect(Collectors.toList());
        sb.append(Joiner.on("_").join(mvPartitionNameFragments));
        return sb.toString();
    }

    // ========== Internal: range building ==========

    /**
     * Build [lower, upper) range cells for non-JDBC external tables (Hive, Hudi, Iceberg, Paimon).
     */
    private static PCellSortedSet buildClosedOpenRangeCells(
            LinkedHashMap<String, PartitionKey> mvPartitionKeysByName,
            Column baseTablePartitionColumn,
            Expr mvPartitionExpr,
            PartitionUtil.DateTimeInterval basePartitionInterval)
            throws AnalysisException {
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mvPartitionExpr, baseTablePartitionColumn);
        PrimitiveType basePartitionColumnPrimitiveType =
                isConvertToDate ? PrimitiveType.DATE : baseTablePartitionColumn.getPrimitiveType();

        PCellSortedSet mvPartitionRangeMap = PCellSortedSet.of();
        for (Map.Entry<String, PartitionKey> entry : mvPartitionKeysByName.entrySet()) {
            String mvPartitionName = entry.getKey();
            PartitionKey basePartitionLowerBound =
                    isConvertToDate ? PartitionUtil.convertToDate(entry.getValue()) : entry.getValue();
            if (basePartitionLowerBound.getKeys().get(0).isNullable()) {
                basePartitionLowerBound = PartitionKey.createInfinityPartitionKeyWithType(
                        ImmutableList.of(basePartitionColumnPrimitiveType), false);
            }
            Preconditions.checkState(!mvPartitionRangeMap.containsName(mvPartitionName));
            PartitionKey basePartitionUpperBound = nextPartitionKey(
                    basePartitionLowerBound, basePartitionInterval, basePartitionColumnPrimitiveType);
            mvPartitionRangeMap.add(
                    mvPartitionName,
                    PRangeCell.of(Range.closedOpen(basePartitionLowerBound, basePartitionUpperBound)));
        }
        return mvPartitionRangeMap;
    }

    /**
     * Build (lower, upper] range cells for JDBC tables.
     * JDBC partitions use different boundary semantics than Hive-style tables.
     */
    private static PCellSortedSet buildOpenClosedRangeCells(
            LinkedHashMap<String, PartitionKey> mvPartitionKeysByName,
            Column baseTablePartitionColumn,
            Expr mvPartitionExpr)
            throws AnalysisException {
        boolean isConvertToDate = PartitionUtil.isConvertToDate(mvPartitionExpr, baseTablePartitionColumn);
        PrimitiveType basePartitionColumnPrimitiveType =
                isConvertToDate ? PrimitiveType.DATE : baseTablePartitionColumn.getPrimitiveType();

        PCellSortedSet mvPartitionRangeMap = PCellSortedSet.of();
        PartitionKey lastBasePartitionKey = null;
        String mvPartitionName = null;

        for (Map.Entry<String, PartitionKey> entry : mvPartitionKeysByName.entrySet()) {
            mvPartitionName = entry.getKey();
            if (lastBasePartitionKey == null) {
                lastBasePartitionKey = entry.getValue();
                if (!lastBasePartitionKey.getKeys().get(0).isMinValue()) {
                    lastBasePartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                            ImmutableList.of(basePartitionColumnPrimitiveType), false);
                } else {
                    if (lastBasePartitionKey.getKeys().get(0).isNullable()) {
                        lastBasePartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                                ImmutableList.of(basePartitionColumnPrimitiveType), false);
                    } else {
                        lastBasePartitionKey = isConvertToDate
                                ? PartitionUtil.convertToDate(entry.getValue()) : entry.getValue();
                    }
                    continue;
                }
            }
            PartitionKey basePartitionUpperBound = isConvertToDate
                    ? PartitionUtil.convertToDate(entry.getValue()) : entry.getValue();
            Preconditions.checkState(!mvPartitionRangeMap.containsName(mvPartitionName));
            mvPartitionRangeMap.add(
                    mvPartitionName,
                    PRangeCell.of(Range.openClosed(lastBasePartitionKey, basePartitionUpperBound)));
            lastBasePartitionKey = basePartitionUpperBound;
        }

        return mvPartitionRangeMap;
    }

    // ========== Internal: helpers ==========

    private static PartitionKey nextPartitionKey(PartitionKey currentPartitionKey,
                                                 PartitionUtil.DateTimeInterval dateTimeInterval,
                                                 PrimitiveType partitionColumnPrimitiveType)
            throws AnalysisException {
        LiteralExpr literalExpr = PartitionUtil.addOffsetForLiteral(
                currentPartitionKey.getKeys().get(0), 1, dateTimeInterval);
        PartitionKey nextPartitionKey = new PartitionKey();
        nextPartitionKey.pushColumn(literalExpr, partitionColumnPrimitiveType);
        return nextPartitionKey;
    }

    private static ExternalPartitionKeyResolver getResolver(Table baseTable) {
        if (baseTable.isIcebergTable()) {
            return IcebergPartitionKeyResolver.INSTANCE;
        }
        return DefaultPartitionKeyResolver.INSTANCE;
    }

    private static String generateLiteralPartitionName(LiteralExpr literalExpr) {
        return literalExpr.getStringValue().replaceAll("[^a-zA-Z0-9_]*", "");
    }

    private static List<List<String>> generateMVPartitionList(PartitionKey mvPartitionKey) {
        List<List<String>> mvPartitionItems = Lists.newArrayList();
        List<String> mvPartitionItem = Lists.newArrayList();
        for (LiteralExpr key : mvPartitionKey.getKeys()) {
            mvPartitionItem.add(key.getStringValue());
        }
        mvPartitionItems.add(mvPartitionItem);
        return mvPartitionItems;
    }
}
