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

package com.starrocks.sql.common;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class ListPartitionDiffer extends PartitionDiffer {
    private static final Logger LOG = LogManager.getLogger(ListPartitionDiffer.class);

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     *
     * Compare the partition of the base table and the partition of the mv.
     * @param baseItems the partition name to its list partition cell of the base table
     * @param mvItems the partition name to its list partition cell of the mv
     * @return the list partition diff between the base table and the mv
     */
    public static ListPartitionDiff getListPartitionDiff(Map<String, PListCell> baseItems,
                                                         Map<String, PListCell> mvItems,
                                                         Set<String> uniqueResultNames) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        // for addition, we need to ensure the partition name is unique in case-insensitive
        Map<String, PListCell> adds = diffList(baseItems, mvItems, uniqueResultNames);
        // for deletion, we don't need to ensure the partition name is unique since mvItems is used as the reference
        Map<String, PListCell> deletes = diffList(mvItems, baseItems, null);
        return new ListPartitionDiff(adds, deletes);
    }

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     * When `uniqueResultNames` is set, use it to ensure the output partition name is unique in case-insensitive.
     * NOTE: Ensure output map keys are distinct in case-insensitive which is because the key is used for partition name,
     * and StarRocks partition name is case-insensitive.
     */
    public static Map<String, PListCell> diffList(Map<String, PListCell> srcListMap,
                                                  Map<String, PListCell> dstListMap,
                                                  Set<String> uniqueResultNames) {
        if (CollectionUtils.sizeIsEmpty(srcListMap)) {
            return Maps.newHashMap();
        }

        // PListCell may contain multi values, we need to ensure they are not duplicated from each other
        // NOTE: dstListMap's partition items may be duplicated, we need to collect them first
        Map<PListAtom, PListCell> dstAtomMaps = Maps.newHashMap();
        for (PCell l : dstListMap.values()) {
            PListCell pListCell = (PListCell) l;
            pListCell.toAtoms().stream().forEach(x -> dstAtomMaps.put(x, pListCell));
        }

        Map<String, PListCell> result = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, PListCell> srcEntry : srcListMap.entrySet()) {
            String pName = srcEntry.getKey();
            PListCell srcItem = srcEntry.getValue();

            if (srcItem.equals(dstListMap.get(pName))) {
                continue;
            }

            // distinct atoms
            List<PListAtom> srcDistinctAtoms = srcItem.toAtoms().stream()
                    .filter(atom -> !dstAtomMaps.containsKey(atom))
                    .collect(Collectors.toList());
            if (!srcDistinctAtoms.isEmpty()) {
                srcDistinctAtoms.forEach(atom -> dstAtomMaps.put(atom, srcItem));
                PListCell newValue = new PListCell(
                        srcDistinctAtoms.stream().map(PListAtom::getPartitionItem).collect(Collectors.toList()));

                // ensure the partition name is unique
                if (uniqueResultNames != null) {
                    if (uniqueResultNames.contains(pName)) {
                        try {
                            // it's fine to use result to keep it unique here, since we always
                            pName = AnalyzerUtils.calculateUniquePartitionName(pName, newValue, result);
                        } catch (Exception e) {
                            throw new RuntimeException("Fail to calculate unique partition name: " + e.getMessage());
                        }
                    }
                    uniqueResultNames.add(pName);
                }

                result.put(pName, newValue);
            }
        }
        return result;
    }

    /**
     * Check if the partition of the base table and the partition of the mv have changed.
     *
     * @param baseListMap the partition name to its list partition cell of the base table
     * @param mvListMap   the partition name to its list partition cell of the mv
     * @return true if the partition has changed, otherwise false
     */
    public static boolean hasListPartitionChanged(Map<String, PListCell> baseListMap,
                                                  Map<String, PListCell> mvListMap) {
        if (checkListPartitionChanged(baseListMap, mvListMap)) {
            return true;
        }
        if (checkListPartitionChanged(mvListMap, baseListMap)) {
            return true;
        }
        return false;
    }

    /**
     * Check if src list map is different from dst list map.
     * @param srcListMap src partition list map
     * @param dstListMap dst partition list map
     * @return true if the partition has changed, otherwise false
     */
    public static boolean checkListPartitionChanged(Map<String, PListCell> srcListMap,
                                                    Map<String, PListCell> dstListMap) {
        for (Map.Entry<String, PListCell> srcEntry : srcListMap.entrySet()) {
            String key = srcEntry.getKey();
            PListCell srcItem = srcEntry.getValue();
            if (!srcItem.equals(dstListMap.get(key))) {
                return true;
            }
        }
        return false;
    }

    private static Map<PListAtom, Set<PListCellPlus>> toAtoms(Map<String, PListCell> partitionMap,
                                                              List<Integer> refIdxes) {
        Map<PListAtom, Set<PListCellPlus>> result = Maps.newHashMap();
        for (Map.Entry<String, PListCell> e : partitionMap.entrySet()) {
            PListCellPlus plus = new PListCellPlus(e.getKey(), e.getValue());
            plus.toAtoms(refIdxes).stream()
                    .forEach(x -> result.computeIfAbsent(x, k -> Sets.newHashSet())
                            .add(new PListCellPlus(e.getKey(), e.getValue())));
        }
        return result;
    }

    private static Map<PListAtom, Set<PListCellPlus>> toAtoms(Map<String, PListCell> partitionMap) {
        Map<PListAtom, Set<PListCellPlus>> result = Maps.newHashMap();
        for (Map.Entry<String, PListCell> e : partitionMap.entrySet()) {
            PListCellPlus plus = new PListCellPlus(e.getKey(), e.getValue());
            plus.toAtoms().stream()
                    .forEach(x -> result.computeIfAbsent(x, k -> Sets.newHashSet())
                            .add(new PListCellPlus(e.getKey(), e.getValue())));
        }
        return result;
    }

    public static Map<String, Set<String>> generateBaseRefMap(Map<PListAtom, Set<PListCellPlus>> mvPartitionMap,
                                                              List<Integer> refIdxes,
                                                              Map<String, PListCell> baseTablePartitionMap) {
        if (mvPartitionMap.isEmpty()) {
            return Maps.newHashMap();
        }
        // for each partition of base, find the corresponding partition of mv
        Map<PListAtom, Set<PListCellPlus>> baseAtoms = toAtoms(baseTablePartitionMap, refIdxes);
        Map<String, Set<String>> result = Maps.newHashMap();
        for (Map.Entry<PListAtom, Set<PListCellPlus>> e : baseAtoms.entrySet()) {
            // once base table's singleton is found in mv, add the partition name of mv into result
            PListAtom baseAtom = e.getKey();
            if (mvPartitionMap.containsKey(baseAtom)) {
                Set<PListCellPlus> mvCellPluses = mvPartitionMap.get(baseAtom);
                for (PListCellPlus baseCellPlus : e.getValue()) {
                    mvCellPluses.stream().forEach(x ->
                            result.computeIfAbsent(baseCellPlus.getPartitionName(), k -> Sets.newHashSet())
                                    .add(x.getPartitionName())
                    );
                }
            } else {
                // add an empty set
                Set<PListCellPlus> baseCellPluses = e.getValue();
                baseCellPluses.stream().forEach(x -> result.computeIfAbsent(x.getPartitionName(), k -> Sets.newHashSet()));
            }
        }
        return result;
    }

    /**
     * MV's partition column may not be the same as the base table's partition column, so we need to convert the base
     * which contains multiple columns to the MV's partition cell which only contains one column.
     * @param table ref base table
     * @param inputs ref base table's partition cells
     * @param refIdxes mv's ref indexes to the base table's partition columns
     * @return converted base table partition cells which aligned with mv's partition column
     */
    private static Map<String, PListCell> alignBasePartitionCells(Table table,
                                                                  Map<String, PListCell> inputs,
                                                                  List<Integer> refIdxes) {
        if (table.getPartitionColumnNames().size() == 1) {
            return inputs;
        }
        // sort by partition name to ensure the result is stable
        Map<String, PListCell> sorted = ImmutableSortedMap.copyOf(inputs);
        Set<PListCell> cells = Sets.newHashSet();
        Map<String, PListCell> result = Maps.newTreeMap();
        for (Map.Entry<String, PListCell> e : sorted.entrySet()) {
            String partName = e.getKey();
            PListCell cell = e.getValue();
            PListCell newCell = cell.toPListCell(refIdxes);
            if (cells.contains(newCell)) {
                continue;
            }
            cells.add(newCell);
            result.put(partName, newCell);
        }
        return result;
    }

    /**
     * Get the index of the partition column in the base table.
     * @param refBaseTable base table
     * @param refPartitionColumn base table's column which is referenced by the mv
     * @return the index of the partition column in the base table, throw DmlException if not found
     */
    public static int getRefBaseTableIdx(Table refBaseTable,
                                         Column refPartitionColumn) {
        List<Column> partitionColumns = PartitionUtil.getPartitionColumns(refBaseTable);
        int refIndex = partitionColumns.indexOf(refPartitionColumn);
        if (refIndex == -1) {
            throw new DmlException("Partition column not found in base table: %s", refPartitionColumn.getName());
        }
        return refIndex;
    }

    /**
     * Collect base table's partition infos.
     * @param basePartitionMaps result to collect base table's partition cells for each table
     * @param allBasePartitionItems result to collect all base table's partition cells(merged)
     * @param tableRefIdxes result to collect mv's ref indexes to the base table's partition columns
     * @return true if success, otherwise false
     */
    public static boolean syncBaseTablePartitionInfos(MaterializedView mv,
                                                      Map<Table, Map<String, PListCell>> basePartitionMaps,
                                                      Map<String, PListCell> allBasePartitionItems,
                                                      Map<Table, List<Integer>> tableRefIdxes) {
        Map<Table, Column> partitionTableAndColumn = mv.getRefBaseTablePartitionColumns();
        try {
            for (Map.Entry<Table, Column> e1 : partitionTableAndColumn.entrySet()) {
                Table refBaseTable = e1.getKey();
                Column refPartitionColumn = e1.getValue();

                // support one column partition only, we can support multi columns later.
                int refIndex = getRefBaseTableIdx(refBaseTable, refPartitionColumn);
                List<Integer> refIdxes = Lists.newArrayList(refIndex);
                tableRefIdxes.put(refBaseTable, refIdxes);

                // collect base table's partition cells
                Map<String, PListCell> basePartitionCells = PartitionUtil.getPartitionList(refBaseTable,
                        refPartitionColumn);
                basePartitionMaps.put(refBaseTable, basePartitionCells);

                // convert to base partition cell to mv partition cell(only one column)
                // eg: base table partition column: (dt, region), mv partition column: dt
                Map<String, PListCell> newBasePartitionCells = alignBasePartitionCells(refBaseTable,
                        basePartitionCells, refIdxes);
                // merge into total map to compute the difference
                for (Map.Entry<String, PListCell> e2 : newBasePartitionCells.entrySet()) {
                    String partitionName = e2.getKey();
                    PListCell partitionCell = e2.getValue();
                    allBasePartitionItems.computeIfAbsent(partitionName, k -> new PListCell(Lists.newArrayList()))
                            .addItems(partitionCell.getPartitionItems());
                }
            }
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.",
                    DebugUtil.getStackTrace(e));
            return false;
        }
        return true;
    }

    public static Map<String, PCell> collectBasePartitionCells(Map<Table, Map<String, PCell>> basePartitionMaps) {
        Map<String, PCell> allBasePartitionItems = Maps.newHashMap();
        // NOTE: how to handle the partition name conflict between different base tables?
        // case1: partition name not equal but partition value equal, it's ok
        // case2: partition name is equal, but partition value is different,
        // merge into a total map to compute the difference
        basePartitionMaps.values().forEach(partitionMap ->
                partitionMap.forEach((key, value) -> {
                    PListCell cell = (PListCell) allBasePartitionItems
                            .computeIfAbsent(key, k -> new PListCell(Lists.newArrayList()));
                    cell.addItems(((PListCell) value).getPartitionItems());
                })
        );
        return allBasePartitionItems;
    }

    public static ListPartitionDiffResult computeListPartitionDiff(MaterializedView mv,
                                                                   boolean isQueryRewrite) {
        // table -> map<partition name -> partition cell>
        Map<Table, Map<String, PListCell>> refBaseTablePartitionMap = Maps.newHashMap();
        // merge all base table partition cells
        Map<String, PListCell> allBasePartitionItems = Maps.newHashMap();
        Map<Table, List<Integer>> tableRefIdxes = Maps.newHashMap();
        if (!syncBaseTablePartitionInfos(mv, refBaseTablePartitionMap, allBasePartitionItems, tableRefIdxes)) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return null;
        }
        return computeListPartitionDiff(mv, refBaseTablePartitionMap, allBasePartitionItems, tableRefIdxes, isQueryRewrite);
    }

    public static ListPartitionDiffResult computeListPartitionDiff(
            MaterializedView mv,
            Map<Table, Map<String, PListCell>> refBaseTablePartitionMap,
            Map<String, PListCell> allBasePartitionItems,
            Map<Table, List<Integer>> tableRefIdxes,
            boolean isQueryRewrite) {
        // generate the reference map between the base table and the mv
        // TODO: prune the partitions based on ttl
        Map<String, PListCell> mvPartitionNameToListMap = mv.getListPartitionItems();

        // ensure the result partition name is unique in case-insensitive
        Set<String> uniqueResultNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        uniqueResultNames.addAll(mvPartitionNameToListMap.keySet());

        ListPartitionDiff diff = ListPartitionDiffer.getListPartitionDiff(
                allBasePartitionItems, mvPartitionNameToListMap, uniqueResultNames);

        // collect external partition column mapping
        Map<Table, Map<String, Set<String>>> externalPartitionMaps = Maps.newHashMap();
        if (!isQueryRewrite) {
            try {
                collectExternalPartitionNameMapping(mv.getRefBaseTablePartitionColumns(), externalPartitionMaps);
            } catch (Exception e) {
                LOG.warn("Get external partition column mapping failed.", DebugUtil.getStackTrace(e));
                return null;
            }
        }
        return new ListPartitionDiffResult(mvPartitionNameToListMap, refBaseTablePartitionMap, diff, tableRefIdxes,
                externalPartitionMaps);
    }

    /**
     * Generate the reference map between the base table and the mv.
     * @param basePartitionMaps src partition list map of the base table
     * @param mvPartitionMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    public static Map<Table, Map<String, Set<String>>> generateBaseRefMap(Map<Table, Map<String, PListCell>> basePartitionMaps,
                                                                          Map<Table, List<Integer>> tableRefIdxes,
                                                                          Map<String, PListCell> mvPartitionMap) {
        Map<PListAtom, Set<PListCellPlus>> mvAtoms = toAtoms(mvPartitionMap);
        Map<Table, Map<String, Set<String>>> result = Maps.newHashMap();
        for (Map.Entry<Table, Map<String, PListCell>> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PListCell> baseTablePartitionMap = entry.getValue();
            List<Integer> baseTablePartitionIdxes = tableRefIdxes.get(baseTable);
            Map<String, Set<String>> baseTableRefMap = generateBaseRefMap(mvAtoms,
                    baseTablePartitionIdxes, baseTablePartitionMap);
            result.put(baseTable, baseTableRefMap);
        }
        return result;
    }

    /**
     * Generate the reference map between the mv and the base table.
     * @param mvPartitionMap mv partition name to its list partition cell
     * @param basePartitionMaps src partition list map of the base table
     * @return mv partition name -> <base table, base partition names> mapping
     */
    public static  Map<String, Map<Table, Set<String>>> generateMvRefMap(Map<String, PListCell> mvPartitionMap,
                                                                         Map<Table, List<Integer>> tableRefIdxes,
                                                                         Map<Table, Map<String, PListCell>> basePartitionMaps) {
        Map<String, Map<Table, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        Map<PListAtom, Set<PListCellPlus>> mvAtoms = toAtoms(mvPartitionMap);
        for (Map.Entry<Table, Map<String, PListCell>> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PListCell> basePartitionMap = entry.getValue();
            List<Integer> refIdxes = tableRefIdxes.get(baseTable);
            Map<PListAtom, Set<PListCellPlus>> baseAtoms = toAtoms(basePartitionMap, refIdxes);
            for (Map.Entry<PListAtom, Set<PListCellPlus>> e : baseAtoms.entrySet()) {
                PListAtom singleton = e.getKey();
                Set<PListCellPlus> baseCellPluses = e.getValue();
                if (mvAtoms.containsKey(singleton)) {
                    Set<PListCellPlus> mvCellPluses = mvAtoms.get(singleton);
                    for (PListCellPlus mvCell : mvCellPluses) {
                        baseCellPluses.stream().forEach(x ->
                                result.computeIfAbsent(mvCell.getPartitionName(), k -> Maps.newHashMap())
                                        .computeIfAbsent(baseTable, k -> Sets.newHashSet())
                                        .add(x.getPartitionName())
                        );
                    }
                }
            }
        }
        return result;
    }
}
