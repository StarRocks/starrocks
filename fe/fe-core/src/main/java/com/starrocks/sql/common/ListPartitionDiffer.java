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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class ListPartitionDiffer extends PartitionDiffer {
    private static final Logger LOG = LogManager.getLogger(ListPartitionDiffer.class);

    public ListPartitionDiffer(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
    }

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     *
     * Compare the partition of the base table and the partition of the mv.
     * @param baseItems the partition name to its list partition cell of the base table
     * @param mvItems the partition name to its list partition cell of the mv
     * @return the list partition diff between the base table and the mv
     */
    public static PartitionDiff getListPartitionDiff(Map<String, PCell> baseItems,
                                                     Map<String, PCell> mvItems,
                                                     Set<String> uniqueResultNames) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        // for addition, we need to ensure the partition name is unique in case-insensitive
        Map<String, PCell> adds = diffList(baseItems, mvItems, uniqueResultNames);
        // for deletion, we don't need to ensure the partition name is unique since mvItems is used as the reference
        Map<String, PCell> deletes = diffList(mvItems, baseItems, null);
        return new PartitionDiff(adds, deletes);
    }

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     * When `uniqueResultNames` is set, use it to ensure the output partition name is unique in case-insensitive.
     * NOTE: Ensure output map keys are distinct in case-insensitive which is because the key is used for partition name,
     * and StarRocks partition name is case-insensitive.
     */
    public static Map<String, PCell> diffList(Map<String, PCell> srcListMap,
                                              Map<String, PCell> dstListMap,
                                              Set<String> uniqueResultNames) {
        if (CollectionUtils.sizeIsEmpty(srcListMap)) {
            return Maps.newHashMap();
        }

        // PListCell may contain multi values, we need to ensure they are not duplicated from each other
        // NOTE: dstListMap's partition items may be duplicated, we need to collect them first
        Map<PListAtom, PListCell> dstAtomMaps = Maps.newHashMap();
        for (PCell l : dstListMap.values()) {
            Preconditions.checkArgument(l instanceof PListCell, "PListCell expected");
            PListCell pListCell = (PListCell) l;
            pListCell.toAtoms().stream().forEach(x -> dstAtomMaps.put(x, pListCell));
        }

        Map<String, PCell> result = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, PCell> srcEntry : srcListMap.entrySet()) {
            String pName = srcEntry.getKey();
            PListCell srcItem = (PListCell) srcEntry.getValue();

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

    private static Map<PListAtom, Set<PListCellPlus>> toAtoms(Map<String, PCell> partitionMap) {
        Map<PListAtom, Set<PListCellPlus>> result = Maps.newHashMap();
        for (Map.Entry<String, PCell> e : partitionMap.entrySet()) {
            PListCell cell = (PListCell) e.getValue();
            PListCellPlus plus = new PListCellPlus(e.getKey(), cell);
            plus.toAtoms().stream().forEach(x -> {
                result.computeIfAbsent(x, k -> Sets.newHashSet())
                        .add(new PListCellPlus(e.getKey(), cell));
            });
        }
        return result;
    }

    public static Map<String, Set<String>> generateBaseRefMapImpl(Map<PListAtom, Set<PListCellPlus>> mvPartitionMap,
                                                                  Map<String, PCell> baseTablePartitionMap) {
        if (mvPartitionMap.isEmpty()) {
            return Maps.newHashMap();
        }
        // for each partition of base, find the corresponding partition of mv
        Map<PListAtom, Set<PListCellPlus>> baseAtoms = toAtoms(baseTablePartitionMap);
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
     * Collect base table's partition infos.
     */
    @Override
    public Map<Table, Map<String, PCell>> syncBaseTablePartitionInfos() {
        Map<Table, Map<String, PCell>> refBaseTablePartitionMap = Maps.newHashMap();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        try {
            for (Map.Entry<Table, List<Column>> e : refBaseTablePartitionColumns.entrySet()) {
                Table refBaseTable = e.getKey();
                List<Column> refPartitionColumns = e.getValue();
                // collect base table's partition cells by aligning with mv's partition column order
                Map<String, PCell> basePartitionCells = PartitionUtil.getPartitionCells(refBaseTable,
                        refPartitionColumns);
                refBaseTablePartitionMap.put(refBaseTable, basePartitionCells);
            }
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.",
                    DebugUtil.getStackTrace(e));
            return null;
        }
        return refBaseTablePartitionMap;
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

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude) {
        // table -> map<partition name -> partition cell>
        Map<Table, Map<String, PCell>> refBaseTablePartitionMap = syncBaseTablePartitionInfos();
        // merge all base table partition cells
        if (refBaseTablePartitionMap == null) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return null;
        }
        return computePartitionDiff(null, refBaseTablePartitionMap);
    }

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                    Map<Table, Map<String, PCell>> refBaseTablePartitionMap) {
        // generate the reference map between the base table and the mv
        // TODO: prune the partitions based on ttl
        Map<String, PCell> mvPartitionNameToListMap = mv.getPartitionCells(Optional.empty());

        // collect all base table partition cells
        Map<String, PCell> allBasePartitionItems = collectBasePartitionCells(refBaseTablePartitionMap);

        // ensure the result partition name is unique in case-insensitive
        Set<String> uniqueResultNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        uniqueResultNames.addAll(mvPartitionNameToListMap.keySet());

        PartitionDiff diff = ListPartitionDiffer.getListPartitionDiff(allBasePartitionItems,
                mvPartitionNameToListMap, uniqueResultNames);

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
        return new PartitionDiffResult(externalPartitionMaps, refBaseTablePartitionMap, mvPartitionNameToListMap, diff);
    }

    /**
     * Generate the reference map between the base table and the mv.
     * @param basePartitionMaps src partition list map of the base table
     * @param mvPartitionMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    @Override
    public Map<Table, Map<String, Set<String>>> generateBaseRefMap(Map<Table, Map<String, PCell>> basePartitionMaps,
                                                                   Map<String, PCell> mvPartitionMap) {
        Map<PListAtom, Set<PListCellPlus>> mvAtoms = toAtoms(mvPartitionMap);
        Map<Table, Map<String, Set<String>>> result = Maps.newHashMap();
        for (Map.Entry<Table, Map<String, PCell>> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PCell> baseTablePartitionMap = entry.getValue();
            Map<String, Set<String>> baseTableRefMap = generateBaseRefMapImpl(mvAtoms, baseTablePartitionMap);
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
    @Override
    public Map<String, Map<Table, Set<String>>> generateMvRefMap(Map<String, PCell> mvPartitionMap,
                                                                 Map<Table, Map<String, PCell>> basePartitionMaps) {
        Map<String, Map<Table, Set<String>>> result = Maps.newHashMap();
        // for each partition of base, find the corresponding partition of mv
        Map<PListAtom, Set<PListCellPlus>> mvAtoms = toAtoms(mvPartitionMap);
        for (Map.Entry<Table, Map<String, PCell>> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            Map<String, PCell> basePartitionMap = entry.getValue();
            Map<PListAtom, Set<PListCellPlus>> baseAtoms = toAtoms(basePartitionMap);
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
