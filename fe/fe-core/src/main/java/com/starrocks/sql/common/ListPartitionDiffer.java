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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class ListPartitionDiffer extends PartitionDiffer {
    private static final Logger LOG = LogManager.getLogger(ListPartitionDiffer.class);

    public ListPartitionDiffer(MaterializedView mv, MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        super(mv, queryRewriteParams);
    }

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     *
     * Compare the partition of the base table and the partition of the mv.
     * @param baseItems the partition name to its list partition cell of the base table
     * @param mvItems the partition name to its list partition cell of the mv
     * @return the list partition diff between the base table and the mv
     */
    public static PartitionDiff getListPartitionDiff(PCellSortedSet baseItems,
                                                     PCellSortedSet mvItems,
                                                     Set<String> uniqueResultNames) {
        // This synchronization method has a one-to-one correspondence
        // between the base table and the partition of the mv.
        // for addition, we need to ensure the partition name is unique in case-insensitive
        PCellSortedSet adds = diffList(baseItems, mvItems, uniqueResultNames);
        // for deletion, we don't need to ensure the partition name is unique since mvItems is used as the reference
        PCellSortedSet deletes = diffList(mvItems, baseItems, null);
        return new PartitionDiff(adds, deletes);
    }

    /**
     * Iterate srcListMap, if the partition name is not in dstListMap or the partition value is different, add into result.
     * When `uniqueResultNames` is set, use it to ensure the output partition name is unique in case-insensitive.
     * NOTE: Ensure output map keys are distinct in case-insensitive which is because the key is used for partition name,
     * and StarRocks partition name is case-insensitive.
     */
    public static PCellSortedSet diffList(PCellSortedSet srcPCells,
                                          PCellSortedSet dstPCells,
                                          Set<String> uniqueResultNames) {
        if (srcPCells == null || srcPCells.isEmpty()) {
            return PCellSortedSet.of();
        }
        // PListCell may contain multi values, we need to ensure they are not duplicated from each other
        // NOTE: dstListMap's partition items may be duplicated, we need to collect them first
        // Use TreeMap to maintain sorted order for efficient merge-join operations
        Map<PListCell, PListCell> dstAtomMaps = dstPCells
                .stream()
                .flatMap(l -> {
                    PListCell pListCell = (PListCell) l.cell();
                    return pListCell.toSingleValueCells().stream().map(x -> Map.entry(x, pListCell));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (v1, v2) -> v1, Maps::newTreeMap));
        PCellSortedSet result = PCellSortedSet.of();
        for (PCellWithName srcPCell : srcPCells.getPartitions()) {
            String pName = srcPCell.name();
            PListCell srcItem = (PListCell) srcPCell.cell();
            if (dstPCells.containsPCellWithName(srcPCell)) {
                continue;
            }
            // Get sorted source atoms for merge-join filtering
            Set<PListCell> srcAtoms = srcItem.toSingleValueCells();
            List<PListCell> srcDistinctAtoms = filterDistinctAtoms(srcAtoms, dstAtomMaps);

            if (!srcDistinctAtoms.isEmpty()) {
                srcDistinctAtoms.forEach(atom -> dstAtomMaps.put(atom, srcItem));
                List<List<String>> newSrcItems = srcDistinctAtoms
                        .stream()
                        .map(PListCell::getPartitionItems)
                        .map(items -> items.get(0))
                        .collect(Collectors.toList());
                PListCell newSrcValue = new PListCell(newSrcItems);
                // ensure the partition name is unique
                if (uniqueResultNames != null && uniqueResultNames.contains(pName)) {
                    try {
                        // it's fine to use result to keep it unique here, since we always
                        pName = AnalyzerUtils.calculateUniquePartitionName(pName, newSrcValue, result);
                    } catch (Exception e) {
                        throw new RuntimeException("Fail to calculate unique partition name: " + e.getMessage());
                    }
                    uniqueResultNames.add(pName);
                }
                result.add(PCellWithName.of(pName, newSrcValue));
            }
        }
        return result;
    }

    /**
     * Filter out atoms that exist in dstAtomMaps using merge-join algorithm.
     * This is more efficient than checking containsKey for each atom when dealing with sorted collections.
     * @param srcAtoms source atoms (will be sorted)
     * @param dstAtomMaps destination atom map (must be sorted, e.g., TreeMap)
     * @return list of atoms in srcAtoms that are not in dstAtomMaps
     */
    private static List<PListCell> filterDistinctAtoms(Set<PListCell> srcAtoms,
                                                       Map<PListCell, PListCell> dstAtomMaps) {
        if (srcAtoms.isEmpty()) {
            return Lists.newArrayList();
        }
        if (dstAtomMaps.isEmpty()) {
            return Lists.newArrayList(srcAtoms);
        }

        // For small sets, direct containsKey check is more efficient
        if (srcAtoms.size() < 10) {
            return srcAtoms.stream()
                    .filter(atom -> !dstAtomMaps.containsKey(atom))
                    .collect(Collectors.toList());
        }

        // For larger sets, use merge-join algorithm
        List<PListCell> result = Lists.newArrayList();
        List<PListCell> sortedSrcAtoms = srcAtoms.stream()
                .sorted()
                .collect(Collectors.toList());

        Iterator<PListCell> srcIter = sortedSrcAtoms.iterator();
        Iterator<Map.Entry<PListCell, PListCell>> dstIter = dstAtomMaps.entrySet().iterator();

        if (!srcIter.hasNext() || !dstIter.hasNext()) {
            while (srcIter.hasNext()) {
                result.add(srcIter.next());
            }
            return result;
        }

        PListCell srcAtom = srcIter.next();
        Map.Entry<PListCell, PListCell> dstEntry = dstIter.next();

        while (srcAtom != null && dstEntry != null) {
            int cmp = srcAtom.compareTo(dstEntry.getKey());
            if (cmp == 0) {
                // srcAtom exists in dstAtomMaps, skip it (not distinct)
                srcAtom = srcIter.hasNext() ? srcIter.next() : null;
                dstEntry = dstIter.hasNext() ? dstIter.next() : null;
            } else if (cmp < 0) {
                // srcAtom < dstEntry, this atom is distinct
                result.add(srcAtom);
                srcAtom = srcIter.hasNext() ? srcIter.next() : null;
            } else {
                // srcAtom > dstEntry, advance dst iterator
                dstEntry = dstIter.hasNext() ? dstIter.next() : null;
            }
        }

        // Add remaining source atoms (all are distinct)
        while (srcAtom != null) {
            result.add(srcAtom);
            srcAtom = srcIter.hasNext() ? srcIter.next() : null;
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
    public static boolean hasListPartitionChanged(PCellSortedSet baseListMap,
                                                  PCellSortedSet mvListMap) {
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
    public static boolean checkListPartitionChanged(PCellSortedSet srcListMap,
                                                    PCellSortedSet dstListMap) {
        for (PCellWithName srcEntry : srcListMap.getPartitions()) {
            String key = srcEntry.name();
            PCell srcItem = srcEntry.cell();
            if (!srcItem.equals(dstListMap.getPCell(key))) {
                return true;
            }
        }
        return false;
    }

    private static Map<PListCell, Set<PCellWithName>> toSortedAtoms(PCellSortedSet partitionMap) {
        Map<PListCell, Set<PCellWithName>> result = Maps.newTreeMap();
        for (PCellWithName cellWithName : partitionMap.getPartitions()) {
            PListCell cell = (PListCell) cellWithName.cell();
            if (cell == null) {
                LOG.warn("PListCell has null partition");
                continue;
            }
            cell.toSingleValueCells().forEach(x -> {
                result.computeIfAbsent(x, k -> Sets.newHashSet())
                        .add(cellWithName);
            });
        }
        return result;
    }

    /**
     * Generate the reference map between the base table and the mv using merge-join algorithm.
     * @param mvAtoms mv partition atoms (sorted)
     * @param mvAtoms base table partition map
     * @return base partition name -> mv partition names mapping
     */
    public PCellSetMapping generateBaseRefMapImpl(Map<PListCell, Set<PCellWithName>> mvAtoms,
                                                  Map<PListCell, Set<PCellWithName>> baseAtoms) {
        PCellSetMapping result = PCellSetMapping.of();
        if (mvAtoms.isEmpty()) {
            // all base partitions have no corresponding mv partitions
            for (Set<PCellWithName> baseCellPluses : baseAtoms.values()) {
                baseCellPluses.forEach(x -> result.put(x.name()));
            }
            return result;
        }

        // Merge-join algorithm: iterate both sorted maps simultaneously
        Iterator<Map.Entry<PListCell, Set<PCellWithName>>> mvIter = mvAtoms.entrySet().iterator();
        Iterator<Map.Entry<PListCell, Set<PCellWithName>>> baseIter = baseAtoms.entrySet().iterator();
        if (!mvIter.hasNext() || !baseIter.hasNext()) {
            return result;
        }

        Map.Entry<PListCell, Set<PCellWithName>> mvEntry = mvIter.next();
        Map.Entry<PListCell, Set<PCellWithName>> baseEntry = baseIter.next();
        while (mvEntry != null && baseEntry != null) {
            // check query rewrite exhausted
            queryRewriteParams.checkQueryRewriteExhausted();

            int cmp = baseEntry.getKey().compareTo(mvEntry.getKey());
            if (cmp == 0) {
                // Found matching atoms - record the relationship
                Set<PCellWithName> mvCellPluses = mvEntry.getValue();
                Set<PCellWithName> baseCellPluses = baseEntry.getValue();
                for (PCellWithName baseCellPlus : baseCellPluses) {
                    mvCellPluses.forEach(x -> result.put(baseCellPlus.name(), x));
                }
                // Advance both iterators
                mvEntry = mvIter.hasNext() ? mvIter.next() : null;
                baseEntry = baseIter.hasNext() ? baseIter.next() : null;
            } else if (cmp < 0) {
                // baseEntry < mvEntry, this base partition has no corresponding mv partition
                Set<PCellWithName> baseCellPluses = baseEntry.getValue();
                baseCellPluses.forEach(x -> result.put(x.name()));
                // Advance base iterator
                baseEntry = baseIter.hasNext() ? baseIter.next() : null;
            } else {
                // baseEntry > mvEntry, advance mv iterator
                mvEntry = mvIter.hasNext() ? mvIter.next() : null;
            }
        }
        // Handle remaining base entries that have no corresponding mv partitions
        while (baseEntry != null) {
            Set<PCellWithName> baseCellPluses = baseEntry.getValue();
            baseCellPluses.forEach(x -> result.put(x.name()));
            baseEntry = baseIter.hasNext() ? baseIter.next() : null;
        }

        return result;
    }

    /**
     * Collect base table's partition infos.
     */
    @Override
    public Map<Table, PCellSortedSet> syncBaseTablePartitionInfos() {
        Map<Table, PCellSortedSet> refBaseTablePartitionMap = Maps.newHashMap();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        try {
            for (Map.Entry<Table, List<Column>> e : refBaseTablePartitionColumns.entrySet()) {
                Table refBaseTable = e.getKey();
                List<Column> refPartitionColumns = e.getValue();
                // collect base table's partition cells by aligning with mv's partition column order
                PCellSortedSet basePartitionCells = PartitionUtil.getPartitionCells(refBaseTable,
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

    public static PCellSortedSet collectBasePartitionCells(Map<Table, PCellSortedSet> basePartitionMaps) {
        // NOTE: how to handle the partition name conflict between different base tables?
        // case1: partition name not equal but partition value equal, it's ok
        // case2: partition name is equal, but partition value is different,
        // merge into a total map to compute the difference
        Map<String, PCellWithName> allPartitionCells = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        basePartitionMaps.values().forEach(tableCells -> {
            tableCells.forEach(pCell -> {
                // only use name to check the existed partition
                PCellWithName existed = allPartitionCells.get(pCell.name());
                if (existed != null) {
                    PListCell listCell = (PListCell) existed.cell();
                    listCell.addItems(((PListCell) pCell.cell()).getPartitionItems());
                } else {
                    allPartitionCells.put(pCell.name(), pCell);
                }
            });
        });
        return PCellSortedSet.of(allPartitionCells.values());
    }

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude) {
        // table -> map<partition name -> partition cell>
        Map<Table, PCellSortedSet> refBaseTablePartitionMap = syncBaseTablePartitionInfos();
        // merge all base table partition cells
        if (refBaseTablePartitionMap == null) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return null;
        }
        return computePartitionDiff(null, refBaseTablePartitionMap);
    }

    @Override
    public PartitionDiffResult computePartitionDiff(Range<PartitionKey> rangeToInclude,
                                                    Map<Table, PCellSortedSet> refBaseTablePartitionMap) {
        // generate the reference map between the base table and the mv
        // TODO: prune the partitions based on ttl
        PCellSortedSet mvPartitionNameToListMap = mv.getPartitionCells(Optional.empty());

        // collect all base table partition cells
        PCellSortedSet allBasePartitionItems = collectBasePartitionCells(refBaseTablePartitionMap);

        // ensure the result partition name is unique in case-insensitive
        Set<String> uniqueResultNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        uniqueResultNames.addAll(mvPartitionNameToListMap.getPartitionNames());

        PartitionDiff diff = ListPartitionDiffer.getListPartitionDiff(allBasePartitionItems,
                mvPartitionNameToListMap, uniqueResultNames);

        // collect external partition column mapping
        Map<Table, PartitionNameSetMap> externalPartitionMaps = Maps.newHashMap();
        if (!queryRewriteParams.isQueryRewrite()) {
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
     * Generate the reference map between the base table and the mv using optimized merge-join algorithm.
     * @param basePartitionMaps src partition list map of the base table
     * @param mvPartitionMap mv partition name to its list partition cell
     * @return base table -> <partition name, mv partition names> mapping
     */
    @Override
    public Map<Table, PCellSetMapping> generateBaseRefMap(Map<Table, PCellSortedSet> basePartitionMaps,
                                                          PCellSortedSet mvPartitionMap) {
        Map<Table, PCellSetMapping> result = Maps.newHashMap();
        // Convert mv partitions to sorted atoms once and reuse for all base tables
        Map<PListCell, Set<PCellWithName>> mvAtoms = toSortedAtoms(mvPartitionMap);
        for (Map.Entry<Table, PCellSortedSet> entry : basePartitionMaps.entrySet()) {
            Table baseTable = entry.getKey();
            // Convert base table partitions to sorted atoms
            Map<PListCell, Set<PCellWithName>> baseAtoms = toSortedAtoms(entry.getValue());
            // Use merge-join algorithm to find matching partitions in O(B + M) instead of O(B × log M)
            PCellSetMapping baseTableRefMap = generateBaseRefMapImpl(mvAtoms, baseAtoms);
            result.put(baseTable, baseTableRefMap);
        }
        return result;
    }

    /**
     * Generate the reference map between the mv and the base table.
     * @param mvPCells mv partition name to its list partition cell
     * @param baseTablePCells src partition list map of the base table
     * @return mv partition name -> <base table, base partition names> mapping
     */
    @Override
    public Map<String, Map<Table, PCellSortedSet>> generateMvRefMap(PCellSortedSet mvPCells,
                                                                    Map<Table, PCellSortedSet> baseTablePCells) {
        Map<String, Map<Table, PCellSortedSet>> result = Maps.newHashMap();
        // Use sorted maps for merge-join optimization
        Map<PListCell, Set<PCellWithName>> mvAtoms = toSortedAtoms(mvPCells);
        for (Map.Entry<Table, PCellSortedSet> entry : baseTablePCells.entrySet()) {
            Table baseTable = entry.getKey();
            Map<PListCell, Set<PCellWithName>> baseAtoms = toSortedAtoms(entry.getValue());
            // Merge-join algorithm: iterate both sorted maps simultaneously
            Iterator<Map.Entry<PListCell, Set<PCellWithName>>> mvIter = mvAtoms.entrySet().iterator();
            Iterator<Map.Entry<PListCell, Set<PCellWithName>>> baseIter = baseAtoms.entrySet().iterator();
            if (!mvIter.hasNext() || !baseIter.hasNext()) {
                continue;
            }
            Map.Entry<PListCell, Set<PCellWithName>> mvEntry = mvIter.next();
            Map.Entry<PListCell, Set<PCellWithName>> baseEntry = baseIter.next();
            while (mvEntry != null && baseEntry != null) {
                // check query rewrite exhausted
                queryRewriteParams.checkQueryRewriteExhausted();

                int cmp = mvEntry.getKey().compareTo(baseEntry.getKey());
                if (cmp == 0) {
                    // Found matching atoms - record the relationship
                    Set<PCellWithName> mvCellPluses = mvEntry.getValue();
                    Set<PCellWithName> baseCellPluses = baseEntry.getValue();
                    for (PCellWithName mvCell : mvCellPluses) {
                        baseCellPluses.forEach(x ->
                                result.computeIfAbsent(mvCell.name(), k -> Maps.newHashMap())
                                        .computeIfAbsent(baseTable, k -> PCellSortedSet.of())
                                        .add(x)
                        );
                    }
                    // Advance both iterators
                    mvEntry = mvIter.hasNext() ? mvIter.next() : null;
                    baseEntry = baseIter.hasNext() ? baseIter.next() : null;
                } else if (cmp < 0) {
                    // mvEntry < baseEntry, advance mv iterator
                    mvEntry = mvIter.hasNext() ? mvIter.next() : null;
                } else {
                    // mvEntry > baseEntry, advance base iterator
                    baseEntry = baseIter.hasNext() ? baseIter.next() : null;
                }
            }
        }
        return result;
    }
}
