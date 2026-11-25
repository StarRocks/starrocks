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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The TABLET clause of a table reference on an OLAP table, checked against the table the reference reads, which for a
 * read scoped to a bookmark is the table as of the bookmark. SELECT and DELETE check their tablets here alike.
 *
 * <p>The reference reads the partitions its PARTITION or TEMPORARY PARTITION clause names, or else the partitions of
 * the table, and each named tablet must be a tablet of one of them. That depends on the statement and the table alone,
 * so a tablet that is gone, such as one a split replaced, fails every statement that names it. Whether a named tablet
 * holds anything the statement reads is left to partition pruning: when the WHERE clause rules its partition out, or
 * the partition holds no data, the tablet is not read, and no error is raised for it.
 */
public final class TabletClause {

    /**
     * Each tablet the clause names, once and in the order first named, mapped to the partition that holds it. A tablet
     * named twice is read once: read twice, its rows would come back twice, and a DELETE would delete, and count, each
     * of its keys twice.
     *
     * @throws SemanticException if a named tablet is not a tablet of the partitions the reference reads
     */
    public static Map<Long, Partition> resolve(OlapTable table, PartitionRef partitionNames, List<Long> tabletIds) {
        Map<Long, Partition> partitionOfTablet = new LinkedHashMap<>();
        if (tabletIds.isEmpty()) {
            return partitionOfTablet;
        }
        // A scan reads, in each physical partition, the queryable layout of the base index of the table, so a tablet of
        // a layout that a schema change or a split has not made queryable yet, or no longer keeps queryable, is not
        // found. The index is taken from the table, not from the physical partition: a direct read of a synchronous
        // materialized view swaps the base index of its copy of the table for the index of the view, and the physical
        // partitions still name the base index of the table.
        Map<Long, Partition> found = partitionsHolding(readPartitions(table, partitionNames), table.getBaseIndexMetaId(),
                new HashSet<>(tabletIds));
        for (long tabletId : tabletIds) {
            Partition partition = found.get(tabletId);
            if (partition == null) {
                throw tabletNotRead(table, tabletId);
            }
            partitionOfTablet.putIfAbsent(tabletId, partition);
        }
        return partitionOfTablet;
    }

    /**
     * The partition that holds each of the tablets, among the given partitions, in the queryable layout of the index.
     * A tablet none of them holds is left out.
     *
     * <p>A TABLET clause may name thousands of tablets of a table with thousands of partitions, so the tablets are
     * found in one pass over those of the partitions, which stops once all are found, rather than by searching every
     * partition for each tablet.
     */
    public static Map<Long, Partition> partitionsHolding(Collection<Partition> partitions, long indexMetaId,
                                                        Set<Long> tabletIds) {
        Map<Long, Partition> partitionOfTablet = new HashMap<>();
        for (Partition partition : partitions) {
            Collection<PhysicalPartition> physicalPartitions = partition.getSubPartitions();
            for (PhysicalPartition physicalPartition : physicalPartitions) {
                List<Tablet> tablets = physicalPartition.getQueryableIndex(indexMetaId).getTablets();
                for (Tablet tablet : tablets) {
                    if (tabletIds.contains(tablet.getId())) {
                        partitionOfTablet.put(tablet.getId(), partition);
                    }
                }
                if (partitionOfTablet.size() == tabletIds.size()) {
                    return partitionOfTablet;
                }
            }
        }
        return partitionOfTablet;
    }

    private static List<Partition> readPartitions(OlapTable table, PartitionRef partitionNames) {
        if (partitionNames == null) {
            return new ArrayList<>(table.getPartitions());
        }
        List<Partition> partitions = new ArrayList<>();
        List<String> names = partitionNames.getPartitionNames();
        for (String name : names) {
            Partition partition = table.getPartition(name, partitionNames.isTemp());
            // Analysis checks the names against the table as it is now, which may have a partition that the table as
            // of a bookmark does not.
            if (partition != null) {
                partitions.add(partition);
            }
        }
        return partitions;
    }

    private static SemanticException tabletNotRead(OlapTable table, long tabletId) {
        Set<Long> missing = Set.of(tabletId);
        Partition partition = partitionsHolding(table.getPartitions(), table.getBaseIndexMetaId(), missing).get(tabletId);
        if (partition != null) {
            return new SemanticException("Invalid tablet id: '%d'. The tablet is in partition '%s', " +
                    "which the statement does not name", tabletId, partition.getName());
        }
        Partition temporaryPartition = partitionsHolding(table.getTempPartitions(), table.getBaseIndexMetaId(), missing)
                .get(tabletId);
        if (temporaryPartition != null) {
            return new SemanticException("Invalid tablet id: '%d'. The tablet is in temporary partition '%s', " +
                    "which the statement does not name", tabletId, temporaryPartition.getName());
        }
        return new SemanticException("Invalid tablet id: '%d'. Table '%s' has no such tablet", tabletId,
                table.getName());
    }
}
