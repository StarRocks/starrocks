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

package com.starrocks.epack.connector.lakeformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.glue.model.UnfilteredPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Every partition Lake Formation authorized for one table in one attempt. Built once, so pruning and listing
 * see the same list; any partition failing a check refuses the whole table rather than returning a subset.
 */
final class LakeFormationPartitionSnapshot {
    private static final Logger LOG = LogManager.getLogger(LakeFormationPartitionSnapshot.class);

    private final List<String> partitionNames;
    private final Map<String, Partition> partitionsByName;

    private LakeFormationPartitionSnapshot(List<String> partitionNames,
                                           Map<String, Partition> partitionsByName) {
        this.partitionNames = partitionNames;
        this.partitionsByName = partitionsByName;
    }

    List<String> partitionNames() {
        return partitionNames;
    }

    Partition partitionFor(String name) {
        return partitionsByName.get(name);
    }

    boolean contains(String name) {
        return partitionsByName.containsKey(name);
    }

    static LakeFormationPartitionSnapshot build(HiveTable table,
                                                List<UnfilteredPartition> unfilteredPartitions,
                                                Set<String> authorizedColumns,
                                                LakeFormationTableIdentity identity) {
        S3Location tableRoot = S3Location.parse(table.getTableLocation());
        List<String> partitionColumnNames = table.getPartitionColumnNames();
        Set<String> expectedDataColumns = dataColumnsOf(authorizedColumns, partitionColumnNames);

        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableMap.Builder<String, Partition> partitions = ImmutableMap.builder();

        for (UnfilteredPartition unfiltered : unfilteredPartitions) {
            software.amazon.awssdk.services.glue.model.Partition gluePartition = unfiltered.partition();
            org.apache.hadoop.hive.metastore.api.Partition apiPartition =
                    CatalogToHiveConverter.convertPartition(gluePartition);
            String name = PartitionUtil.toHivePartitionName(partitionColumnNames, gluePartition.values());

            checkAuthorizationMatchesTheTable(unfiltered, expectedDataColumns, identity, name);
            StorageDescriptor sd = apiPartition.getSd();
            if (sd == null) {
                throw refuse(identity, name, "it has no storage descriptor");
            }
            checkInsideTableRoot(sd, tableRoot, identity, name);
            checkFormat(sd, identity, name);

            names.add(name);
            partitions.put(name, HiveMetastoreApiConverter.toPartition(sd, apiPartition.getParameters()));
        }
        return new LakeFormationPartitionSnapshot(names.build(), partitions.buildKeepingLast());
    }

    /** A table credential covers only the table's subtree. Names the partition, never its columns. */
    private static void checkInsideTableRoot(StorageDescriptor sd, S3Location tableRoot,
                                             LakeFormationTableIdentity identity, String name) {
        String location = sd.getLocation();
        if (location == null || location.isEmpty()) {
            throw refuse(identity, name, "it has no location");
        }
        if (!S3Location.parse(location).isSameOrDescendantOf(tableRoot)) {
            throw refuse(identity, name, "it is stored outside the table's own location, and tables with"
                    + " custom partition locations are not supported in this version");
        }
    }

    /**
     * ORC and Parquet columns are mapped by position, so a partition in another format would put one column's
     * values under another's name. Same predicate as the table level.
     */
    private static void checkFormat(StorageDescriptor sd, LakeFormationTableIdentity identity, String name) {
        if (!LakeFormationTableGuard.isSupportedStorageFormat(sd)) {
            throw refuse(identity, name, "it is not stored as Parquet, and this version reads only Parquet");
        }
    }

    /** A partition whose authorized columns differ from the table's is refused; the message never lists them. */
    private static void checkAuthorizationMatchesTheTable(UnfilteredPartition unfiltered,
                                                          Set<String> expectedDataColumns,
                                                          LakeFormationTableIdentity identity, String name) {
        // Absent fields refuse, as at the table level: "not sent" cannot be told apart from "unrestricted".
        Boolean registered = unfiltered.isRegisteredWithLakeFormation();
        if (registered == null) {
            throw refuse(identity, name, "Lake Formation returned no registration flag for it, and"
                    + " guessing whether it is registered is not something this can do");
        }
        if (!registered) {
            throw refuse(identity, name, "Lake Formation reports it as not registered while its table is");
        }
        if (!unfiltered.hasAuthorizedColumns()) {
            throw refuse(identity, name, "Lake Formation returned no authorized columns for it, so there"
                    + " is nothing to compare against its table's projection");
        }
        Set<String> granted = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        granted.addAll(unfiltered.authorizedColumns());
        if (!granted.equals(expectedDataColumns)) {
            LOG.warn("Refusing {}: partition {} is authorized for a different column set than its table",
                    identity, name);
            throw refuse(identity, name, "Lake Formation authorized a different set of columns for it than"
                    + " for its table");
        }
    }

    /**
     * The table's authorized columns minus its partition keys: Lake Formation never lists a partition key in a
     * partition's authorized columns, even when the grant names it.
     */
    private static Set<String> dataColumnsOf(Set<String> authorizedColumns,
                                             List<String> partitionColumnNames) {
        Set<String> dataColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        dataColumns.addAll(authorizedColumns);
        if (partitionColumnNames != null) {
            // One at a time: Set.removeAll may use List.contains, bypassing the case-insensitive comparator.
            for (String partitionColumn : partitionColumnNames) {
                dataColumns.remove(partitionColumn);
            }
        }
        return dataColumns;
    }

    private static LakeFormationTableAccessException refuse(LakeFormationTableIdentity identity, String name,
                                                            String why) {
        return new LakeFormationTableAccessException("Cannot query " + identity + ": partition " + name
                + " cannot be read because " + why + ".");
    }

    /** As the metastore cache does: an absent value matches anything, a present one must match exactly. */
    static List<String> filterByValues(List<String> partitionNames, List<java.util.Optional<String>> values) {
        if (values == null || values.stream().noneMatch(java.util.Optional::isPresent)) {
            return partitionNames;
        }
        return new ArrayList<>(PartitionUtil.getFilteredPartitionKeys(partitionNames, values));
    }
}
