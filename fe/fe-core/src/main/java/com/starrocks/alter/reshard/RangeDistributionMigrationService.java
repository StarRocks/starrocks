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

package com.starrocks.alter.reshard;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaxVariant;
import com.starrocks.catalog.MinVariant;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ADMIN-script bridge used by the cross-cluster migration tool for range-distributed tables. */
public class RangeDistributionMigrationService {
    /** A complete external range. A null endpoint is infinity; a null tuple cell is SQL NULL. */
    public record RangeSpec(List<String> lowerBound, boolean lowerIncluded,
                            List<String> upperBound, boolean upperIncluded) {
        public RangeSpec {
            lowerBound = immutableNullableTuple(lowerBound);
            upperBound = immutableNullableTuple(upperBound);
        }

        private static List<String> immutableNullableTuple(List<String> tuple) {
            return tuple == null ? null : Collections.unmodifiableList(new ArrayList<>(tuple));
        }
    }

    public String getTopology(String databaseName, String tableName) throws StarRocksException {
        ResolvedTable resolved = resolveTable(databaseName, tableName);
        Database database = resolved.database();
        OlapTable table = resolved.table();
        JsonObject topology;

        try (AutoCloseableLock ignored = new AutoCloseableLock(database.getId(), table.getId(), LockType.READ)) {
            resolved = reResolveAfterLock(databaseName, tableName, resolved);
            database = resolved.database();
            table = resolved.table();
            validateSupportedTable(database, table);
            topology = new JsonObject();
            topology.addProperty("databaseName", database.getFullName());
            topology.addProperty("tableName", table.getName());
            topology.addProperty("tableId", table.getId());
            JsonArray partitionsJson = new JsonArray();

            List<Partition> partitions = new ArrayList<>(table.getPartitions());
            partitions.sort(Comparator.comparingLong(Partition::getId));
            for (Partition partition : partitions) {
                List<PhysicalPartition> physicalPartitions = new ArrayList<>(partition.getSubPartitions());
                physicalPartitions.sort(Comparator.comparingLong(PhysicalPartition::getId));
                for (PhysicalPartition physicalPartition : physicalPartitions) {
                    partitionsJson.add(toJson(table, partition, physicalPartition));
                }
            }
            topology.add("partitions", partitionsJson);
        }
        return topology.toString();
    }

    public long submitSplit(String databaseName, String tableName,
                            Map<Long, List<RangeSpec>> parentTabletIdToRanges) throws StarRocksException {
        if (!isLeaderAdmissionOpen()) {
            throw new StarRocksException("Range split must be submitted to the active leader FE");
        }
        if (parentTabletIdToRanges == null || parentTabletIdToRanges.isEmpty()) {
            throw new IllegalArgumentException("Range split requires at least one parent tablet");
        }

        ResolvedTable resolved = resolveTable(databaseName, tableName);
        Database database = resolved.database();
        OlapTable table = resolved.table();
        TabletReshardJob job;
        for (Long tabletId : parentTabletIdToRanges.keySet()) {
            if (tabletId == null) {
                throw new IllegalArgumentException("Parent tablet id must not be null");
            }
        }

        try (AutoCloseableLock ignored = new AutoCloseableLock(database.getId(), table.getId(), LockType.READ)) {
            resolved = reResolveAfterLock(databaseName, tableName, resolved);
            database = resolved.database();
            table = resolved.table();
            validateSupportedTable(database, table);

            Map<Long, ParentTablet> parents = findCurrentParents(table, parentTabletIdToRanges.keySet());
            Map<Long, List<Column>> rangeColumnsByIndexMetaId = new LinkedHashMap<>();
            Map<Long, List<TabletRange>> tabletRanges = new LinkedHashMap<>();
            for (Map.Entry<Long, List<RangeSpec>> entry : parentTabletIdToRanges.entrySet()) {
                ParentTablet parent = parents.get(entry.getKey());
                long indexMetaId = parent.index().getMetaId();
                List<Column> rangeColumns = rangeColumnsByIndexMetaId.get(indexMetaId);
                if (rangeColumns == null) {
                    rangeColumns = MetaUtils.getRangeDistributionColumns(table, indexMetaId);
                    rangeColumnsByIndexMetaId.put(indexMetaId, rangeColumns);
                }
                tabletRanges.put(entry.getKey(), convertAndValidateRanges(
                        entry.getKey(), parent.tablet().getRange(), entry.getValue(), rangeColumns));
            }

            job = createSplitJob(database, table, tabletRanges);
        }
        addTabletReshardJob(job);
        return job.getJobId();
    }

    protected TabletReshardJob createSplitJob(Database database, OlapTable table,
                                               Map<Long, List<TabletRange>> ranges) throws StarRocksException {
        return SplitTabletJobFactory.forExternalBoundaries(database, table, ranges);
    }

    protected void addTabletReshardJob(TabletReshardJob job) throws StarRocksException {
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addTabletReshardJob(job);
    }

    protected boolean isLeaderAdmissionOpen() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        return globalStateMgr.isLeader() && globalStateMgr.isLeaderWorkAdmissionOpen();
    }

    private static JsonObject toJson(OlapTable table, Partition partition, PhysicalPartition physicalPartition) {
        JsonObject partitionJson = new JsonObject();
        partitionJson.addProperty("partitionName", partition.getName());
        partitionJson.addProperty("partitionId", partition.getId());
        partitionJson.addProperty("physicalPartitionId", physicalPartition.getId());
        partitionJson.addProperty("visibleVersion", physicalPartition.getVisibleVersion());

        JsonArray indexesJson = new JsonArray();
        List<MaterializedIndex> indexes = new ArrayList<>(
                physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE));
        indexes.sort(Comparator.comparingLong(MaterializedIndex::getMetaId)
                .thenComparingLong(MaterializedIndex::getId));
        for (MaterializedIndex index : indexes) {
            JsonObject indexJson = new JsonObject();
            indexJson.addProperty("indexName", table.getIndexNameByMetaId(index.getMetaId()));
            indexJson.addProperty("indexMetaId", index.getMetaId());
            indexJson.addProperty("currentIndexId", index.getId());
            JsonArray tabletsJson = new JsonArray();
            List<Tablet> tablets = new ArrayList<>(index.getTablets());
            tablets.sort(Comparator.comparing(Tablet::getRange,
                            Comparator.nullsFirst(Comparator.comparing(TabletRange::getRange)))
                    .thenComparingLong(Tablet::getId));
            for (Tablet tablet : tablets) {
                if (tablet.getRange() == null) {
                    throw new IllegalStateException("Range-distributed tablet " + tablet.getId() + " has no range");
                }
                JsonObject tabletJson = new JsonObject();
                tabletJson.addProperty("tabletId", tablet.getId());
                tabletJson.add("range", rangeToJson(tablet.getRange().getRange()));
                tabletsJson.add(tabletJson);
            }
            indexJson.add("tablets", tabletsJson);
            indexesJson.add(indexJson);
        }
        partitionJson.add("indexes", indexesJson);
        return partitionJson;
    }

    private static JsonObject rangeToJson(Range<Tuple> range) {
        JsonObject result = new JsonObject();
        result.add("lowerBound", tupleToJson(range.getLowerBound()));
        result.addProperty("lowerIncluded", range.isLowerBoundIncluded());
        result.add("upperBound", tupleToJson(range.getUpperBound()));
        result.addProperty("upperIncluded", range.isUpperBoundIncluded());
        return result;
    }

    private static com.google.gson.JsonElement tupleToJson(Tuple tuple) {
        if (tuple == null) {
            return JsonNull.INSTANCE;
        }
        JsonArray values = new JsonArray();
        for (Variant value : tuple.getValues()) {
            if (value instanceof NullVariant) {
                values.add(JsonNull.INSTANCE);
            } else if (value instanceof MinVariant || value instanceof MaxVariant) {
                throw new IllegalStateException("MINIMUM/MAXIMUM variants cannot be emitted as finite range values");
            } else {
                values.add(value.getStringValue());
            }
        }
        return values;
    }

    private static List<TabletRange> convertAndValidateRanges(long tabletId, TabletRange parentTabletRange,
                                                               List<RangeSpec> specs, List<Column> columns) {
        if (parentTabletRange == null) {
            throw new IllegalArgumentException("Parent tablet " + tabletId + " has no range");
        }
        if (specs == null || specs.size() < 2 || specs.size() > Config.tablet_reshard_max_split_count) {
            throw new IllegalArgumentException("Parent tablet " + tabletId + " must have between 2 and "
                    + Config.tablet_reshard_max_split_count + " child ranges");
        }

        List<TabletRange> result = new ArrayList<>(specs.size());
        for (int i = 0; i < specs.size(); i++) {
            RangeSpec spec = Objects.requireNonNull(specs.get(i), "Child range must not be null");
            validateHalfOpenRange(spec, tabletId, i);
            Tuple lower = toTuple(spec.lowerBound(), columns, tabletId, i, "lower");
            Tuple upper = toTuple(spec.upperBound(), columns, tabletId, i, "upper");
            if (lower != null && upper != null && lower.compareTo(upper) >= 0) {
                throw new IllegalArgumentException("Child range " + i + " for parent tablet " + tabletId
                        + " is empty or reversed");
            }
            result.add(new TabletRange(Range.of(lower, upper, spec.lowerIncluded(), spec.upperIncluded())));
        }

        Range<Tuple> parent = parentTabletRange.getRange();
        Range<Tuple> first = result.get(0).getRange();
        Range<Tuple> last = result.get(result.size() - 1).getRange();
        if (!Objects.equals(parent.getLowerBound(), first.getLowerBound())
                || parent.isLowerBoundIncluded() != first.isLowerBoundIncluded()
                || !Objects.equals(parent.getUpperBound(), last.getUpperBound())
                || parent.isUpperBoundIncluded() != last.isUpperBoundIncluded()) {
            throw new IllegalArgumentException("Child ranges do not exactly cover parent tablet " + tabletId);
        }
        for (int i = 1; i < result.size(); i++) {
            Range<Tuple> previous = result.get(i - 1).getRange();
            Range<Tuple> current = result.get(i).getRange();
            if (!Objects.equals(previous.getUpperBound(), current.getLowerBound())
                    || previous.isUpperBoundIncluded() == current.isLowerBoundIncluded()) {
                throw new IllegalArgumentException("Child ranges for parent tablet " + tabletId
                        + " have a gap, overlap, or duplicate at position " + i);
            }
        }
        return List.copyOf(result);
    }

    private static void validateHalfOpenRange(RangeSpec spec, long tabletId, int rangeIndex) {
        if (spec.lowerBound() == null && spec.lowerIncluded()) {
            throw new IllegalArgumentException("-infinity cannot be included");
        }
        if (spec.upperBound() == null && spec.upperIncluded()) {
            throw new IllegalArgumentException("+infinity cannot be included");
        }
        if (spec.lowerBound() != null && !spec.lowerIncluded()) {
            throw new IllegalArgumentException("Finite lower endpoint of child range " + rangeIndex
                    + " for parent tablet " + tabletId + " must be included");
        }
        if (spec.upperBound() != null && spec.upperIncluded()) {
            throw new IllegalArgumentException("Finite upper endpoint of child range " + rangeIndex
                    + " for parent tablet " + tabletId + " must be excluded");
        }
    }

    private static Tuple toTuple(List<String> values, List<Column> columns, long tabletId, int rangeIndex,
                                 String endpointName) {
        if (values == null) {
            return null;
        }
        if (values.size() != columns.size()) {
            throw new IllegalArgumentException("The " + endpointName + " endpoint of child range " + rangeIndex
                    + " for parent tablet " + tabletId + " has " + values.size()
                    + " values, expected " + columns.size());
        }
        List<Variant> variants = new ArrayList<>(values.size());
        try {
            for (int i = 0; i < values.size(); i++) {
                String value = values.get(i);
                variants.add(value == null
                        ? Variant.nullVariant(columns.get(i).getType())
                        : Variant.of(columns.get(i).getType(), value));
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid " + endpointName + " endpoint of child range "
                    + rangeIndex + " for parent tablet " + tabletId, e);
        }
        return new Tuple(List.copyOf(variants));
    }

    private static Map<Long, ParentTablet> findCurrentParents(OlapTable table, Iterable<Long> requestedTabletIds)
            throws StarRocksException {
        Map<Long, ParentTablet> parents = new LinkedHashMap<>();
        Map<Long, Boolean> requested = new LinkedHashMap<>();
        for (Long tabletId : requestedTabletIds) {
            requested.put(tabletId, Boolean.TRUE);
        }
        for (Partition partition : table.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        if (requested.containsKey(tablet.getId())) {
                            parents.put(tablet.getId(), new ParentTablet(index, tablet));
                        }
                    }
                }
            }
        }
        List<Long> missingTabletIds = new ArrayList<>();
        for (Long tabletId : requested.keySet()) {
            if (!parents.containsKey(tabletId)) {
                missingTabletIds.add(tabletId);
            }
        }
        if (!missingTabletIds.isEmpty()) {
            missingTabletIds.sort(Long::compareTo);
            throw new StarRocksException("Tablets " + missingTabletIds
                    + " are not current visible tablets of table " + table.getName());
        }
        return parents;
    }

    private static ResolvedTable resolveTable(String databaseName, String tableName) throws StarRocksException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName);
        if (database == null) {
            throw new StarRocksException("Cannot find database " + databaseName);
        }
        Table table = database.getTable(tableName);
        if (!(table instanceof OlapTable olapTable)) {
            throw new StarRocksException("Cannot find OLAP table " + databaseName + "." + tableName);
        }
        return new ResolvedTable(database, olapTable);
    }

    private static ResolvedTable reResolveAfterLock(String databaseName, String tableName,
                                                    ResolvedTable expected) throws StarRocksException {
        Database currentDatabase = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(databaseName);
        Table currentTable = currentDatabase == null ? null : currentDatabase.getTable(tableName);
        OlapTable currentOlapTable = validateCurrentIdentity(
                expected.database().getId(), expected.table().getId(), currentDatabase, currentTable,
                databaseName, tableName);
        return new ResolvedTable(currentDatabase, currentOlapTable);
    }

    private static OlapTable validateCurrentIdentity(long expectedDatabaseId, long expectedTableId,
                                                     Database currentDatabase, Table currentTable,
                                                     String databaseName, String tableName)
            throws StarRocksException {
        if (currentDatabase == null || currentDatabase.getId() != expectedDatabaseId) {
            throw new StarRocksException("Database " + databaseName + " changed while acquiring table lock");
        }
        if (!(currentTable instanceof OlapTable currentOlapTable) || currentTable.getId() != expectedTableId) {
            throw new StarRocksException("Table " + databaseName + "." + tableName
                    + " changed while acquiring table lock");
        }
        return currentOlapTable;
    }

    private static void validateSupportedTable(Database database, OlapTable table) throws StarRocksException {
        if (!table.isCloudNativeTable()) {
            throw new StarRocksException("Table " + database.getFullName() + "." + table.getName()
                    + " is not a cloud-native table");
        }
        if (!table.isRangeDistribution()) {
            throw new StarRocksException("Table " + database.getFullName() + "." + table.getName()
                    + " is not range-distributed");
        }
        if (table.hasColocateGroup()) {
            throw new StarRocksException("Range-colocate tables are not supported by cross-cluster migration");
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new StarRocksException("Table " + database.getFullName() + "." + table.getName()
                    + " is not in NORMAL state: " + table.getState());
        }
    }

    private record ResolvedTable(Database database, OlapTable table) {
    }

    private record ParentTablet(MaterializedIndex index, Tablet tablet) {
    }
}
