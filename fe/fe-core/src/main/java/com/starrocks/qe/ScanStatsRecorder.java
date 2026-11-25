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

package com.starrocks.qe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.PerScanStatsItemPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Per-scan read statistics of a statement, one entry per execution attempt, serialized as the {@code ScanStats}
 * column of the audit log. A scan is a scan node of the attempt's plan, keyed by its plan node id; the counters
 * come from the {@link PerScanStatsItemPB} items the BEs report. A counter a scan does not track is left out
 * rather than written as 0, and only the connectors whose counters are defined report counters at all.
 */
public class ScanStatsRecorder {
    public static final int SCHEMA_VERSION = 1;

    public enum AttemptState {
        OK, ERR, CANCELLED, TIMEOUT
    }

    public enum ExecutionPath {
        // BE scan nodes read the tables.
        SCAN,
        // The plan scans no table, though the statement references some: metadata answered it.
        METADATA_ONLY,
        // The statement references no table.
        FE_LOCAL
    }

    // The connectors whose counters are defined, by connector name.
    private static final String CONNECTOR_ICEBERG = "iceberg";
    private static final String CONNECTOR_JDBC = "jdbc";
    private static final String CONNECTOR_OLAP = "olap";
    // Connectors whose scans read Parquet / ORC files through the BE file readers, which track the counters. Splits
    // these connectors read through JNI report output rows only.
    private static final Set<String> CONNECTORS_WITH_COUNTERS = ImmutableSet.of(CONNECTOR_ICEBERG, CONNECTOR_JDBC,
            CONNECTOR_OLAP, "hive", "hudi", "deltalake", "paimon", "file");

    private static final class Scan {
        final int scanId;
        final String table;
        final String connector;
        final boolean isMv;
        // The scans an Iceberg equality-delete rewrite split one table scan into share a group id (-1: none). They
        // are reported as that one scan: the data-file scans add up, the equality-delete-file scan is left out.
        final long morGroupId;
        final boolean equalityDeleteScan;

        Scan(int scanId, String table, String connector, boolean isMv, long morGroupId,
                boolean equalityDeleteScan) {
            this.scanId = scanId;
            this.table = table;
            this.connector = connector;
            this.isMv = isMv;
            this.morGroupId = morGroupId;
            this.equalityDeleteScan = equalityDeleteScan;
        }
    }

    private static final class Attempt {
        final int attemptId;
        final String executionId;
        final long startTimeMs;
        final long endTimeMs;
        final List<Scan> scans;
        final boolean scansTruncated;
        // Set when the attempt ended: a retried attempt failed, the last one ends with the statement.
        AttemptState state;
        PQueryStatistics statistics;

        Attempt(int attemptId, String executionId, long startTimeMs, long endTimeMs, List<Scan> scans,
                boolean scansTruncated) {
            this.attemptId = attemptId;
            this.executionId = executionId;
            this.startTimeMs = startTimeMs;
            this.endTimeMs = endTimeMs;
            this.scans = scans;
            this.scansTruncated = scansTruncated;
        }
    }

    private final List<Attempt> attempts = new ArrayList<>();
    private Attempt lastAttempt;

    /** Records an attempt that failed and was retried, with the statistics it reported. */
    public void recordRetriedAttempt(int attemptId, TUniqueId executionId, long startTimeMs, long endTimeMs,
                                     ExecPlan plan, PQueryStatistics statistics) {
        Attempt attempt = newAttempt(attemptId, executionId, startTimeMs, endTimeMs, plan);
        attempt.state = AttemptState.ERR;
        attempt.statistics = statistics;
        attempts.add(attempt);
    }

    /**
     * Records the attempt the statement ends with. Its state and statistics are known only when the statement is
     * audited, see {@link #toJson}.
     */
    public void recordLastAttempt(int attemptId, TUniqueId executionId, long startTimeMs, long endTimeMs,
                                  ExecPlan plan) {
        lastAttempt = newAttempt(attemptId, executionId, startTimeMs, endTimeMs, plan);
    }

    /**
     * The JSON of all attempts. The last attempt takes {@code finalState} and {@code finalStatistics}; a statement
     * that ran no retry loop has none recorded and runs {@code fallbackPlan} as its only attempt.
     * {@code referencesTables} tells a plan answered from metadata from one that needs no table.
     */
    public String toJson(AttemptState finalState, PQueryStatistics finalStatistics, boolean referencesTables,
                         ExecPlan fallbackPlan, TUniqueId fallbackExecutionId, long fallbackStartTimeMs,
                         long endTimeMs) {
        Attempt last = lastAttempt;
        if (last == null) {
            if (fallbackPlan == null) {
                return null;
            }
            last = newAttempt(attempts.size(), fallbackExecutionId, fallbackStartTimeMs, endTimeMs, fallbackPlan);
        }
        last.state = finalState;
        last.statistics = finalStatistics;

        List<Attempt> all = new ArrayList<>(attempts);
        all.add(last);
        JsonObject root = new JsonObject();
        root.addProperty("schemaVersion", SCHEMA_VERSION);
        JsonArray attemptArray = new JsonArray();
        JsonObject lastJson = null;
        for (Attempt attempt : all) {
            lastJson = attemptToJson(attempt, referencesTables);
            attemptArray.add(lastJson);
        }
        root.add("statsComplete", lastJson.get("statsComplete"));
        root.add("attempts", attemptArray);
        return root.toString();
    }

    private static Attempt newAttempt(int attemptId, TUniqueId executionId, long startTimeMs, long endTimeMs,
                                      ExecPlan plan) {
        List<ScanNode> scanNodes = plan == null || plan.getScanNodes() == null ? List.of() : plan.getScanNodes();
        int maxScans = Math.max(0, Config.audit_scan_stats_max_scans);
        // Each equality-delete rewrite creates one IcebergTableMORParams for the scans it splits a table scan into,
        // but its MOR id is the table id, shared by every rewrite of the same table: group by the object instead.
        Map<IcebergTableMORParams, Long> morGroups = new IdentityHashMap<>();
        // The limit counts audit entries: an equality-delete rewrite group is one entry and is kept or dropped as
        // a whole, so a kept entry never misses the counters of a group member.
        List<Scan> scans = new ArrayList<>(Math.min(scanNodes.size(), maxScans));
        Set<Long> keptGroups = new HashSet<>();
        int entries = 0;
        for (ScanNode scanNode : scanNodes) {
            Scan scan = describeScan(scanNode, morGroups);
            if (scan.morGroupId >= 0 && keptGroups.contains(scan.morGroupId)) {
                scans.add(scan);
                continue;
            }
            if (entries >= maxScans) {
                continue;
            }
            entries++;
            if (scan.morGroupId >= 0) {
                keptGroups.add(scan.morGroupId);
            }
            scans.add(scan);
        }
        return new Attempt(attemptId, executionId == null ? null : DebugUtil.printId(executionId), startTimeMs,
                endTimeMs, scans, scanNodes.size() > scans.size());
    }

    @VisibleForTesting
    static String connectorOf(Table table) {
        if (table.isNativeTableOrMaterializedView()) {
            return CONNECTOR_OLAP;
        }
        return table.getType().name().toLowerCase();
    }

    private static Scan describeScan(ScanNode scanNode, Map<IcebergTableMORParams, Long> morGroups) {
        int scanId = scanNode.getId().asInt();
        Table table = scanNode.getDesc() == null ? null : scanNode.getDesc().getTable();
        if (table == null) {
            return new Scan(scanId, scanNode.getTableName(), scanNode.getPlanNodeName().toLowerCase(), false, -1,
                    false);
        }
        long morGroupId = -1;
        boolean equalityDeleteScan = false;
        if (scanNode instanceof IcebergScanNode icebergScanNode && icebergScanNode.getTableFullMORParams() != null &&
                !icebergScanNode.getTableFullMORParams().isEmpty()) {
            morGroupId = morGroups.computeIfAbsent(icebergScanNode.getTableFullMORParams(),
                    params -> (long) morGroups.size());
            equalityDeleteScan = icebergScanNode.getMORParams() != null &&
                    icebergScanNode.getMORParams().getScanTaskType() == IcebergMORParams.ScanTaskType.EQ_DELETE;
        }
        return new Scan(scanId, qualifiedName(table), connectorOf(table), table.isMaterializedView(), morGroupId,
                equalityDeleteScan);
    }

    // catalog.db.table, the same as the QueriedRelations column.
    @VisibleForTesting
    static String qualifiedName(Table table) {
        String catalog = table.getCatalogName();
        String db = null;
        String name = table.getName();
        if (table.isNativeTableOrMaterializedView()) {
            Optional<Long> dbId = table.mayGetDatabaseId();
            if (dbId.isPresent()) {
                Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId.get());
                if (database != null) {
                    db = database.getFullName();
                }
            }
        } else {
            // getName(), not getCatalogTableName(): a JDBC table planned with pushdown carries the remote query there.
            try {
                db = table.getCatalogDBName();
            } catch (Exception e) {
                // A table kind that does not name its database: leave the database out.
            }
        }
        StringBuilder sb = new StringBuilder();
        if (catalog != null) {
            sb.append(catalog).append('.');
        }
        if (db != null) {
            sb.append(db).append('.');
        }
        return sb.append(name).toString();
    }

    private static JsonObject attemptToJson(Attempt attempt, boolean referencesTables) {
        JsonObject json = new JsonObject();
        json.addProperty("attemptId", attempt.attemptId);
        if (attempt.executionId != null) {
            json.addProperty("executionId", attempt.executionId);
        }
        json.addProperty("state", attempt.state.name());
        json.addProperty("startTimeMs", attempt.startTimeMs);
        json.addProperty("endTimeMs", attempt.endTimeMs);
        // Set below, once it is known whether every scan reported.
        json.addProperty("statsComplete", false);
        json.addProperty("executionPath", executionPath(attempt, referencesTables).name());
        if (attempt.scansTruncated) {
            json.addProperty("scansTruncated", true);
        }

        Map<Integer, PerScanStatsItemPB> items = new HashMap<>();
        if (attempt.statistics != null && attempt.statistics.perScanStatsItems != null) {
            for (PerScanStatsItemPB item : attempt.statistics.perScanStatsItems) {
                if (item != null && item.planNodeId != null) {
                    items.put(item.planNodeId, item);
                }
            }
        }
        // An equality-delete rewrite group is reported at its first data-file scan, with the counters of all its
        // data-file scans added up.
        Map<Long, Scan> groupHeads = new HashMap<>();
        Map<Long, PerScanStatsItemPB> groupItems = new HashMap<>();
        for (Scan scan : attempt.scans) {
            if (scan.morGroupId < 0 || scan.equalityDeleteScan) {
                continue;
            }
            groupHeads.putIfAbsent(scan.morGroupId, scan);
            PerScanStatsItemPB item = items.get(scan.scanId);
            if (item != null) {
                groupItems.merge(scan.morGroupId, copyItem(item), ScanStatsRecorder::addItems);
            }
        }
        JsonArray scans = new JsonArray();
        boolean allReported = true;
        for (Scan scan : attempt.scans) {
            PerScanStatsItemPB item;
            if (scan.morGroupId < 0) {
                item = items.get(scan.scanId);
            } else if (groupHeads.get(scan.morGroupId) == scan) {
                item = groupItems.get(scan.morGroupId);
            } else {
                continue;
            }
            if (item == null && hasCounters(scan)) {
                // No report: not run, or by a path or a BE that does not collect them. Its counts are unknown.
                allReported = false;
            }
            scans.add(scanToJson(scan, item));
        }
        json.addProperty("statsComplete", attempt.state == AttemptState.OK && allReported);
        json.add("scans", scans);
        return json;
    }

    private static ExecutionPath executionPath(Attempt attempt, boolean referencesTables) {
        if (!attempt.scans.isEmpty() || attempt.scansTruncated) {
            return ExecutionPath.SCAN;
        }
        return referencesTables ? ExecutionPath.METADATA_ONLY : ExecutionPath.FE_LOCAL;
    }

    private static JsonObject scanToJson(Scan scan, PerScanStatsItemPB item) {
        JsonObject json = new JsonObject();
        json.addProperty("scanId", scan.scanId);
        json.addProperty("table", scan.table);
        json.addProperty("connector", scan.connector);
        json.addProperty("isMv", scan.isMv);
        boolean jdbc = CONNECTOR_JDBC.equals(scan.connector);
        if (!hasCounters(scan) || item == null) {
            // Counters of other connectors are not defined yet; a scan that did not report has unknown counts.
            return json;
        }
        if (item.outputRows != null && item.outputRows == 0 && item.physicalRowsRead == null &&
                item.logicalRowsRead == null && item.remoteBytes == null && item.cacheBytes == null) {
            // Reported, but its reader never opened: it read nothing.
            item = copyItem(item);
            item.physicalRowsRead = 0L;
            item.logicalRowsRead = 0L;
            item.remoteBytes = 0L;
            item.cacheBytes = 0L;
        }
        addIfSet(json, "physicalRowsRead", item.physicalRowsRead);
        addIfSet(json, "logicalRowsRead", item.logicalRowsRead);
        addIfSet(json, "outputRows", item.outputRows);
        addIfSet(json, "remoteBytes", item.remoteBytes);
        addIfSet(json, "cacheBytes", item.cacheBytes);
        if (item.remoteBytes != null || item.cacheBytes != null) {
            json.addProperty("bytesEstimated", jdbc);
        }
        return json;
    }

    // Whether the counters of the scan's connector are defined.
    private static boolean hasCounters(Scan scan) {
        return CONNECTORS_WITH_COUNTERS.contains(scan.connector);
    }

    private static PerScanStatsItemPB copyItem(PerScanStatsItemPB item) {
        PerScanStatsItemPB copied = new PerScanStatsItemPB();
        copied.planNodeId = item.planNodeId;
        copied.physicalRowsRead = item.physicalRowsRead;
        copied.logicalRowsRead = item.logicalRowsRead;
        copied.outputRows = item.outputRows;
        copied.remoteBytes = item.remoteBytes;
        copied.cacheBytes = item.cacheBytes;
        return copied;
    }

    private static PerScanStatsItemPB addItems(PerScanStatsItemPB left, PerScanStatsItemPB right) {
        left.physicalRowsRead = addCounter(left.physicalRowsRead, right.physicalRowsRead);
        left.logicalRowsRead = addCounter(left.logicalRowsRead, right.logicalRowsRead);
        left.outputRows = addCounter(left.outputRows, right.outputRows);
        left.remoteBytes = addCounter(left.remoteBytes, right.remoteBytes);
        left.cacheBytes = addCounter(left.cacheBytes, right.cacheBytes);
        return left;
    }

    private static Long addCounter(Long left, Long right) {
        if (left == null) {
            return right;
        }
        return right == null ? left : left + right;
    }

    private static void addIfSet(JsonObject json, String name, Long value) {
        if (value != null) {
            json.addProperty(name, value);
        }
    }
}
