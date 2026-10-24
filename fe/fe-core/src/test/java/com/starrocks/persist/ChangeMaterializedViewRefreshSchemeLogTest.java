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


package com.starrocks.persist;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SystemHandler;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ChangeMaterializedViewRefreshSchemeLogTest {

    private String fileName = "./ChangeMaterializedViewRefreshSchemeLogTest";

    @AfterEach
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr,
                           @Injectable Database db) throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));

        List<Column> columns = new LinkedList<Column>();
        columns.add(new Column("k1", IntegerType.TINYINT, true, null, "", ""));
        columns.add(new Column("k2", IntegerType.SMALLINT, true, null, "", ""));
        columns.add(new Column("v1", IntegerType.INT, false, AggregateType.SUM, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setMoment(MaterializedView.RefreshMoment.DEFERRED);
        final MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
        asyncRefreshContext.setStartTime(1655732457);
        asyncRefreshContext.setStep(1);
        asyncRefreshContext.setTimeUnit("DAY");
        MaterializedView materializedView = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        ChangeMaterializedViewRefreshSchemeLog changeLog =
                new ChangeMaterializedViewRefreshSchemeLog(materializedView);
        Text.writeString(out, GsonUtils.GSON.toJson(changeLog, ChangeMaterializedViewRefreshSchemeLog.class));
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        ChangeMaterializedViewRefreshSchemeLog readChangeLog = ChangeMaterializedViewRefreshSchemeLog.read(in);
        final MaterializedView.AsyncRefreshContext readChangeLogAsyncRefreshContext = readChangeLog.getAsyncRefreshContext();
        Assertions.assertEquals(readChangeLog.getRefreshType().name(), "ASYNC");
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getStartTime(), 1655732457);
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getTimeUnit(), "DAY");
        Assertions.assertEquals(readChangeLogAsyncRefreshContext.getStep(), 1);
        in.close();

        new Expectations() {
            {
                globalStateMgr.getCurrentState().getLocalMetastore().getDb(anyLong);
                result = db;

                globalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = materializedView;

                db.getId();
                result = anyLong;

                materializedView.getId();
                result = anyLong;
            }
        };
        new AlterJobMgr(null, null, null)
                .replayChangeMaterializedViewRefreshScheme(changeLog);

        Assertions.assertEquals(materializedView.getRefreshScheme().getMoment(), MaterializedView.RefreshMoment.DEFERRED);
    }

    @Test
    public void testFallBack() throws IOException {
        Config.ignore_materialized_view_error = true;
        String str = "bad data";
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(byteArrayOutputStream), str);
        byteArrayOutputStream.close();
        byte[] data = byteArrayOutputStream.toByteArray();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        ChangeMaterializedViewRefreshSchemeLog readChangeLog = ChangeMaterializedViewRefreshSchemeLog.read(in);
        Assertions.assertEquals(0, readChangeLog.getDbId());
        Config.ignore_materialized_view_error = false;
    }

    @Test
    public void testReplayWhenDbIsEmpty() {
        AlterJobMgr alterJobMgr = new AlterJobMgr(
                new SchemaChangeHandler(),
                new MaterializedViewHandler(),
                new SystemHandler());
        alterJobMgr.replayChangeMaterializedViewRefreshScheme(new ChangeMaterializedViewRefreshSchemeLog());
    }

    @Test
    public void testLastFreshnessConfirmedAtSurvivesReplay(@Mocked GlobalStateMgr globalStateMgr,
                                                           @Injectable Database db) throws IOException {
        long freshnessTime = 1700000000000L;
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));

        List<Column> columns = new LinkedList<>();
        columns.add(new Column("k1", IntegerType.TINYINT, true, null, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastFreshnessConfirmedAt(freshnessTime);
        MaterializedView materializedView = new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
        ChangeMaterializedViewRefreshSchemeLog changeLog =
                new ChangeMaterializedViewRefreshSchemeLog(materializedView);
        Text.writeString(out, GsonUtils.GSON.toJson(changeLog, ChangeMaterializedViewRefreshSchemeLog.class));
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        ChangeMaterializedViewRefreshSchemeLog readChangeLog = ChangeMaterializedViewRefreshSchemeLog.read(in);
        in.close();
        // Not recomputable from the version map, so it must round-trip through the log itself.
        Assertions.assertEquals(freshnessTime, readChangeLog.getLastFreshnessConfirmedAt());

        new Expectations() {
            {
                globalStateMgr.getCurrentState().getLocalMetastore().getDb(anyLong);
                result = db;

                globalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = materializedView;

                db.getId();
                result = anyLong;

                materializedView.getId();
                result = anyLong;
            }
        };
        new AlterJobMgr(null, null, null)
                .replayChangeMaterializedViewRefreshScheme(changeLog);
        Assertions.assertEquals(freshnessTime, materializedView.getRefreshScheme().getLastFreshnessConfirmedAt());
    }



    private static MaterializedView buildMv(MaterializedView.MvRefreshScheme refreshScheme) {
        List<Column> columns = new LinkedList<>();
        columns.add(new Column("k1", IntegerType.TINYINT, true, null, "", ""));
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(1, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(1, (short) 3);
        return new MaterializedView(1000, 100, "mv_name", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, refreshScheme);
    }

    private static Map<String, MaterializedView.BasePartitionInfo> partitionInfo(String name, long refreshTime) {
        Map<String, MaterializedView.BasePartitionInfo> infos = new HashMap<>();
        infos.put(name, new MaterializedView.BasePartitionInfo(1, 2, refreshTime));
        return infos;
    }

    private static ChangeMaterializedViewRefreshSchemeLog writeAndRead(MaterializedView mv, boolean asPreUpgradeLog)
            throws IOException {
        String json = GsonUtils.GSON.toJson(new ChangeMaterializedViewRefreshSchemeLog(mv),
                ChangeMaterializedViewRefreshSchemeLog.class);
        if (asPreUpgradeLog) {
            JsonObject object = JsonParser.parseString(json).getAsJsonObject();
            object.remove("lastRefreshTime");
            json = object.toString();
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        Text.writeString(out, json);
        out.flush();
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            return ChangeMaterializedViewRefreshSchemeLog.read(in);
        }
    }

    private static void replay(ChangeMaterializedViewRefreshSchemeLog log, MaterializedView mv,
                               GlobalStateMgr globalStateMgr, Database db) {
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getLocalMetastore().getDb(anyLong);
                result = db;

                globalStateMgr.getCurrentState().getLocalMetastore().getTable(anyLong, anyLong);
                result = mv;

                db.getId();
                result = anyLong;

                mv.getId();
                result = anyLong;
            }
        };
        new AlterJobMgr(null, null, null).replayChangeMaterializedViewRefreshScheme(log);
    }

    // An external-table-only MV leaves the OLAP version map empty; replay must not read that as "now".
    @Test
    public void testLastRefreshTimeSurvivesReplayWhenOlapVersionMapIsEmpty(@Mocked GlobalStateMgr globalStateMgr,
                                                                          @Injectable Database db) throws IOException {
        long historicalRefreshTime = 1735689600000L; // 2025-01-01T00:00:00Z
        long beforeReplay = System.currentTimeMillis();

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(historicalRefreshTime);
        Assertions.assertTrue(refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        MaterializedView materializedView = buildMv(refreshScheme);

        ChangeMaterializedViewRefreshSchemeLog readChangeLog = writeAndRead(materializedView, false);
        Assertions.assertTrue(readChangeLog.getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        replay(readChangeLog, materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        long afterReplay = System.currentTimeMillis();
        System.out.printf("[repro] expected lastRefreshTime=%d, observed=%d, replay window=[%d, %d], "
                        + "observedIsWallClockNow=%b%n",
                historicalRefreshTime, replayed, beforeReplay, afterReplay,
                replayed >= beforeReplay && replayed <= afterReplay);
        Assertions.assertEquals(historicalRefreshTime, replayed,
                "replay must not recompute lastRefreshTime as wall-clock now when the OLAP version map is empty");
    }

    // The version map accumulates every partition ever refreshed, so its max can be newer than this run's.
    @Test
    public void testLastRefreshTimeSurvivesReplayWhenOlapVersionMapHasNewerPartition(
            @Mocked GlobalStateMgr globalStateMgr, @Injectable Database db) throws IOException {
        long refreshedPartitionTime = 1735689600000L;  // what this run refreshed: 2025-01-01T00:00:00Z
        long newerPartitionTime = 1767225600000L;      // already in the accumulated map: 2026-01-01T00:00:00Z

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(refreshedPartitionTime);
        Map<String, MaterializedView.BasePartitionInfo> accumulated = partitionInfo("p1", refreshedPartitionTime);
        accumulated.put("p3", new MaterializedView.BasePartitionInfo(3, 2, newerPartitionTime));
        refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().put(1000L, accumulated);
        MaterializedView materializedView = buildMv(refreshScheme);

        replay(writeAndRead(materializedView, false), materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        System.out.printf("[repro-olap] expected lastRefreshTime=%d, observed=%d, "
                        + "observedIsAccumulatedMax=%b%n",
                refreshedPartitionTime, replayed, replayed == newerPartitionTime);
        Assertions.assertEquals(refreshedPartitionTime, replayed,
                "replay must not recompute lastRefreshTime from the accumulated version map");
    }

    // An external-table-only MV records its versions in the external map, the one the old derivation
    // never looked at.
    @Test
    public void testPreUpgradeLogDerivesFromExternalVersionMap(@Mocked GlobalStateMgr globalStateMgr,
                                                               @Injectable Database db) throws IOException {
        long externalRefreshTime = 1782405968232000L; // iceberg micros
        long beforeReplay = System.currentTimeMillis();

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(0L);
        refreshScheme.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                .put(new BaseTableInfo("iceberg_catalog", "db", "tbl", "tbl:1"),
                        partitionInfo("p1", externalRefreshTime));
        MaterializedView materializedView = buildMv(refreshScheme);

        ChangeMaterializedViewRefreshSchemeLog readChangeLog = writeAndRead(materializedView, true);
        Assertions.assertNull(readChangeLog.getLastRefreshTime());
        Assertions.assertTrue(readChangeLog.getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        replay(readChangeLog, materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        long afterReplay = System.currentTimeMillis();
        System.out.printf("[legacy-external] expected lastRefreshTime=%d, observed=%d, "
                        + "observedIsWallClockNow=%b%n",
                externalRefreshTime, replayed, replayed >= beforeReplay && replayed <= afterReplay);
        Assertions.assertEquals(externalRefreshTime, replayed,
                "replay of a pre-upgrade log must derive from the external version map, not the wall clock");
    }

    // The maps get cleared and pruned, so a derivation alone can come out lower than what the MV absorbed.
    @Test
    public void testPreUpgradeLogNeverMovesLastRefreshTimeBackwards(@Mocked GlobalStateMgr globalStateMgr,
                                                                    @Injectable Database db) throws IOException {
        long absorbedRefreshTime = 1767225600000L;  // already in memory: 2026-01-01T00:00:00Z
        long prunedMapRefreshTime = 1735689600000L; // all the pruned map still proves: 2025-01-01T00:00:00Z

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(absorbedRefreshTime);
        refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                .put(1000L, partitionInfo("p1", prunedMapRefreshTime));
        MaterializedView materializedView = buildMv(refreshScheme);

        ChangeMaterializedViewRefreshSchemeLog readChangeLog = writeAndRead(materializedView, true);
        Assertions.assertNull(readChangeLog.getLastRefreshTime());
        replay(readChangeLog, materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        System.out.printf("[legacy-monotonic] expected lastRefreshTime=%d, observed=%d, observedIsDerived=%b%n",
                absorbedRefreshTime, replayed, replayed == prunedMapRefreshTime);
        Assertions.assertEquals(absorbedRefreshTime, replayed,
                "replay of a pre-upgrade log must not regress below the value the MV already absorbed");
    }

    // A mixed MV (internal join external) is the only shape that needs BOTH maps read: dropping either
    // one silently loses half the base tables. Iceberg micros dominate OLAP millis, as they do for the leader.
    @Test
    public void testPreUpgradeLogDerivesFromBothVersionMaps(@Mocked GlobalStateMgr globalStateMgr,
                                                            @Injectable Database db) throws IOException {
        long olapRefreshTime = 1735689600000L;        // millis
        long externalRefreshTime = 1782405968232000L; // iceberg micros, numerically larger

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(0L);
        refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                .put(1000L, partitionInfo("p1", olapRefreshTime));
        refreshScheme.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap()
                .put(new BaseTableInfo("iceberg_catalog", "db", "tbl", "tbl:1"),
                        partitionInfo("p1", externalRefreshTime));
        MaterializedView materializedView = buildMv(refreshScheme);

        ChangeMaterializedViewRefreshSchemeLog readChangeLog = writeAndRead(materializedView, true);
        Assertions.assertNull(readChangeLog.getLastRefreshTime());
        Assertions.assertFalse(readChangeLog.getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        Assertions.assertFalse(readChangeLog.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().isEmpty());
        replay(readChangeLog, materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        System.out.printf("[legacy-both] olap=%d external=%d observed=%d observedIsOlapOnly=%b%n",
                olapRefreshTime, externalRefreshTime, replayed, replayed == olapRefreshTime);
        Assertions.assertEquals(externalRefreshTime, replayed,
                "replay of a pre-upgrade log must max over both version maps, not just one");
    }

    // Nothing to derive from at all: the value in memory is the only thing left that is not a guess.
    @Test
    public void testPreUpgradeLogKeepsMemoryValueWhenBothMapsAreEmpty(@Mocked GlobalStateMgr globalStateMgr,
                                                                      @Injectable Database db) throws IOException {
        long absorbedRefreshTime = 1767225600000L;
        long beforeReplay = System.currentTimeMillis();

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setLastRefreshTime(absorbedRefreshTime);
        MaterializedView materializedView = buildMv(refreshScheme);

        ChangeMaterializedViewRefreshSchemeLog readChangeLog = writeAndRead(materializedView, true);
        Assertions.assertNull(readChangeLog.getLastRefreshTime());
        Assertions.assertTrue(readChangeLog.getAsyncRefreshContext().getBaseTableVisibleVersionMap().isEmpty());
        Assertions.assertTrue(readChangeLog.getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap().isEmpty());
        replay(readChangeLog, materializedView, globalStateMgr, db);

        long replayed = materializedView.getRefreshScheme().getLastRefreshTime();
        long afterReplay = System.currentTimeMillis();
        System.out.printf("[legacy-empty] expected=%d observed=%d observedIsWallClockNow=%b observedIsZero=%b%n",
                absorbedRefreshTime, replayed, replayed >= beforeReplay && replayed <= afterReplay, replayed == 0L);
        Assertions.assertEquals(absorbedRefreshTime, replayed,
                "replay of a pre-upgrade log with nothing to derive from must keep the in-memory value");
    }
}
