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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.persist.gson.GsonUtils.GSON;
import static com.starrocks.statistic.StatsConstants.INIT_SAMPLE_STATS_JOB;

public class BasicStatsMetaTest extends PlanTestBase {

    @BeforeEach
    public void before() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testHealthy() {
        {
            // total row in cached table statistic is 6, the updated row is 100.
            Database db =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(new ConnectContext(), "default_catalog", "test");
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(new ConnectContext(), "default_catalog", "test", "region");
            List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());
            new Expectations(partitions.get(0)) {
                {
                    partitions.get(0).getRowCount();
                    result = 100L;
                }
            };
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                    StatsConstants.AnalyzeType.FULL,
                    LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 100);
            Assertions.assertEquals(0.05, basicStatsMeta.getHealthy(), 0.01);
        }

        {
            // total row in cached table statistic is 10000, the updated row is 10000, the delta row is 5000.
            Database db =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(new ConnectContext(), "default_catalog", "test");
            Table tbl =
                    GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .getTable(new ConnectContext(), "default_catalog", "test", "supplier");
            List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());
            new Expectations(partitions.get(0)) {
                {
                    partitions.get(0).getRowCount();
                    result = 10000L;
                }
            };
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                    StatsConstants.AnalyzeType.FULL,
                    LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 10000);
            basicStatsMeta.increaseDeltaRows(5000L);
            basicStatsMeta.setTotalRows(10000L);
            Assertions.assertEquals(0.5, basicStatsMeta.getHealthy(), 0.01);
            basicStatsMeta.resetDeltaRows();
            Assertions.assertEquals(1.0, basicStatsMeta.getHealthy(), 0.01);

            basicStatsMeta.setProperties(ImmutableBiMap.of(INIT_SAMPLE_STATS_JOB, "true"));
            basicStatsMeta.increaseDeltaRows(5000L);
            basicStatsMeta.setTotalRows(10000L);
            Assertions.assertEquals(0.5, basicStatsMeta.getHealthy(), 0.01);
        }
    }

    @Test
    public void testHealthyWithUpdateModifiedRows() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime statsUpdateTime = now.minusHours(1);
        LocalDateTime tabletStatsReportTime = now.minusMinutes(30); // Before table update time

        // Mock StatisticUtils to control table update time
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return now; // Table was updated after tablet stats report time
            }
           
            @Mock
            public boolean isPartitionStatsHealthy(Partition partition, BasicStatsMeta stats, long statsRowCount) {
                return true; // All partitions are healthy
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(new ConnectContext(), "default_catalog", "test");
        Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", "test", "supplier");
        List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());
        Partition partition = partitions.get(0);

        // Mock partition row count - need to allow multiple invocations
        new Expectations(partition) {
            {
                partition.getRowCount();
                result = 10000L;
                minTimes = 0; // Allow any number of calls
            }
        };

        // Mock statistic storage to return cached row count
        StatisticStorage statisticStorage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        Map<Long, Optional<Long>> tableStatsMap = Maps.newHashMap();
        tableStatsMap.put(partition.getId(), Optional.of(10000L));
        new Expectations(statisticStorage) {
            {
                statisticStorage.getTableStatistics(tbl.getId(), tbl.getPartitions());
                result = tableStatsMap;
                minTimes = 0; // Allow any number of calls
            }
        };

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                StatsConstants.AnalyzeType.FULL, statsUpdateTime, Map.of(), 10000);
        // Manually set tabletStatsReportTime to ensure it's before tableUpdateTime
        Deencapsulation.setField(basicStatsMeta, "tabletStatsReportTime", tabletStatsReportTime);

        {
            // Test case 1: Only UPDATE operations, no INSERT/DELETE
            // Table has 10000 rows, cached stats also has 10000 rows (no row count change)
            // 5000 rows were updated via UPDATE statement
            // updatePartitionRowCountForCalc ≈ 1 (minimal, since row count didn't change)
            // totalChangedRows = 1 + 5000 = 5001
            // tableRowCount = 1 + 10000 = 10001
            // updateRatio = 5001 / 10001 ≈ 0.5
            // Expected: health ≈ 0.5
            basicStatsMeta.resetDeltaRows(); // No INSERT/DELETE
            basicStatsMeta.increaseUpdateModifiedRows(5000L); // 5000 rows updated
            basicStatsMeta.setTotalRows(10000L);

            double health = basicStatsMeta.getHealthy();
            Assertions.assertTrue(health >= 0.49 && health <= 0.51,
                    "Health should be around 0.5 when 50% of rows are updated via UPDATE, actual: " + health);
        }

        {
            // Test case 2: UPDATE operations with smaller ratio
            // Table has 10000 rows, 2000 rows were updated
            // updatePartitionRowCountForCalc ≈ 1
            // totalChangedRows = 1 + 2000 = 2001
            // updateRatio = 2001 / 10001 ≈ 0.2
            // Expected: health ≈ 0.8
            basicStatsMeta.resetUpdateModifiedRows();
            basicStatsMeta.increaseUpdateModifiedRows(2000L);

            double health = basicStatsMeta.getHealthy();
            Assertions.assertTrue(health >= 0.79 && health <= 0.81,
                    "Health should be around 0.8 when 20% of rows are updated via UPDATE, actual: " + health);
        }

        {
            // Test case 3: No UPDATE operations, but table was updated after stats collection
            // updatePartitionRowCountForCalc ≈ 1 (minimal change)
            // totalChangedRows = 1 + 0 = 1
            // updateRatio = 1 / 10001 ≈ 0.0001
            // Expected: health ≈ 1.0 (very close to 1.0)
            basicStatsMeta.resetUpdateModifiedRows();
            basicStatsMeta.resetDeltaRows();
            basicStatsMeta.setTotalRows(10000L);

            double health = basicStatsMeta.getHealthy();
            Assertions.assertTrue(health >= 0.99,
                    "Health should be close to 1.0 when there are minimal updates, actual: " + health);
        }

        {
            // Test case 4: UPDATE operations combined with INSERT/DELETE
            // Table has 10000 rows, 3000 rows inserted/deleted, 2000 rows updated
            // Expected: health = 1 - ((3000 + 2000) / 10000) = 0.5
            basicStatsMeta.increaseDeltaRows(3000L); // 3000 rows inserted/deleted
            basicStatsMeta.increaseUpdateModifiedRows(2000L); // 2000 rows updated
            basicStatsMeta.setTotalRows(13000L); // New total after insert

            double health = basicStatsMeta.getHealthy();
            // updatePartitionRowCountForCalc = max(1, max(13000 + 3000, 13000) - 10000) = 6000
            // totalChangedRows = 6000 + 2000 = 8000
            // But tableRowCount = 10000 + 1 = 10001 (from partition.getRowCount() + 1)
            // Actually, let's recalculate: tableRowCount comes from partition.getRowCount() which is 10000
            // So tableRowCount = 10000 + 1 = 10001
            // updatePartitionRowCountForCalc = max(1, max(10001 + 3000, 13000) - 10000) = max(1, 13000 - 10000) = 3000
            // totalChangedRows = 3000 + 2000 = 5000
            // updateRatio = 5000 / 10001 ≈ 0.5
            // health = 1 - 0.5 = 0.5
            Assertions.assertTrue(health < 0.6 && health > 0.4,
                    "Health should be around 0.5 when both INSERT/DELETE and UPDATE occur");
        }

        {
            // Test case 5: Large UPDATE operations (more than table size)
            // Table has 10000 rows, 15000 rows were updated (should be capped at 1.0)
            // updatePartitionRowCountForCalc ≈ 1
            // totalChangedRows = 1 + 15000 = 15001
            // updateRatio = 15001 / 10001 ≈ 1.5, capped at 1.0
            // Expected: health = 1 - 1.0 = 0.0
            basicStatsMeta.resetDeltaRows();
            basicStatsMeta.resetUpdateModifiedRows();
            basicStatsMeta.setTotalRows(10000L);
            basicStatsMeta.increaseUpdateModifiedRows(15000L);

            double health = basicStatsMeta.getHealthy();
            Assertions.assertTrue(health <= 0.01,
                    "Health should be 0.0 when update ratio exceeds 1.0, actual: " + health);
        }
    }

    @Test
    public void testSerialization() throws IOException {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(new ConnectContext(), "default_catalog", "test");
        Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", "test", "region");
        {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            String s = "{\"dbId\":" + db.getId() +
                    ",\"tableId\":" + tbl.getId() + ",\"columns\":[],\"type\":\"FULL\",\"updateTime\":1721650800," +
                    "\"properties\":{},\"updateRows\":10000}";
            Text.writeString(dataOutputStream, s);

            byte[] bytes = byteArrayOutputStream.toByteArray();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
            String deserializedString = Text.readString(dataInputStream);
            BasicStatsMeta deserializedMeta = GSON.fromJson(deserializedString, BasicStatsMeta.class);
            Assertions.assertEquals(db.getId(), deserializedMeta.getDbId());

        }

        {
            BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                    StatsConstants.AnalyzeType.FULL,
                    LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 10000);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            String s = GSON.toJson(basicStatsMeta);
            Text.writeString(dataOutputStream, s);
            dataOutputStream.close();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
            String deserializedString = Text.readString(dataInputStream);
            BasicStatsMetaDemo deserializedMeta = GSON.fromJson(deserializedString, BasicStatsMetaDemo.class);
            Assertions.assertEquals(db.getId(), deserializedMeta.dbId);
        }
    }

    @AfterEach
    public void after() {
        FeConstants.runningUnitTest = false;
    }

    private static class BasicStatsMetaDemo {
        @SerializedName("dbId")
        public long dbId;

        @SerializedName("tableId")
        public long tableId;

        @SerializedName("columns")
        public List<String> columns;

        @SerializedName("type")
        public StatsConstants.AnalyzeType type;

        @SerializedName("updateTime")
        public LocalDateTime updateTime;

        @SerializedName("properties")
        public Map<String, String> properties;

        @SerializedName("updateRows")
        public long updateRows;
    }

}
