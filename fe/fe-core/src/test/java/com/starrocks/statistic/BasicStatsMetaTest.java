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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.persist.gson.GsonUtils.GSON;
import static com.starrocks.statistic.StatsConstants.INIT_SAMPLE_STATS_JOB;

public class BasicStatsMetaTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `stats_health_parts` (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `dt` date NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY RANGE(`dt`) (\n" +
                "  PARTITION p1 VALUES LESS THAN (\"2024-02-01\"),\n" +
                "  PARTITION p2 VALUES LESS THAN (\"2024-03-01\"),\n" +
                "  PARTITION p3 VALUES LESS THAN (\"2024-04-01\"),\n" +
                "  PARTITION p4 VALUES LESS THAN (\"2024-05-01\"),\n" +
                "  PARTITION p5 VALUES LESS THAN (\"2024-06-01\"),\n" +
                "  PARTITION p6 VALUES LESS THAN (\"2024-07-01\"),\n" +
                "  PARTITION p7 VALUES LESS THAN (\"2024-08-01\"),\n" +
                "  PARTITION p8 VALUES LESS THAN (\"2024-09-01\"),\n" +
                "  PARTITION p9 VALUES LESS THAN (\"2024-10-01\"),\n" +
                "  PARTITION p10 VALUES LESS THAN (\"2024-11-01\"),\n" +
                "  PARTITION p11 VALUES LESS THAN (\"2024-12-01\"),\n" +
                "  PARTITION p12 VALUES LESS THAN (\"2025-01-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

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
    public void testHealthyWithManyStalePartitions() {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(new ConnectContext(), "default_catalog", "test");
        Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(new ConnectContext(), "default_catalog", "test", "stats_health_parts");
        List<Partition> partitions = Lists.newArrayList(tbl.getPartitions());

        new MockUp<Partition>() {
            @Mock
            public boolean hasData() {
                return true;
            }

            @Mock
            public long getRowCount() {
                return 100000L;
            }
        };

        Map<Long, Optional<Long>> tableStatistics = Maps.newHashMap();
        for (Partition partition : partitions) {
            tableStatistics.put(partition.getId(), Optional.empty());
        }
        tableStatistics.put(partitions.get(0).getId(), Optional.of(100000L));

        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        new Expectations(storage) {
            {
                storage.getTableStatistics(tbl.getId(), (Collection<Partition>) any);
                result = tableStatistics;
            }
        };

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db.getId(), tbl.getId(), List.of(),
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2024, 07, 22, 12, 20), Map.of(), 1200000);
        double healthy = basicStatsMeta.getHealthy();
        Assertions.assertTrue(healthy < 0.5,
                "health must be low when most rows sit in stale partitions, but was " + healthy);
        Assertions.assertEquals(0.08, healthy, 0.01);
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
