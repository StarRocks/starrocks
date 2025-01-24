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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Expectations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.starrocks.persist.gson.GsonUtils.GSON;

public class BasicStatsMetaTest extends PlanTestBase {

    @Before
    public void before() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testHealthy() {
        {
            // total row in cached table statistic is 6, the updated row is 100.
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "test");
            Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "test", "region");
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
            Assert.assertEquals(0.05, basicStatsMeta.getHealthy(), 0.01);
        }

        {
            // total row in cached table statistic is 10000, the updated row is 10000, the delta row is 5000.
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "test");
            Table tbl =
                    GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "test", "supplier");
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
            basicStatsMeta.setUpdateRows(10000L);
            Assert.assertEquals(1.0, basicStatsMeta.getHealthy(), 0.01);
            basicStatsMeta.resetDeltaRows();
            Assert.assertEquals(1.0, basicStatsMeta.getHealthy(), 0.01);
        }
    }

    @Test
    public void testSerialization() throws IOException {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "test");
        Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "test", "region");
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
            Assert.assertEquals(db.getId(), deserializedMeta.getDbId());

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
            Assert.assertEquals(db.getId(), deserializedMeta.dbId);
        }
    }

    @After
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
