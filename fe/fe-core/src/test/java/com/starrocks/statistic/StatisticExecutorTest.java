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
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TStatisticData;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

public class StatisticExecutorTest extends PlanTestBase {
    @Test
    public void testEmpty() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(db.getId(), olapTable.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        Assertions.assertThrows(SemanticException.class,
                () -> statisticExecutor.queryStatisticSync(
                        StatisticUtils.buildConnectContext(), db.getId(), olapTable.getId(), Lists.newArrayList("foo", "bar")));
    }

    @Test
    public void testDroppedDB() throws Exception {
        StatisticExecutor statisticExecutor = new StatisticExecutor();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addBasicStatsMeta(new BasicStatsMeta(db.getId(), 1000, null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1, 1),
                Maps.newHashMap()));

        List<TStatisticData> stats = statisticExecutor.queryStatisticSync(
                StatisticUtils.buildConnectContext(), null, 1000L, Lists.newArrayList("foo", "bar"));
        Assertions.assertEquals(0, stats.size());
    }

    @Test
    public void testExecuteWideRowDQL() {
        String json1 = "{\"data\": [100, \"string_val\", 0, 10, 1]}";
        String json2 = "{\"data\": [200, \"another\", 1, 20, 2]}";
        TResultBatch batch = new TResultBatch();
        batch.setRows(Lists.newArrayList(
                ByteBuffer.wrap(json1.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(json2.getBytes(StandardCharsets.UTF_8))));
        batch.setIs_compressed(false);
        batch.setPacket_seq(0);

        List<TResultBatch> mockResult = Lists.newArrayList(batch);

        new MockUp<StatisticExecutor>() {
            @Mock
            List<TResultBatch> executeDQL(ConnectContext context, String sql,
                                                    TResultSinkType sinkType) {
                return mockResult;
            }
        };

        StatisticExecutor executor = new StatisticExecutor();
        List<JsonArray> rows = executor.executeWideRowDQL(
                StatisticUtils.buildConnectContext(), "SELECT 1");

        Assertions.assertEquals(2, rows.size());

        JsonArray row0 = rows.get(0);
        Assertions.assertEquals(5, row0.size());
        Assertions.assertEquals(100, row0.get(0).getAsInt());
        Assertions.assertEquals("string_val", row0.get(1).getAsString());
        Assertions.assertEquals(0, row0.get(2).getAsInt());
        Assertions.assertEquals(10, row0.get(3).getAsInt());
        Assertions.assertEquals(1, row0.get(4).getAsInt());

        JsonArray row1 = rows.get(1);
        Assertions.assertEquals(5, row1.size());
        Assertions.assertEquals(200, row1.get(0).getAsInt());
        Assertions.assertEquals("another", row1.get(1).getAsString());
    }
}
