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

package com.starrocks.sql.plan;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpSerializer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Replays a query dump over an automatically (expression) partitioned table. The CREATE TABLE of such a table
 * omits its concrete partitions, so before this fix the replayed table had zero partitions, partition pruning
 * saw nothing, and the OlapScanNode cardinality collapsed to the empty-table default (1) even though the dump
 * recorded per-partition row counts.
 *
 * The dump now carries a "partition_values" section (one representative value per concrete partition), which
 * replay uses to recreate the partitions (UtFrameUtils.initMockEnv via
 * AnalyzerUtils.getAddPartitionClauseFromPartitionValues). The per-partition row counts then match by name, so
 * pruning and cardinality come out the same as for an equivalent explicit-range-partitioned table.
 *
 * The dump was captured from a real cluster:
 *   CREATE TABLE t_expr (id int, dt date, v int) PARTITION BY date_trunc('day', dt) ...  -- 3 daily partitions
 *   query: select v from t_expr where dt &gt;= '2023-01-02'   -- prunes to p20230102(1 row) + p20230103(3 rows)
 */
public class QueryDumpAutoPartitionReplayTest extends ReplayFromDumpTestBase {

    @Test
    public void testReplayAutomaticRangePartition() throws Exception {
        Pair<QueryDumpInfo, String> replayPair =
                getCostPlanFragment(getDumpInfoFromFile("query_dump/auto_partition_expr"));
        String plan = replayPair.second;

        // The three concrete partitions are recreated, so the dt >= '2023-01-02' predicate prunes to 2 of 3.
        Assertions.assertTrue(plan.contains("partitionsRatio=2/3"),
                "expected 2/3 partitions after recreation and pruning, plan:\n" + plan);
        // Cardinality reflects the recovered per-partition row counts (p20230102=1 + p20230103=3), not the
        // empty-table default of 1.
        Assertions.assertTrue(plan.contains("cardinality: 4"),
                "expected scan cardinality 4 from recovered partition row counts, plan:\n" + plan);
    }

    // Replays the dump and asserts the recreated partitions drive the expected pruning and scan cardinality.
    private void assertReplayScan(String resource, String partitionsRatio, String cardinality) throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile(resource));
        String plan = replayPair.second;
        Assertions.assertTrue(plan.contains("partitionsRatio=" + partitionsRatio),
                "expected partitionsRatio=" + partitionsRatio + " after partition recreation, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("cardinality: " + cardinality),
                "expected scan cardinality " + cardinality + " from recovered partition row counts, plan:\n" + plan);
    }

    @Test
    public void testReplayDatetimeExpressionPartition() throws Exception {
        // date_trunc('day', <datetime>): values round-trip as "yyyy-MM-dd HH:mm:ss"; prune to p20230302(1)+p20230303(2).
        assertReplayScan("query_dump/auto_partition_datetime", "2/3", "3");
    }

    @Test
    public void testReplayMonthGranularityPartition() throws Exception {
        // date_trunc('month', <date>): month-boundary values; prune to p202302(1)+p202303(3).
        assertReplayScan("query_dump/auto_partition_month", "2/3", "4");
    }

    @Test
    public void testReplayAutomaticListPartition() throws Exception {
        // automatic list partitioning on a string column: values are the raw list values; prune to pguangzhou(3).
        assertReplayScan("query_dump/auto_partition_list", "1/3", "3");
    }

    @Test
    public void testSerializeCapturesAutomaticPartitionValuesExcludingShadow() throws Exception {
        connectContext.setThreadLocalInfo();
        starRocksAssert.withDatabase("auto_part_ser_db").useDatabase("auto_part_ser_db");
        starRocksAssert.withTable("CREATE TABLE t_auto (id int, dt date) DUPLICATE KEY(id) "
                + "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        // batch ADD PARTITIONS is allowed on automatic-partition tables; create two real partitions
        // (p20230101, p20230102) alongside the hidden shadow partition.
        starRocksAssert.alterTable("ALTER TABLE auto_part_ser_db.t_auto ADD PARTITIONS "
                + "START (\"2023-01-01\") END (\"2023-01-03\") EVERY (INTERVAL 1 DAY)");
        Table table = starRocksAssert.getTable("auto_part_ser_db", "t_auto");

        QueryDumpInfo dumpInfo = new QueryDumpInfo(connectContext);
        dumpInfo.addTable("auto_part_ser_db", table);
        dumpInfo.setOriginStmt("select * from auto_part_ser_db.t_auto");
        dumpInfo.setStatement(UtFrameUtils.parseStmtWithNewParser(
                "select * from auto_part_ser_db.t_auto", connectContext));

        connectContext.setThreadLocalInfo();
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpSerializer())
                .create();
        String dump = gson.toJson(dumpInfo, QueryDumpInfo.class);

        JsonObject json = JsonParser.parseString(dump).getAsJsonObject();
        Assertions.assertTrue(json.has("partition_values"), "dump should carry partition_values:\n" + dump);
        JsonArray captured = json.getAsJsonObject("partition_values").getAsJsonArray("auto_part_ser_db.t_auto");
        // the two real partitions are captured...
        Assertions.assertEquals(2, captured.size(), "expected the two real partitions captured, dump:\n" + dump);
        // ...and the hidden shadow partition ($shadow_automatic_partition, bound "0000-...") is excluded.
        Assertions.assertFalse(dump.contains("0000-"), "shadow partition value must not be captured:\n" + dump);
    }

    @Test
    public void testDesensitizedDumpKeepsPartitionValues() throws Exception {
        connectContext.setThreadLocalInfo();
        starRocksAssert.withDatabase("auto_part_desen_db").useDatabase("auto_part_desen_db");
        starRocksAssert.withTable("CREATE TABLE t_auto2 (id int, dt date) DUPLICATE KEY(id) "
                + "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        starRocksAssert.alterTable("ALTER TABLE auto_part_desen_db.t_auto2 ADD PARTITIONS "
                + "START (\"2023-01-01\") END (\"2023-01-03\") EVERY (INTERVAL 1 DAY)");
        Table table = starRocksAssert.getTable("auto_part_desen_db", "t_auto2");

        QueryDumpInfo dumpInfo = new QueryDumpInfo(connectContext);
        dumpInfo.addTable("auto_part_desen_db", table);
        dumpInfo.setOriginStmt("select * from auto_part_desen_db.t_auto2");
        dumpInfo.setStatement(UtFrameUtils.parseStmtWithNewParser(
                "select * from auto_part_desen_db.t_auto2", connectContext));
        dumpInfo.setDesensitizedInfo(true);

        connectContext.setThreadLocalInfo();
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpSerializer())
                .create();
        String dump = gson.toJson(dumpInfo, QueryDumpInfo.class);

        JsonObject json = JsonParser.parseString(dump).getAsJsonObject();
        // A desensitized dump of an automatic-partition table must still carry partition_values, otherwise it
        // would replay with zero concrete partitions and wrong pruning/cardinality. The section is emitted on
        // both serializer paths: the desensitized path adds it, and if desensitization bails out (e.g. the
        // date_trunc('day', ...) expression currently can't be desensitized) the plain fall-back path adds it too.
        Assertions.assertTrue(json.has("partition_values"),
                "desensitized dump should still carry partition_values:\n" + dump);
    }
}
