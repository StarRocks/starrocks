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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpSerializer;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Replays real query dumps captured from a cluster with {@code mock=false} on columns that carry a histogram.
 * It guards the histogram round-trip end to end: the cluster serializes the histogram into the
 * {@code column_histogram} section (QueryDumpSerializer), and replay here reconstructs it
 * (QueryDumpDeserializer + HistogramUtils) and injects it back into the statistics storage
 * (UtFrameUtils.initMockEnv) so it drives cardinality estimation.
 *
 * Three columns of different types are covered so both encodings and both histogram shapes are exercised:
 *   - DATE  (l_shipdate): buckets whose bounds are long-encoded timestamps stored as double, plus MCV.
 *   - BIGINT(l_partkey) : buckets whose bounds are plain numeric doubles, plus MCV.
 *   - CHAR  (l_shipmode): MCV-only histogram with no buckets (string values as MCV keys).
 *
 * Each dump was produced by:
 *   ANALYZE TABLE tpch_100g.lineitem UPDATE HISTOGRAM ON &lt;col&gt; WITH 32 BUCKETS;
 *   POST /api/query_dump?db=tpch_100g&amp;mock=false -d "select count(*) from lineitem where &lt;pred on col&gt;"
 * and the exact bucket/MCV values asserted below are read straight from those frozen dump files.
 */
public class QueryDumpHistogramReplayTest extends ReplayFromDumpTestBase {

    // Replays the dump, asserts it plans (scans lineitem), and returns the reconstructed histogram.
    private Histogram replayHistogram(String dumpResource, String column) throws Exception {
        String dumpString = getDumpInfoFromFile(dumpResource);
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpString);
        Assertions.assertTrue(replayPair.second.toLowerCase().contains("lineitem"),
                "replayed cost plan should scan lineitem:\n" + replayPair.second);
        ColumnStatistic columnStatistic =
                replayPair.first.getTableStatisticsMap().get("tpch_100g.lineitem").get(column);
        Assertions.assertNotNull(columnStatistic, column + " column statistic should exist after replay");
        Histogram histogram = columnStatistic.getHistogram();
        Assertions.assertNotNull(histogram, column + " histogram should be reconstructed from column_histogram");
        return histogram;
    }

    @Test
    public void testReplayDateHistogram() throws Exception {
        Histogram histogram = replayHistogram("query_dump/histogram_lineitem_shipdate", "l_shipdate");

        Assertions.assertEquals(32, histogram.getBuckets().size());
        Bucket first = histogram.getBuckets().get(0);
        // DATE bounds round-trip as the long-encoded timestamp cast to double.
        Assertions.assertEquals(6.97392E8, first.getLower());
        Assertions.assertEquals(7.028352E8, first.getUpper());
        Assertions.assertEquals(17630197L, first.getCount().longValue());
        Assertions.assertEquals(5098043L, first.getUpperRepeats().longValue());

        Assertions.assertEquals(100, histogram.getMCV().size());
        Assertions.assertEquals(1933322L, histogram.getMCV().get("1993-12-03").longValue());
    }

    @Test
    public void testReplayNumericHistogram() throws Exception {
        Histogram histogram = replayHistogram("query_dump/histogram_lineitem_partkey", "l_partkey");

        Assertions.assertEquals(32, histogram.getBuckets().size());
        Bucket first = histogram.getBuckets().get(0);
        // BIGINT bounds round-trip as plain numeric doubles.
        Assertions.assertEquals(1.0, first.getLower());
        Assertions.assertEquals(614061.0, first.getUpper());
        Assertions.assertEquals(18751188L, first.getCount().longValue());
        Assertions.assertEquals(60L, first.getUpperRepeats().longValue());

        Assertions.assertEquals(100, histogram.getMCV().size());
        Assertions.assertEquals(360L, histogram.getMCV().get("13248558").longValue());
    }

    @Test
    public void testReplayStringMcvOnlyHistogram() throws Exception {
        Histogram histogram = replayHistogram("query_dump/histogram_lineitem_shipmode", "l_shipmode");

        // A low-cardinality string column produces an MCV-only histogram with no buckets.
        Assertions.assertTrue(histogram.getBuckets().isEmpty(), "string histogram should have no buckets");
        Assertions.assertEquals(7, histogram.getMCV().size());
        Assertions.assertEquals(87666458L, histogram.getMCV().get("MAIL").longValue());
    }

    @Test
    public void testReplayMixedColumnsSomeWithoutHistogram() throws Exception {
        // The dump's column_statistics carries both l_shipdate and l_orderkey, but column_histogram carries
        // only l_shipdate (l_orderkey has no histogram collected). Replay must attach the histogram to
        // l_shipdate and leave l_orderkey untouched -- the has()/null guards in QueryDumpDeserializer handle
        // the histogram section being a subset of the statistics section, with no error.
        String dumpString = getDumpInfoFromFile("query_dump/histogram_lineitem_mixed");
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(dumpString);
        Assertions.assertTrue(replayPair.second.toLowerCase().contains("lineitem"),
                "replayed cost plan should scan lineitem:\n" + replayPair.second);

        Map<String, ColumnStatistic> lineitemStats =
                replayPair.first.getTableStatisticsMap().get("tpch_100g.lineitem");

        // The column that has a histogram gets it reconstructed.
        ColumnStatistic shipDate = lineitemStats.get("l_shipdate");
        Assertions.assertNotNull(shipDate, "l_shipdate column statistic should exist");
        Assertions.assertNotNull(shipDate.getHistogram(), "l_shipdate should have a histogram");
        Assertions.assertEquals(32, shipDate.getHistogram().getBuckets().size());

        // The column without a histogram keeps its base statistic and carries no histogram -- no error.
        ColumnStatistic orderKey = lineitemStats.get("l_orderkey");
        Assertions.assertNotNull(orderKey, "l_orderkey column statistic should exist");
        Assertions.assertNull(orderKey.getHistogram(), "l_orderkey should have no histogram");
    }

    @Test
    public void testDesensitizedDumpDoesNotLeakRawStatisticValues() throws Exception {
        // A histogram's MCV and string min/max fields hold raw column values. On the desensitized dump path
        // they must not reach the output: column_histogram is skipped, and column_statistics strips those
        // raw-value fields before writing the dump DTO.
        connectContext.setThreadLocalInfo();
        starRocksAssert.withDatabase("hist_leak_db").useDatabase("hist_leak_db");
        starRocksAssert.withTable("CREATE TABLE t_hist (k1 int, k2 date) DUPLICATE KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num'='1')");
        Table table = starRocksAssert.getTable("hist_leak_db", "t_hist");

        Histogram histogram = new Histogram(Lists.newArrayList(new Bucket(1.0, 10.0, 100L, 5L)),
                ImmutableMap.of("SENSITIVE_MCV_VALUE", 50L));
        ColumnStatistic stat = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4)
                .setDistinctValuesCount(10).setHistogram(histogram)
                .setMinString("SENSITIVE_MIN_STRING")
                .setMaxString("SENSITIVE_MAX_STRING")
                .build();

        QueryDumpInfo dumpInfo = new QueryDumpInfo(connectContext);
        dumpInfo.addTable("hist_leak_db", table);
        dumpInfo.setOriginStmt("select * from hist_leak_db.t_hist");
        // Analyze the statement so the desensitizer's collector can resolve table/column names.
        dumpInfo.setStatement(UtFrameUtils.parseStmtWithNewParser("select * from hist_leak_db.t_hist", connectContext));
        dumpInfo.addTableStatistics(table, "k1", stat);

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpSerializer())
                .create();

        // Non-desensitized dump: the full histogram (including its MCV) is emitted in column_histogram.
        String normalDump = gson.toJson(dumpInfo, QueryDumpInfo.class);
        Assertions.assertTrue(normalDump.contains("column_histogram"),
                "non-desensitized dump should carry column_histogram");
        Assertions.assertTrue(normalDump.contains("SENSITIVE_MCV_VALUE"),
                "non-desensitized dump carries the real MCV value");
        Assertions.assertTrue(normalDump.contains("SENSITIVE_MIN_STRING"),
                "non-desensitized dump preserves raw minString values for replay");
        Assertions.assertTrue(normalDump.contains("SENSITIVE_MAX_STRING"),
                "non-desensitized dump preserves raw maxString values for replay");

        // Desensitized dump: no histogram section, and the raw MCV value must not leak via column_statistics.
        dumpInfo.setDesensitizedInfo(true);
        String desensitizedDump = gson.toJson(dumpInfo, QueryDumpInfo.class);
        Assertions.assertFalse(desensitizedDump.contains("column_histogram"),
                "desensitized dump must not carry column_histogram");
        Assertions.assertFalse(desensitizedDump.contains("MCV:"),
                "desensitized dump must not carry the histogram MCV preview");
        Assertions.assertFalse(desensitizedDump.contains("SENSITIVE_MCV_VALUE"),
                "desensitized dump must not leak raw MCV values");
        Assertions.assertFalse(desensitizedDump.contains("SENSITIVE_MIN_STRING"),
                "desensitized dump must not leak raw minString values");
        Assertions.assertFalse(desensitizedDump.contains("SENSITIVE_MAX_STRING"),
                "desensitized dump must not leak raw maxString values");
    }
}
