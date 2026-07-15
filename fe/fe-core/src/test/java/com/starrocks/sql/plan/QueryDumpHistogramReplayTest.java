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

import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
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
}
