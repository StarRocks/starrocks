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

package com.starrocks.connector.benchmark;

import com.starrocks.connector.benchmark.ssb.SsbBenchmarkSuite;
import com.starrocks.connector.benchmark.tpcds.TpcdsBenchmarkSuite;
import com.starrocks.connector.benchmark.tpch.TpchBenchmarkSuite;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BenchmarkSuiteTest {
    private static final long LINEITEM_SCALE5 = 29_999_795L;
    private static final long LINEITEM_SCALE10 = 59_986_052L;

    @Test
    public void testRowCountEstimateKnownUnknown() {
        RowCountEstimate known = RowCountEstimate.known(42L);
        Assertions.assertTrue(known.isKnown());
        Assertions.assertEquals(42L, known.getRowCount());

        RowCountEstimate unknown = RowCountEstimate.unknown();
        Assertions.assertFalse(unknown.isKnown());
        Assertions.assertEquals(-1L, unknown.getRowCount());
    }

    @Test
    public void testBenchmarkSuiteDefaults() {
        BenchmarkSuite suite = new TpcdsBenchmarkSuite();
        Assertions.assertEquals("/connector/benchmark/schema/tpcds_schema.json", suite.getSchemaResourcePath());
        Assertions.assertEquals("", suite.normalizeTableName(null));
        Assertions.assertEquals("store_sales", suite.normalizeTableName("STORE_SALES"));
    }

    @Test
    public void testSuiteFactory() {
        BenchmarkSuite defaultSuite = BenchmarkSuiteFactory.getDefaultSuite();
        Assertions.assertEquals(TpcdsBenchmarkSuite.NAME, defaultSuite.getName());

        List<BenchmarkSuite> suites = BenchmarkSuiteFactory.getSuites();
        Assertions.assertEquals(3, suites.size());
        Set<String> suiteNames = new HashSet<>();
        for (BenchmarkSuite suite : suites) {
            suiteNames.add(suite.getName());
        }
        Assertions.assertEquals(Set.of("tpcds", "tpch", "ssb"), suiteNames);

        Assertions.assertNull(BenchmarkSuiteFactory.getSuiteIfExists("missing"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> BenchmarkSuiteFactory.getSuite("missing"));
    }

    @Test
    public void testTpchRowCounts() {
        TpchBenchmarkSuite suite = new TpchBenchmarkSuite();

        assertKnownRowCount(suite.estimateRowCount("part", 2.0), 400_000L);
        assertKnownRowCount(suite.estimateRowCount("partsupp", 2.0), 1_600_000L);
        assertKnownRowCount(suite.estimateRowCount("supplier", 2.0), 20_000L);
        assertKnownRowCount(suite.estimateRowCount("customer", 2.0), 300_000L);
        assertKnownRowCount(suite.estimateRowCount("orders", 2.0), 3_000_000L);
        assertKnownRowCount(suite.estimateRowCount("nation", 2.0), 25L);
        assertKnownRowCount(suite.estimateRowCount("region", 2.0), 5L);

        assertKnownRowCount(suite.estimateRowCount("part", 0.0005), 1L);
        assertKnownRowCount(suite.estimateRowCount("lineitem", 0.5), 3_000_607L);
        assertKnownRowCount(suite.estimateRowCount("lineitem", 2.0), 12_000_860L);
        assertKnownRowCount(suite.estimateRowCount("lineitem", 5.0), LINEITEM_SCALE5);
        assertKnownRowCount(suite.estimateRowCount("lineitem", 10.0), LINEITEM_SCALE10);
        assertKnownRowCount(suite.estimateRowCount("lineitem", 16.0), 95_983_098L);

        Assertions.assertThrows(IllegalArgumentException.class, () -> suite.estimateRowCount("missing", 1.0));
    }

    @Test
    public void testSsbRowCounts() {
        SsbBenchmarkSuite suite = new SsbBenchmarkSuite();

        assertKnownRowCount(suite.estimateRowCount("customer", 2.0), 60_000L);
        assertKnownRowCount(suite.estimateRowCount("supplier", 0.0001), 1L);
        assertKnownRowCount(suite.estimateRowCount("part", 4.0), 600_000L);
        assertKnownRowCount(suite.estimateRowCount("part", 0.5), 100_000L);
        assertKnownRowCount(suite.estimateRowCount("date", 0.5), 1_278L);

        assertKnownRowCount(suite.estimateRowCount("lineorder", 0.5), 3_000_607L);
        assertKnownRowCount(suite.estimateRowCount("lineorder", 2.0), 12_000_860L);
        assertKnownRowCount(suite.estimateRowCount("lineorder", 5.0), LINEITEM_SCALE5);
        assertKnownRowCount(suite.estimateRowCount("lineorder", 10.0), LINEITEM_SCALE10);
        assertKnownRowCount(suite.estimateRowCount("lineorder", 16.0), 95_983_098L);

        Assertions.assertThrows(IllegalArgumentException.class, () -> suite.estimateRowCount("missing", 1.0));
    }

    @Test
    public void testTpcdsRowCounts() {
        TpcdsBenchmarkSuite suite = new TpcdsBenchmarkSuite();

        RowCountEstimate scaleOne = suite.estimateRowCount("STORE_SALES", 1.0);
        assertKnownRowCount(scaleOne, 2_880_404L);

        RowCountEstimate scaleThousand = suite.estimateRowCount("store_sales", 1000.0);
        assertKnownRowCount(scaleThousand, 2_879_987_999L);

        RowCountEstimate scaleTen = suite.estimateRowCount("store_sales", 10.0);
        Assertions.assertTrue(scaleTen.isKnown());
        Assertions.assertTrue(scaleTen.getRowCount() > scaleOne.getRowCount());
        Assertions.assertTrue(scaleTen.getRowCount() < scaleThousand.getRowCount());

        RowCountEstimate scaleZero = suite.estimateRowCount("store_sales", 0.0);
        Assertions.assertTrue(scaleZero.isKnown());
        Assertions.assertEquals(0L, scaleZero.getRowCount());

        Assertions.assertThrows(IllegalArgumentException.class, () -> suite.estimateRowCount("missing", 1.0));
    }

    private static void assertKnownRowCount(RowCountEstimate estimate, long expected) {
        Assertions.assertTrue(estimate.isKnown());
        Assertions.assertEquals(expected, estimate.getRowCount());
    }
}
