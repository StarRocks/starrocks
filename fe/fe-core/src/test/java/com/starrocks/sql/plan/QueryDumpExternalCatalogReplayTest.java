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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Replays real query dumps captured from a cluster (with {@code mock=false}) over external-catalog tables
 * referenced by their fully-qualified {@code catalog.db.table} name -- the shape modern iceberg and hive
 * catalogs produce, as opposed to the legacy resource-mapping form the older dump tests exercise.
 *
 * <p>Such a dump records the table only as a {@code CREATE EXTERNAL TABLE ... ENGINE=ICEBERG|HIVE
 * ("resource"=...)} statement in {@code table_meta}; the backing metastore is not part of the dump.
 * Replay must therefore synthesize the table from the declared schema entirely offline
 * (UtFrameUtils.registerReplayExternalCatalogTables via ReplayIcebergResourceMetadata /
 * ReplayHiveResourceMetadata) and register a real external catalog of the matching type so the
 * fully-qualified reference resolves. Before this, the query analyzer's catalog-name rewrite mapped every
 * catalog reference to a (nonexistent) {@code resource_mapping_inside_catalog_hive_*} catalog and the
 * replay failed at analysis; the hive side additionally NPE'd in the legacy resource-mapping replay when
 * the dump carried no per-column statistics.
 *
 * <p>These dumps were produced by:
 *   POST /api/query_dump?db=&lt;catalog&gt;.bench_starrocks&amp;mock=false -d "&lt;query&gt;"
 * against a cluster whose {@code iceberg_test_rd} / {@code hive_test_rd} catalogs expose the same
 * {@code lineorder_100g} table. The tables carry no analyzed column statistics, so replay falls back to
 * UNKNOWN per-column statistics -- exactly what the cluster itself would hand back.
 */
public class QueryDumpExternalCatalogReplayTest extends ReplayFromDumpTestBase {

    private String replay(String dumpResource) throws Exception {
        Pair<QueryDumpInfo, String> replayPair = getCostPlanFragment(getDumpInfoFromFile(dumpResource));
        Assertions.assertNotNull(replayPair.second, "replay produced no plan for " + dumpResource);
        return replayPair.second;
    }

    @Test
    public void testReplayIcebergCatalogCount() throws Exception {
        // select count(*) from iceberg_test_rd.bench_starrocks.lineorder_100g
        String plan = replay("query_dump/iceberg_catalog_count");
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lineorder_100g"), "expected the iceberg table scanned, plan:\n" + plan);
        // The dump carries no table_row_count, so replay falls back to the default row count (100); the scan
        // cardinality must reflect that recovered value rather than collapsing to the empty-table default.
        Assertions.assertTrue(plan.contains("cardinality=100"),
                "expected the iceberg scan cardinality from the fallback row count, plan:\n" + plan);
    }

    @Test
    public void testReplayIcebergCatalogPredicate() throws Exception {
        // select lo_orderkey, count(*) ... where lo_orderkey > 1000 group by lo_orderkey -- exercises
        // column projection and a scan predicate over the synthesized iceberg table.
        String plan = replay("query_dump/iceberg_catalog_predicate");
        Assertions.assertTrue(plan.contains("IcebergScanNode"), "expected an iceberg scan, plan:\n" + plan);
        // The WHERE predicate must survive onto the synthesized iceberg scan (asserting only that the
        // projected column appears would pass even if the predicate were dropped, since lo_orderkey is also
        // the GROUP BY key/output slot).
        Assertions.assertTrue(plan.contains("lo_orderkey > 1000"),
                "expected the WHERE predicate pushed onto the iceberg scan, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogCount() throws Exception {
        // select count(*) from hive_test_rd.bench_starrocks.lineorder_100g
        String plan = replay("query_dump/hive_catalog_count");
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        Assertions.assertTrue(plan.contains("lineorder_100g"), "expected the hive table scanned, plan:\n" + plan);
        // The dump's hms row count is 0, so replay falls back to the numRows table property (620031330):
        // the scan cardinality must reflect that real row count, not the empty-table default.
        Assertions.assertTrue(plan.contains("cardinality: 620031330"),
                "expected the scan cardinality recovered from the numRows property, plan:\n" + plan);
    }

    @Test
    public void testReplayHiveCatalogPredicate() throws Exception {
        // select lo_orderkey, count(*) ... where lo_orderkey > 1000 group by lo_orderkey over hive.
        String plan = replay("query_dump/hive_catalog_predicate");
        Assertions.assertTrue(plan.contains("HdfsScanNode"), "expected a hive scan, plan:\n" + plan);
        // The WHERE predicate must survive onto the synthesized hive scan (hive renders it as a
        // NON-PARTITION PREDICATES line, which still contains this substring).
        Assertions.assertTrue(plan.contains("lo_orderkey > 1000"),
                "expected the WHERE predicate pushed onto the hive scan, plan:\n" + plan);
    }
}
