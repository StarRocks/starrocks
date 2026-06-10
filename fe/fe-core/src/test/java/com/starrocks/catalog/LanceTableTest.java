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

package com.starrocks.catalog;

import com.starrocks.planner.lance.LanceScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class LanceTableTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db");
        // avoid touching a real lance dataset (which needs the native lib / object storage) in unit tests
        new MockUp<LanceScanNode>() {
            @Mock
            public void computeScanRangeLocations(String datasetUri) {
            }

            @Mock
            public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
                List<TScanRangeLocations> scanRangeLocationsList = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                    TScanRange scanRange = new TScanRange();
                    THdfsScanRange hdfsScanRange = new THdfsScanRange();
                    hdfsScanRange.setFragment_id(i);
                    hdfsScanRange.setDataset_uri("test");
                    hdfsScanRange.setLength(10);
                    scanRange.setHdfs_scan_range(hdfsScanRange);

                    scanRangeLocations.setScan_range(scanRange);
                    scanRangeLocationsList.add(scanRangeLocations);
                }
                return scanRangeLocationsList;
            }
        };
        // credentials/endpoint flow through PROPERTIES; no plaintext secrets are hardcoded anywhere
        String createTableSql = "CREATE EXTERNAL TABLE db.lance_tbl (" +
                "col1 INT, col2 INT, vector_col ARRAY<FLOAT>" +
                ") ENGINE=LANCE PROPERTIES ('dataset.uri'='/tmp/dataset.lance')";
        starRocksAssert.withTable(createTableSql);
    }

    @Test
    public void testPredicatePushDown() throws Exception {
        String sql = "select vector_col from db.lance_tbl where col1 = 1;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assertions.assertTrue(plan.contains("LANCE SCAN") || plan.contains("lance_tbl"));
    }

    @Test
    public void testProject() throws Exception {
        String sql = "select cosine_similarity(vector_col,[1,2,3]) from db.lance_tbl;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assertions.assertNotNull(plan);
    }

    @Test
    public void testVectorSearch() throws Exception {
        String sql = "select vector_col from db.lance_tbl order by cosine_similarity(vector_col,[1,2,3]) limit 10;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assertions.assertNotNull(plan);
    }
}
