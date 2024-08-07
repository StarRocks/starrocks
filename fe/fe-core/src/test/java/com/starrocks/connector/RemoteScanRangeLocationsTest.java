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

package com.starrocks.connector;

import com.starrocks.common.Pair;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class RemoteScanRangeLocationsTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        connectContext.getSessionVariable().setConnectorMaxSplitSize(512 * 1024 * 1024);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
        connectContext.getSessionVariable().setForceScheduleLocal(false);
    }

    @Test
    public void testHiveSplit() throws Exception {
        String executeSql = "select * from hive0.file_split_db.file_split_tbl;";

        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(true);
            connectContext.getSessionVariable().setConnectorHugeFileSize(1024 * 1024 * 1024);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
            // in this case, if we split in huge file size, we will get two splits
            // which is not suitable for backend split, then it will fall back to fe split.
            // 2 * 1G / 64MB = 32
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeParams> scanRangeLocations = collectAllScanRangeParams(pair.second);
            Assert.assertEquals(32, scanRangeLocations.size());
        }
        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(true);
            // in this case, if we split in huge file size, we will get 4 splits
            // which is suitable for backend split.
            // 2 * 1G / 512MB = 4
            connectContext.getSessionVariable().setConnectorHugeFileSize(512 * 1024 * 1024);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(64 * 1024 * 1024);
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeParams> scanRangeLocations = collectAllScanRangeParams(pair.second);
            Assert.assertEquals(4, scanRangeLocations.size());
        }
        {
            connectContext.getSessionVariable().setEnableConnectorSplitIoTasks(false);
            connectContext.getSessionVariable().setConnectorMaxSplitSize(512 * 1024 * 1024);
            Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
            List<TScanRangeParams> scanRangeLocations = collectAllScanRangeParams(pair.second);
            Assert.assertEquals(4, scanRangeLocations.size());
            scanRangeLocations.sort((o1, o2) -> {
                THdfsScanRange scanRange1 = o1.scan_range.hdfs_scan_range;
                THdfsScanRange scanRange2 = o2.scan_range.hdfs_scan_range;
                if (scanRange1.relative_path.equalsIgnoreCase(scanRange2.relative_path)) {
                    return (int) (scanRange1.offset - scanRange2.offset);
                } else {
                    return scanRange1.compareTo(scanRange2);
                }
            });

            TScanRange scanRange1 = scanRangeLocations.get(0).scan_range;
            TScanRange scanRange2 = scanRangeLocations.get(1).scan_range;

            Assert.assertEquals(scanRange1.hdfs_scan_range.length, scanRange2.hdfs_scan_range.offset);
        }
    }

    @Test
    public void testHiveSplitWithForceLocalSchedule() throws Exception {
        connectContext.getSessionVariable().setForceScheduleLocal(true);

        String executeSql = "select * from hive0.file_split_db.file_split_tbl;";
        Pair<String, DefaultCoordinator> pair = UtFrameUtils.getPlanAndStartScheduling(connectContext, executeSql);
        List<TScanRangeParams> scanRangeLocations = collectAllScanRangeParams(pair.second);
        Assert.assertEquals(8, scanRangeLocations.size());

        scanRangeLocations.sort((o1, o2) -> {
            THdfsScanRange scanRange1 = o1.scan_range.hdfs_scan_range;
            THdfsScanRange scanRange2 = o2.scan_range.hdfs_scan_range;
            if (scanRange1.relative_path.equalsIgnoreCase(scanRange2.relative_path)) {
                return (int) (scanRange1.offset - scanRange2.offset);
            } else {
                return scanRange1.compareTo(scanRange2);
            }
        });

        Assert.assertEquals(0, scanRangeLocations.get(0).scan_range.hdfs_scan_range.offset);
        long previousOffset = scanRangeLocations.get(0).scan_range.hdfs_scan_range.length;
        for (int i = 1; i < 4; i++) {
            Assert.assertEquals(previousOffset, scanRangeLocations.get(i).scan_range.hdfs_scan_range.offset);
            previousOffset += scanRangeLocations.get(i).scan_range.hdfs_scan_range.length;
        }

        Assert.assertEquals(0, scanRangeLocations.get(4).scan_range.hdfs_scan_range.offset);
        previousOffset = scanRangeLocations.get(4).scan_range.hdfs_scan_range.length;
        for (int i = 5; i < 8; i++) {
            Assert.assertEquals(previousOffset, scanRangeLocations.get(i).scan_range.hdfs_scan_range.offset);
            previousOffset += scanRangeLocations.get(i).scan_range.hdfs_scan_range.length;
        }
    }
}
