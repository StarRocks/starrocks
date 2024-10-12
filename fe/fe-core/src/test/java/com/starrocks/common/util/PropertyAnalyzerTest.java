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

package com.starrocks.common.util;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.externalcooldown.ExternalCooldownConfig;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PropertyAnalyzerTest {
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;
    protected static StarRocksAssert starRocksAssert;
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    protected static long startSuiteTime = 0;
    protected long startCaseTime = 0;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE tbl1\n" +
                        "(\n" +
                        "    id int,\n" +
                        "    ts datetime,\n" +
                        "    data string\n" +
                        ")\n" +
                        "DUPLICATE KEY(`id`, `ts`)\n" +
                        "PARTITION BY RANGE(`ts`)\n" +
                        "(\n" +
                        "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                        "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                        "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                        "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                        "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES(\n" +
                        "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                        "'external_cooldown_schedule'='START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");");
    }

    @Test
    public void testAnalyzeDataCacheInfo() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, "true");
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, "true");

        try {
            PropertyAnalyzer.analyzeDataCacheInfo(properties);
            Assert.assertTrue(false);
        } catch (AnalysisException e) {
            Assert.assertEquals("enable_async_write_back is disabled since version 3.1.4", e.getMessage());
        }
    }

    @Test
    public void testAnalyzeDataCachePartitionDuration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "7 day");

        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
        } catch (AnalysisException e) {
            Assert.assertTrue(false);
        }

        Assert.assertTrue(properties.size() == 0);
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, "abcd");
        try {
            PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
            Assert.assertTrue(false);
        } catch (AnalysisException e) {
            Assert.assertEquals("Cannot parse text to Duration", e.getMessage());
        }
    }

    @Test
    public void testAnalyzeExternalCoolDownConfig() throws Exception {
        // normal
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET,
                "iceberg0.partitioned_transforms_db.t0_day");
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE,
                "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE");
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND,
                "3600");

        ExternalCooldownConfig config = PropertyAnalyzer.analyzeExternalCoolDownConfig(properties);
        Assert.assertNotNull(config);
        Assert.assertEquals("START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", config.getSchedule());
        Assert.assertEquals("iceberg0.partitioned_transforms_db.t0_day", config.getTarget());
        Assert.assertEquals((Long) 3600L, config.getWaitSecond());

        // regex not match
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET,
                "iceberg0.partitioned_transforms_db.t0_day.xxx");
        Assert.assertThrows(DdlException.class, () -> PropertyAnalyzer.analyzeExternalCoolDownConfig(properties));

        // table not exists
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET,
                "default_catalog.test.non_exist");
        Assert.assertThrows(DdlException.class, () -> PropertyAnalyzer.analyzeExternalCoolDownConfig(properties));

        // table not iceberg table
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET,
                "default_catalog.test.tbl1");
        Assert.assertThrows(DdlException.class, () -> PropertyAnalyzer.analyzeExternalCoolDownConfig(properties));

        // reset target
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET,
                "iceberg0.partitioned_transforms_db.t0_day");

        // schedule invalid
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE,
                "START 01:00 END 07:59 EVERY INTERVAL 1");
        Assert.assertThrows(DdlException.class, () -> PropertyAnalyzer.analyzeExternalCoolDownConfig(properties));

        // reset schedule
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE,
                "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE");

        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND,
                "");
        Assert.assertThrows(DdlException.class, () -> PropertyAnalyzer.analyzeExternalCoolDownConfig(properties));
    }

    @Test
    public void testAnalyzeExternalCooldownSyncedTimeMs() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SYNCED_TIME,
                "2024-10-23 16:00:00");

        long waitSeconds = PropertyAnalyzer.analyzeExternalCooldownSyncedTimeMs(properties);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date parsedDate = dateFormat.parse("2024-10-23 16:00:00");
        Assert.assertEquals(waitSeconds, parsedDate.getTime());

        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SYNCED_TIME, "");
        waitSeconds = PropertyAnalyzer.analyzeExternalCooldownSyncedTimeMs(properties);
        Assert.assertEquals(0L, waitSeconds);

        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SYNCED_TIME, "abc");
        Assert.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeExternalCooldownSyncedTimeMs(properties));
    }

    @Test
    public void testAnalyzeExternalCooldownConsistencyCheckTimeMs() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_CONSISTENCY_CHECK_TIME,
                "2024-10-23 18:00:00");

        long checkTimeMs = PropertyAnalyzer.analyzeExternalCooldownConsistencyCheckTimeMs(properties);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date parsedDate = dateFormat.parse("2024-10-23 18:00:00");
        Assert.assertEquals(checkTimeMs, parsedDate.getTime());

        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_CONSISTENCY_CHECK_TIME, "");
        checkTimeMs = PropertyAnalyzer.analyzeExternalCooldownConsistencyCheckTimeMs(properties);
        Assert.assertEquals(0L, checkTimeMs);

        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_CONSISTENCY_CHECK_TIME, "abc");
        Assert.assertThrows(SemanticException.class,
                () -> PropertyAnalyzer.analyzeExternalCooldownConsistencyCheckTimeMs(properties));
    }
}
