// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Analyzer;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Before;

public class DDLTestBase {

    protected static Analyzer analyzer;
    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), ctx);

        FeConstants.runningUnitTest = true;
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(GlobalStateMgrTestUtil.testDb1)
                .useDatabase(GlobalStateMgrTestUtil.testDb1);

        starRocksAssert.withTable("CREATE TABLE `testTable1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }
}
