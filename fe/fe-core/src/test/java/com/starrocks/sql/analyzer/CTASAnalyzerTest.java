package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccessUseInsert;

public class CTASAnalyzerTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeInsertTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinStarRocksCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("ctas").useDatabase("ctas")
                .withTable("create table test(c1 varchar(10),c2 varchar(10)) DISTRIBUTED BY HASH(c1) " +
                        "BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\" );")
                .withTable("create table test3(c1 varchar(10),c2 varchar(10)) DISTRIBUTED BY HASH(c1) " +
                        "BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\" );");
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testSimpleCase() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String CTASSQL1 = "create table test2 as select * from test;";

        UtFrameUtils.parseStmtWithNewAnalyzer(CTASSQL1, ctx);

        String CTASSQL2 = "create table test6 as select c1+c2 as cr from test3;";

        UtFrameUtils.parseStmtWithNewAnalyzer(CTASSQL2, ctx);
    }
}
