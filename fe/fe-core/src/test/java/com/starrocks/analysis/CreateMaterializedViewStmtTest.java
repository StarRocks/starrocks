package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateMaterializedViewStmtTest {
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE IF NOT EXISTS detailDemo (\n" +
                        "    recruit_date  DATE           NOT NULL COMMENT \"YYYY-MM-DD\",\n" +
                        "    region_num    TINYINT        COMMENT \"range [-128, 127]\",\n" +
                        "    num_plate     SMALLINT       COMMENT \"range [-32768, 32767] \",\n" +
                        "    tel           INT            COMMENT \"range [-2147483648, 2147483647]\",\n" +
                        "    id            BIGINT         COMMENT \"range [-2^63 + 1 ~ 2^63 - 1]\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(recruit_date, region_num)\n" +
                        "DISTRIBUTED BY HASH(recruit_date, region_num) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\" \n" +
                        ");")
                .useDatabase("test");
    }

    @Test(expected = AnalysisException.class)
    public void testCreateMVWithReturnWrongType() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW mv_detailDemo2 AS SELECT recruit_date, " +
                "bitmap_count(bitmap_union(to_bitmap(region_num))) FROM detailDemo GROUP BY recruit_date;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }
}
