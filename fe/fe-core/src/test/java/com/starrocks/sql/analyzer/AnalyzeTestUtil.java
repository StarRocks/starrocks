// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.SqlParser;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.relation.InsertRelation;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;

import java.io.StringReader;

public class AnalyzeTestUtil {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static String DB_NAME = "test";

    public static void init() throws Exception {
        // create connect context
<<<<<<< HEAD
=======
        UtFrameUtils.createMinStarRocksCluster();
>>>>>>> ffe46259 (BugFix: Fix bug of master exit wrong when adding follower (#4428) (#4867))
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t2` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tall` (\n" +
                "  `ta` varchar(20) NULL COMMENT \"\",\n" +
                "  `tb` smallint(6) NULL COMMENT \"\",\n" +
                "  `tc` int(11) NULL COMMENT \"\",\n" +
                "  `td` bigint(20) NULL COMMENT \"\",\n" +
                "  `te` float NULL COMMENT \"\",\n" +
                "  `tf` double NULL COMMENT \"\",\n" +
                "  `tg` bigint(20) NULL COMMENT \"\",\n" +
                "  `th` datetime NULL COMMENT \"\",\n" +
                "  `ti` date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ta`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ta`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_object` (\n" +
                "  `v1` int(11) NULL,\n" +
                "  `v2` int(11) NULL,\n" +
                "  `v3` int(11) NULL,\n" +
                "  `v4` int(11) NULL,\n" +
                "  `b1` bitmap BITMAP_UNION NULL,\n" +
                "  `b2` bitmap BITMAP_UNION NULL,\n" +
                "  `b3` bitmap BITMAP_UNION NULL,\n" +
                "  `b4` bitmap BITMAP_UNION NULL,\n" +
                "  `h1` hll hll_union NULL,\n" +
                "  `h2` hll hll_union NULL,\n" +
                "  `h3` hll hll_union NULL,\n" +
                "  `h4` hll hll_union NULL,\n" +
                "  `p1` percentile PERCENTILE_UNION NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, `v3`, `v4`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tarray` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)>  NULL,\n" +
                "  `v4` ARRAY<largeint>  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tnotnull` (\n" +
                "  `v1` bigint NOT NULL,\n" +
                "  `v2` bigint NOT NULL,\n" +
                "  `v3` bigint NOT NULL DEFAULT \"100\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    public static ConnectContext getConnectContext() {
        return connectContext;
    }

    public static QueryRelation analyzeSuccess(String originStmt) {
        try {
            SqlScanner input =
                    new SqlScanner(new StringReader(originStmt), connectContext.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

            Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
            return (QueryRelation) analyzer.analyze(statementBase);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail();
            return null;
        }
    }

    public static InsertRelation analyzeSuccessUseInsert(String originStmt) {
        try {
            SqlScanner input =
                    new SqlScanner(new StringReader(originStmt), connectContext.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

            Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
            return (InsertRelation) analyzer.analyze(statementBase);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail();
            return null;
        }
    }

    public static void analyzeFail(String originStmt) {
        analyzeFail(originStmt, "");
    }

    public static void analyzeFail(String originStmt, String exceptMessage) {
        try {
            SqlScanner input =
                    new SqlScanner(new StringReader(originStmt), connectContext.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

            Analyzer analyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
            analyzer.analyze(statementBase);
            Assert.fail("Miss semantic error exception");
        } catch (SemanticException | AnalysisException | UnsupportedException e) {
            if (!exceptMessage.equals("")) {
                Assert.assertTrue(e.getMessage().contains(exceptMessage));
            }
        } catch (Exception e) {
            Assert.fail("analyze exception");
        }
    }
}
