package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FsBroker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.system.BrokerHbResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * TEST :
 * [Cancel | Show ] Export stmt
 */
public class ExportRelativeStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testExportStmt() {
        FsBroker fsBroker = new FsBroker("127.0.0.1", 8118);
        long time = System.currentTimeMillis();
        BrokerHbResponse hbResponse = new BrokerHbResponse("broker", "127.0.0.1", 8118, time);
        fsBroker.handleHbResponse(hbResponse, false);
        GlobalStateMgr.getCurrentState().getBrokerMgr().replayAddBrokers("broker", Lists.newArrayList(fsBroker));
        String originStmt = "EXPORT TABLE tall TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        ExportStmt stmt = (ExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getRedirectStatus());
        Assert.assertTrue(stmt.needAuditEncryption());
        Assert.assertNotNull(stmt.getRowDelimiter());
        Assert.assertNotNull(stmt.getTblName());
        Assert.assertEquals("EXPORT TABLE `test`.`tall`\n" +
                " TO 'hdfs://hdfs_host:port/a/b/c/'\n" +
                "PROPERTIES (\"load_mem_limit\" = \"2147483648\", \"timeout\" = \"7200\")\n" +
                " WITH BROKER 'broker' (\"password\" = \"***\", \"username\" = \"test\")", stmt.toString());
        Assert.assertEquals(stmt.getPath(), "hdfs://hdfs_host:port/a/b/c/");

        // run with sync mode
        originStmt = "EXPORT TABLE tall TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH SYNC MODE " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        stmt = (ExportStmt) analyzeSuccess(originStmt);
        Assert.assertTrue(stmt.getSync());

        // partition data
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) TO \"hdfs://hdfs_host:port/a/b/c/test_data_\" PROPERTIES " +
                "(\"column_separator\"=\",\") WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        stmt = (ExportStmt) analyzeSuccess(originStmt);
        Assert.assertEquals(Lists.newArrayList("p1", "p2"), stmt.getPartitions());
        Assert.assertEquals("EXPORT TABLE `test`.`tp` PARTITION (p1, p2)\n" +
                " TO 'hdfs://hdfs_host:port/a/b/c/'\n" +
                "PROPERTIES (\"load_mem_limit\" = \"2147483648\", \"timeout\" = \"7200\")\n" +
                " WITH BROKER 'broker' (\"password\" = \"***\", \"username\" = \"test\")", stmt.toString());
        Assert.assertEquals(",", stmt.getColumnSeparator());
        Assert.assertEquals("test_data_", stmt.getFileNamePrefix());

        // column data
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        stmt = (ExportStmt) analyzeSuccess(originStmt);
        Assert.assertEquals(Lists.newArrayList("c1", "c2"), stmt.getColumnNames());
        stmt.setExportStartTime(time);
        Assert.assertFalse(stmt.isIncludeQueryId());
        Assert.assertEquals(time, stmt.getExportStartTime());
        Map<String, String> properties = stmt.getProperties();
        // bad cases
        // bad table
        originStmt = "EXPORT TABLE test TO \"hdfs://hdfs_host:port/a/b/c/\" WITH BROKER \"broker\" " +
                "(\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE db1.tp TO \"hdfs://hdfs_host:port/a/b/c/\" WITH BROKER \"broker\" " +
                "(\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        // bad partition
        originStmt = "EXPORT TABLE tp TEMPORARY PARTITION (p1,p2) TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p6,p7) TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        // bad broker desc
        originStmt = "EXPORT TABLE tp TO \"hdfs://hdfs_host:port/a/b/c/\" WITH " +
                "BROKER \"broker1\" (\"username\"=\"test\")";
        analyzeFail(originStmt);
        // bad properties
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"tf\", \"timeout\" = \"7200\", \"include_query_id\" = \"true\") " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\">\"10\", \"timeout\" = \"7200\", \"include_query_id\" = \"true\") WITH BROKER " +
                "\"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"22\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"g\", \"include_query_id\" = \"true\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        // bad path
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp TO \"port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c2) TO \"hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        // bad columns
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c5,c6) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp PARTITION (p1,p2) (c1,c1) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);
        originStmt = "EXPORT TABLE tp (,) TO \"hdfs://hdfs_host:port/a/b/c/\" PROPERTIES " +
                "(\"load_mem_limit\"=\"2147483648\", \"timeout\" = \"7200\", \"include_query_id\" = \"false\") WITH " +
                "BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);

        originStmt = "EXPORT TABLE view_to_drop TO \"hdfs://hdfs_host:port/a/b/c/\" " +
                "WITH BROKER \"broker\" (\"username\"=\"test\", \"password\"=\"test\");";
        analyzeFail(originStmt);

        String path = "hdfs://127.0.0.1:9002/export/";
        //BrokerDesc brokerDesc = new BrokerDesc("broker", null);

        String dbName = "test";
        String tableName = "tp";
        TableName tb = new TableName(dbName, tableName);
        List<String> columnLst = Lists.newArrayList("c1", "c2");

        ExportStmt stmt1 = new ExportStmt(new TableRef(tb, null), columnLst, path, new HashMap<>(), null);

        try {
            Analyzer.analyze(stmt1, AnalyzeTestUtil.getConnectContext());
        } catch (SemanticException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testShowExport() {
        String originStmt = "Show Export limit 10";
        ShowExportStmt stmt = (ShowExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getMetaData());
        Assert.assertNotNull(stmt.getRedirectStatus());
        Assert.assertNull(stmt.getJobState());
        Assert.assertEquals(0, stmt.getJobId());
        Assert.assertNull(stmt.getQueryId());
        Assert.assertEquals(10L, stmt.getLimit());
        Assert.assertNotNull(stmt.toString());

        originStmt = "SHOW EXPORT FROM test WHERE queryid = \"921d8f80-7c9d-11eb-9342-acde48001122\";";
        stmt = (ShowExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getWhereClause());
        Assert.assertNotNull(stmt.getQueryId());
        Assert.assertEquals(-1L, stmt.getLimit());

        originStmt = "SHOW EXPORT FROM test WHERE id = 10";
        stmt = (ShowExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getWhereClause());
        Assert.assertEquals(10L, stmt.getJobId());


        originStmt = "SHOW EXPORT FROM test ORDER BY StartTime DESC;";
        stmt = (ShowExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getOrderByElements());
        Assert.assertNotNull(stmt.getOrderByPairs());

        originStmt = "SHOW EXPORT FROM test WHERE STATE = \"exporting\" ORDER BY StartTime DESC;";
        stmt = (ShowExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getJobState());
        Assert.assertNotNull(stmt.toString());

        // bad cases
        originStmt = "SHOW EXPORT FROM test WHERE test = \"test\"";
        analyzeFail(originStmt);
        originStmt = "SHOW EXPORT FROM test WHERE id = 13dfsf";
        analyzeFail(originStmt);
        originStmt = "SHOW EXPORT FROM test WHERE STATE = \"exporting\" ORDER BY test DESC;";
        analyzeFail(originStmt);
        originStmt = "SHOW EXPORT FROM test WHERE id > 10";
        analyzeFail(originStmt);
        originStmt = "SHOW EXPORT FROM test WHERE true";
        analyzeFail(originStmt);
    }

    @Test
    public void testCancelExport() {
        String originStmt = "CANCEL EXPORT FROM test WHERE queryid = \"921d8f80-7c9d-11eb-9342-acde48001122\";";
        CancelExportStmt stmt = (CancelExportStmt) analyzeSuccess(originStmt);
        Assert.assertNotNull(stmt.getWhereClause());
        Assert.assertNotNull(stmt.getQueryId());
        Assert.assertNotNull(stmt.toString());
        // bad cases
        originStmt = "CANCEL EXPORT FROM test WHERE id = \"921d8f80-7c9d-11eb-9342-acde48001122\";";
        analyzeFail(originStmt);
        originStmt = "CANCEL EXPORT FROM test WHERE queryid=\"921d8f80\";";
        analyzeFail(originStmt);
        originStmt = "CANCEL EXPORT FROM test WHERE queryid=6;";
        analyzeFail(originStmt);
        CancelExportStmt stmt1 = new CancelExportStmt("test", null);
        try {
            Analyzer.analyze(stmt1, AnalyzeTestUtil.getConnectContext());
        } catch (SemanticException e) {
            e.printStackTrace();
        }
    }
}
