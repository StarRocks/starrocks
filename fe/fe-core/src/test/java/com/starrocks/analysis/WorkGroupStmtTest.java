package com.starrocks.analysis;

import com.starrocks.catalog.WorkGroup;
import com.starrocks.catalog.WorkGroupClassifier;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TWorkGroupType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.Set;
import java.util.UUID;

public class WorkGroupStmtTest {
    private static String runningDir = "fe/mocked/WorkGroupStmtTest/" + UUID.randomUUID().toString() + "/";
    private static StarRocksAssert starRocksAssert;

    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanStarRocksFeDir(runningDir);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testCreateResourceGroup() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "create resource_group rg1 " +
                "to (user='abcdefg', role='abc', query_type in ('select', 'insert'), source_ip='192.168.1.1/24') " +
                "with ('cpu_core_limit'='8', 'mem_limit'='50%','concurrency_limit'='10','type'='normal')";

        SqlScanner input = new SqlScanner(new StringReader(sql), ctx.getSessionVariable().getSqlMode());
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);
        SqlParser parser = new SqlParser(input);
        com.starrocks.analysis.Analyzer analyzer =
                new com.starrocks.analysis.Analyzer(ctx.getCatalog(), ctx);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);
        statementBase.analyze(analyzer);
        CreateWorkGroupStmt createWorkGroupStmt = (CreateWorkGroupStmt) statementBase;
        WorkGroup workGroup = createWorkGroupStmt.getWorkgroup();
        Assert.assertEquals(workGroup.getName(), "rg1");
        Assert.assertEquals(workGroup.getCpuCoreLimit(), 8);
        Assert.assertEquals("" + workGroup.getMemLimit(), "0.5");
        Assert.assertEquals(workGroup.getConcurrencyLimit(), 10);
        Assert.assertEquals(workGroup.getWorkGroupType(), TWorkGroupType.WG_NORMAL);
        Assert.assertTrue(workGroup.getClassifiers().size() == 1);
        WorkGroupClassifier classifier = workGroup.getClassifiers().get(0);
        Assert.assertEquals(classifier.getUser(), "abcdefg");
        Assert.assertEquals(classifier.getRole(), "abc");
        Assert.assertTrue(classifier.getQueryTypes().contains(WorkGroupClassifier.QueryType.INSERT));
        Assert.assertTrue(classifier.getQueryTypes().contains(WorkGroupClassifier.QueryType.SELECT));
        Assert.assertEquals(classifier.getSourceIp().getCidrSignature(), "192.168.1.1/24");
    }
}
