package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.common.UserException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAlterTableTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testTableRename() {
        AlterTableStmt alterTableStmt = (AlterTableStmt) analyzeSuccess("alter table test rename test1");
        Assert.assertEquals(alterTableStmt.getOps().size(), 1);
        Assert.assertEquals("ALTER TABLE `default_cluster:test`.`test` RENAME test1", alterTableStmt.toString());
        Assert.assertEquals("test", alterTableStmt.getTbl().getTbl());
        analyzeFail("alter table test rename");
    }

    @Test
    public void testOldAnalyzer() {
        analyzeSuccess("alter table test add column col1 int");
    }

    @Test(expected = SemanticException.class)
    public void testNoClause() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        AlterStmtAnalyzer.analyze(alterTableStmt, AnalyzeTestUtil.getConnectContext());
    }
}