package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAlterTableStatementTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
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
    public void testTableRenameClause() {
        TableRenameClause clause = new TableRenameClause("newTableName");
        AlterTableStatementAnalyzer.analyze(clause, connectContext);
        Assert.assertEquals("RENAME newTableName",
                clause.toSql());

    }

    @Test(expected = SemanticException.class)
    public void testEmptyNewTableName() {
        TableRenameClause clause = new TableRenameClause("");
        AlterTableStatementAnalyzer.analyze(clause, connectContext);
    }

    @Test(expected = SemanticException.class)
    public void testIllegalNewTableName() {
        TableRenameClause clause = new TableRenameClause("_newName");
        AlterTableStatementAnalyzer.analyze(clause, connectContext);
    }

    @Test(expected = SemanticException.class)
    public void testNoClause() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        AlterTableStatementAnalyzer.analyze(alterTableStmt, AnalyzeTestUtil.getConnectContext());
    }
}