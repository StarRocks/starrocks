// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableRenameClause;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AnalyzeAlterTableClauseTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testTableRename() {
        TableRenameClause clause = new TableRenameClause("newTableName");
        AlterTableClauseAnalyzer.analyze(clause, connectContext);
        Assert.assertEquals("RENAME newTableName",
                clause.toSql());

    }

    @Test(expected = SemanticException.class)
    public void testEmptyNewTableName() {
        TableRenameClause clause = new TableRenameClause("");
        AlterTableClauseAnalyzer.analyze(clause, connectContext);
    }

    @Test(expected = SemanticException.class)
    public void testIllegalName() {
        TableRenameClause clause = new TableRenameClause("_newName");
        AlterTableClauseAnalyzer.analyze(clause, connectContext);
    }
}
