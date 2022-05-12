// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import org.junit.Assert;
import org.junit.Test;

public class ShowCatalogsStmtTest {
    @Test
    public void testShowCatalogsParserAndAnalyzer() throws AnalysisException {
        final Analyzer analyzer = AccessTestUtil.fetchBlockAnalyzer();
        ShowCatalogsStmt stmt = new ShowCatalogsStmt();
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW CATALOGS", stmt.toSql());
    }
}
