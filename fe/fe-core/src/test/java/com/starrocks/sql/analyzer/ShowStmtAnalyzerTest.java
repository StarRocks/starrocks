// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowStmtAnalyzerTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testShowBackendsBasic() throws Exception {
        String sql = "SHOW BACKENDS";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNull(showStmt.getPredicate());
        Assert.assertNull(showStmt.getOrderByElements());
        Assert.assertNull(showStmt.getLimitElement());
    }

    @Test
    public void testShowBackendsWithLike() throws Exception {
        String sql = "SHOW BACKENDS LIKE 'host%'";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertEquals("host%", showStmt.getPattern());
        Assert.assertNull(showStmt.getPredicate());
        Assert.assertNull(showStmt.getOrderByElements());
        Assert.assertNull(showStmt.getLimitElement());
    }

    @Test
    public void testShowBackendsWithWhere() throws Exception {
        String sql = "SHOW BACKENDS WHERE Alive = 'true'";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNotNull(showStmt.getPredicate());
        Assert.assertNull(showStmt.getOrderByElements());
        Assert.assertNull(showStmt.getLimitElement());
    }

    @Test
    public void testShowBackendsWithOrderBy() throws Exception {
        String sql = "SHOW BACKENDS ORDER BY Host";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNull(showStmt.getPredicate());
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(1, showStmt.getOrderByElements().size());
        Assert.assertNull(showStmt.getLimitElement());
    }

    @Test
    public void testShowBackendsWithOrderByDesc() throws Exception {
        String sql = "SHOW BACKENDS ORDER BY Host DESC";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(1, showStmt.getOrderByElements().size());
        Assert.assertFalse(showStmt.getOrderByElements().get(0).getIsAsc());
    }

    @Test
    public void testShowBackendsWithMultipleOrderBy() throws Exception {
        String sql = "SHOW BACKENDS ORDER BY Alive, Host DESC";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(2, showStmt.getOrderByElements().size());
        Assert.assertTrue(showStmt.getOrderByElements().get(0).getIsAsc());
        Assert.assertFalse(showStmt.getOrderByElements().get(1).getIsAsc());
    }

    @Test
    public void testShowBackendsWithLimit() throws Exception {
        String sql = "SHOW BACKENDS LIMIT 5";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNull(showStmt.getPredicate());
        Assert.assertNull(showStmt.getOrderByElements());
        Assert.assertNotNull(showStmt.getLimitElement());
        Assert.assertTrue(showStmt.getLimitElement().hasLimit());
        Assert.assertEquals(5, showStmt.getLimitElement().getLimit());
    }

    @Test
    public void testShowBackendsWithLimitOffset() throws Exception {
        String sql = "SHOW BACKENDS LIMIT 2, 5";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNotNull(showStmt.getLimitElement());
        Assert.assertTrue(showStmt.getLimitElement().hasOffset());
        Assert.assertTrue(showStmt.getLimitElement().hasLimit());
        Assert.assertEquals(2, showStmt.getLimitElement().getOffset());
        Assert.assertEquals(5, showStmt.getLimitElement().getLimit());
    }

    @Test
    public void testShowBackendsWithAllClauses() throws Exception {
        String sql = "SHOW BACKENDS WHERE Alive = 'true' ORDER BY Host DESC LIMIT 2, 3";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNotNull(showStmt.getPredicate());
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(1, showStmt.getOrderByElements().size());
        Assert.assertFalse(showStmt.getOrderByElements().get(0).getIsAsc());
        Assert.assertNotNull(showStmt.getLimitElement());
        Assert.assertEquals(2, showStmt.getLimitElement().getOffset());
        Assert.assertEquals(3, showStmt.getLimitElement().getLimit());
    }

    @Test
    public void testShowEnginesWithClauses() throws Exception {
        String sql = "SHOW ENGINES WHERE Support = 'YES' ORDER BY Engine LIMIT 3";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowEnginesStmt);
        ShowEnginesStmt showStmt = (ShowEnginesStmt) stmt;
        
        Assert.assertNull(showStmt.getPattern());
        Assert.assertNotNull(showStmt.getPredicate());
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(1, showStmt.getOrderByElements().size());
        Assert.assertNotNull(showStmt.getLimitElement());
        Assert.assertEquals(3, showStmt.getLimitElement().getLimit());
    }

    @Test
    public void testShowFrontendsWithClauses() throws Exception {
        String sql = "SHOW FRONTENDS LIKE '%leader%' ORDER BY Role DESC";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowFrontendsStmt);
        ShowFrontendsStmt showStmt = (ShowFrontendsStmt) stmt;
        
        Assert.assertEquals("%leader%", showStmt.getPattern());
        Assert.assertNull(showStmt.getPredicate());
        Assert.assertNotNull(showStmt.getOrderByElements());
        Assert.assertEquals(1, showStmt.getOrderByElements().size());
        Assert.assertFalse(showStmt.getOrderByElements().get(0).getIsAsc());
        Assert.assertNull(showStmt.getLimitElement());
    }

    @Test
    public void testShowEnginesLikePattern() throws Exception {
        String sql = "SHOW ENGINES LIKE 'OLAP'";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowEnginesStmt);
        ShowEnginesStmt showStmt = (ShowEnginesStmt) stmt;
        
        Assert.assertEquals("OLAP", showStmt.getPattern());
    }

    @Test
    public void testComplexWhereClause() throws Exception {
        String sql = "SHOW BACKENDS WHERE Alive = 'true' AND Host LIKE '%server%'";
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        
        Assert.assertTrue(stmt instanceof ShowBackendsStmt);
        ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        
        Assert.assertNotNull(showStmt.getPredicate());
        // The predicate should be a compound predicate with AND
    }

    @Test
    public void testInvalidSyntax() {
        try {
            String sql = "SHOW BACKENDS INVALID CLAUSE";
            SqlParser.parse(sql, connectContext.getSessionVariable());
            Assert.fail("Should have thrown parsing exception");
        } catch (Exception e) {
            // Expected parsing exception
            Assert.assertTrue(e.getMessage().contains("parse") || e.getMessage().contains("syntax"));
        }
    }

    @Test
    public void testLikeAndWhereConflict() {
        try {
            String sql = "SHOW BACKENDS LIKE 'host%' WHERE Alive = 'true'";
            SqlParser.parse(sql, connectContext.getSessionVariable());
            Assert.fail("Should have thrown parsing exception for LIKE and WHERE together");
        } catch (Exception e) {
            // Expected - LIKE and WHERE are mutually exclusive in the grammar
        }
    }
}