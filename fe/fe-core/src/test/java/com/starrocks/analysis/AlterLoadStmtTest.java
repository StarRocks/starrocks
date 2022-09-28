// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.collect.Maps;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LoadStmt;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class AlterLoadStmtTest {

    private Analyzer analyzer;

    @Mocked
    private Auth auth;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer();

        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "NORMAL");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);

            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
            Assert.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "HIGH");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assert.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "HIGHEST");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assert.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "LOW");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assert.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "LOWEST");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());

            Assert.assertEquals(1, stmt.getAnalyzedJobProperties().size());
            Assert.assertTrue(
                    stmt.getAnalyzedJobProperties().containsKey(LoadStmt.PRIORITY));
        }
    }

    @Test(expected = SemanticException.class)
    public void testNoProperties() {
        AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"), null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
    }

    @Test
    public void testUnsupportedProperties() {
        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(CreateRoutineLoadStmt.FORMAT, "csv");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);
            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
                Assert.fail();
            } catch (SemanticException e) {
                Assert.assertTrue(e.getMessage().contains("format is invalid property"));
            } catch (Exception e) {
                Assert.fail();
            }
        }

        {
            Map<String, String> jobProperties = Maps.newHashMap();
            jobProperties.put(LoadStmt.PRIORITY, "abc");
            AlterLoadStmt stmt = new AlterLoadStmt(new LabelName("db1", "label1"),
                    jobProperties);

            try {
                com.starrocks.sql.analyzer.Analyzer.analyze(stmt, new ConnectContext());
                Assert.fail();
            } catch (SemanticException e) {
                Assert.assertTrue(e.getMessage().contains("priority"));
            } catch (Exception e) {
                Assert.fail();
            }
        }
    }
}
