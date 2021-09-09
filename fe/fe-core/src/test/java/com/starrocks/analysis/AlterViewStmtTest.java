// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/AlterViewStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class AlterViewStmtTest {
    private Analyzer analyzer;

    private Catalog catalog;

    @Mocked
    EditLog editLog;

    @Mocked
    private ConnectContext connectContext;

    @Mocked
    private Auth auth;

    @Before
    public void setUp() {
        catalog = Deencapsulation.newInstance(Catalog.class);
        analyzer = new Analyzer(catalog, connectContext);

        Database db = new Database(50000L, "testCluster:testDb");

        Column column1 = new Column("col1", Type.BIGINT);
        Column column2 = new Column("col2", Type.DOUBLE);

        List<Column> baseSchema = new LinkedList<Column>();
        baseSchema.add(column1);
        baseSchema.add(column2);

        OlapTable table = new OlapTable(30000, "testTbl",
                baseSchema, KeysType.AGG_KEYS, new SinglePartitionInfo(), null);
        db.createTable(table);

        new Expectations(auth) {
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

        new Expectations(editLog) {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                editLog.logModifyViewDef((AlterViewInfo) any);
                minTimes = 0;
            }
        };

        Deencapsulation.setField(catalog, "editLog", editLog);

        new MockUp<Catalog>() {
            @Mock
            Catalog getCurrentCatalog() {
                return catalog;
            }

            @Mock
            Auth getAuth() {
                return auth;
            }

            @Mock
            Database getDb(long dbId) {
                return db;
            }

            @Mock
            Database getDb(String dbName) {
                return db;
            }
        };

        new MockUp<Analyzer>() {
            @Mock
            String getClusterName() {
                return "testCluster";
            }
        };
    }

    @Test
    public void testNormal() {
        String originStmt = "select col1 as c1, sum(col2) as c2 from testDb.testTbl group by col1";
        View view = new View(30000L, "testView", null);
        view.setInlineViewDefWithSqlMode("select col1 as c1, sum(col2) as c2 from testDb.testTbl group by col1", 0L);
        try {
            view.init();
        } catch (UserException e) {
            Assert.fail();
        }

        Database db = analyzer.getCatalog().getDb("testDb");
        db.createTable(view);

        Assert.assertEquals(originStmt, view.getInlineViewDef());

        String alterStmt =
                "with testTbl_cte (w1, w2) as (select col1, col2 from testDb.testTbl) select w1 as c1, sum(w2) as c2 from testTbl_cte where w1 > 10 group by w1 order by w1";
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(alterStmt)));
        QueryStmt alterQueryStmt = null;
        try {
            alterQueryStmt = (QueryStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        ColWithComment col1 = new ColWithComment("h1", null);
        ColWithComment col2 = new ColWithComment("h2", null);

        AlterViewStmt alterViewStmt =
                new AlterViewStmt(new TableName("testDb", "testView"), Lists.newArrayList(col1, col2), alterQueryStmt);
        try {
            alterViewStmt.analyze(analyzer);
            Catalog catalog1 = analyzer.getCatalog();
            if (catalog1 == null) {
                System.out.println("cmy get null");
                return;
            }
            catalog1.alterView(alterViewStmt);
        } catch (UserException e) {
            Assert.fail();
        }

        View newView = (View) db.getTable("testView");

        Assert.assertEquals(
                "WITH testTbl_cte(w1, w2) AS (SELECT `col1` AS `col1`, `col2` AS `col2` FROM `testCluster:testDb`.`testTbl`)" +
                        " SELECT `w1` AS `h1`, sum(`w2`) AS `h2` FROM `testTbl_cte` WHERE `w1` > 10 GROUP BY `w1` ORDER BY `w1`",
                newView.getInlineViewDef());
    }
}
