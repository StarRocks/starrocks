// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/AlterTableStmtTest.java

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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.MetaUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class AlterTableStmtTest {
    private Analyzer analyzer;

    @Mocked
    private Auth auth;

    @Mocked
    private MetaUtils metaUtils;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);

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
    public void testAddRollup() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new AddRollupClause("index1", Lists.newArrayList("col1", "col2"), null, "testTbl", null));
        ops.add(new AddRollupClause("index2", Lists.newArrayList("col2", "col3"), null, "testTbl", null));
        AlterTableStmt stmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        stmt.analyze(analyzer);
        Assert.assertEquals(
                "ALTER TABLE `testDb`.`testTbl` ADD ROLLUP `index1` (`col1`, `col2`) FROM `testTbl`, \n" +
                        " `index2` (`col2`, `col3`) FROM `testTbl`",
                stmt.toSql());
        Assert.assertEquals("default_cluster:testDb", stmt.getTbl().getDb());
        Assert.assertEquals(2, stmt.getOps().size());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        ops.add(new DropColumnClause("col1", "", null));
        AlterTableStmt stmt = new AlterTableStmt(null, ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testNoClause() throws UserException {
        List<AlterClause> ops = Lists.newArrayList();
        AlterTableStmt stmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }
}