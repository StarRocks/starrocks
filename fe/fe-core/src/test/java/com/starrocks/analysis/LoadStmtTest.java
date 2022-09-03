// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/LoadStmtTest.java

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

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.UserException;
import com.starrocks.load.EtlJobType;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class LoadStmtTest {

    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal(@Injectable DataDescription desc, @Mocked GlobalStateMgr globalStateMgr, @Injectable ResourceMgr resourceMgr, @Injectable
    Auth auth)
            throws UserException {
        String resourceName = "spark0";
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                globalStateMgr.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
                globalStateMgr.getAuth();
                result = auth;
                auth.checkResourcePriv((ConnectContext) any, resourceName, PrivPredicate.USAGE);
                result = true;
                auth.checkTblPriv((ConnectContext) any, "test",
                        "t0", PrivPredicate.LOAD);
                result = true;
            }
        };
        LoadStmt stmt = (LoadStmt) analyzeSuccess(
                "LOAD LABEL test.testLabel " +
                        "(DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`)");
        DataDescription dataDescription = stmt.getDataDescriptions().get(0);
        Assert.assertEquals("test", stmt.getLabel().getDbName());
        Assert.assertFalse(dataDescription.isLoadFromTable());
        Assert.assertTrue(dataDescription.isHadoopLoad());
        Assert.assertNull(stmt.getProperties());
        Assert.assertEquals("LOAD LABEL `test`.`testLabel`\n" +
                "(DATA INFILE ('hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file') INTO TABLE t0)", stmt.toString());

        // test ResourceDesc
        stmt = (LoadStmt) analyzeSuccess(
                "LOAD LABEL test.testLabel " +
                        "(DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) " +
                        "WITH RESOURCE spark0 " +
                        "PROPERTIES (\"strict_mode\"=\"true\")");
        Assert.assertEquals(EtlJobType.SPARK, stmt.getResourceDesc().getEtlJobType());
        Assert.assertEquals("root", stmt.getUser());
        Assert.assertEquals(ImmutableSet.of("strict_mode"), stmt.getProperties().keySet());
    }

    @Test
    public void testNoData() {
        analyzeFail("LOAD LABEL test.testLabel", "No data file in load statement.");
    }

    @Test
    public void testNoDb() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("LOAD LABEL testLabel", "No database selected");
    }

    @Test
    public void testMultiTable() {
        analyzeFail(
                "LOAD LABEL testLabel (DATA FROM TABLE t0 INTO TABLE t1, DATA FROM TABLE t2 INTO TABLE t1)",
                "Only support one olap table load from one external table");
    }

    @Test
    public void testNoSparkLoad() {
        analyzeFail(
                "LOAD LABEL testLabel (DATA FROM TABLE t0 INTO TABLE t1)",
                "Load from table should use Spark Load");
    }
}