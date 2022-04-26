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

import com.google.common.collect.Lists;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.load.EtlJobType;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LoadStmtTest {
    private List<DataDescription> dataDescriptions;
    private Analyzer analyzer;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    DataDescription desc;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        dataDescriptions = Lists.newArrayList();
        dataDescriptions.add(desc);
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "default_cluster:user";

                desc.toSql();
                minTimes = 0;
                result = "XXX";
            }
        };
    }

    @Test
    public void testNormal(@Injectable DataDescription desc, @Mocked GlobalStateMgr globalStateMgr,
                           @Injectable ResourceMgr resourceMgr, @Injectable Auth auth)
            throws UserException, AnalysisException {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(desc);
        String resourceName = "spark0";
        SparkResource resource = new SparkResource(resourceName);

        new Expectations() {
            {
                desc.toSql();
                minTimes = 0;
                result = "XXX";
                globalStateMgr.getResourceMgr();
                result = resourceMgr;
                resourceMgr.getResource(resourceName);
                result = resource;
                globalStateMgr.getAuth();
                result = auth;
                auth.checkResourcePriv((ConnectContext) any, resourceName, PrivPredicate.USAGE);
                result = true;
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList, null, null, null);
        stmt.analyze(analyzer);
        Assert.assertEquals("testCluster:testDb", stmt.getLabel().getDbName());
        Assert.assertEquals(dataDescriptionList, stmt.getDataDescriptions());
        Assert.assertNull(stmt.getProperties());

        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n"
                + "(XXX)", stmt.toString());

        // test ResourceDesc
        stmt = new LoadStmt(new LabelName("testDb", "testLabel"), dataDescriptionList,
                new ResourceDesc(resourceName, null), null);
        stmt.analyze(analyzer);
        Assert.assertEquals(EtlJobType.SPARK, stmt.getResourceDesc().getEtlJobType());
        Assert.assertEquals("LOAD LABEL `testCluster:testDb`.`testLabel`\n(XXX)\nWITH RESOURCE 'spark0'",
                stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoData() throws UserException, AnalysisException {
        new Expectations() {
            {
                desc.analyze(anyString);
                minTimes = 0;
            }
        };

        LoadStmt stmt = new LoadStmt(new LabelName("testDb", "testLabel"), null, null, null, null);
        stmt.analyze(analyzer);

        Assert.fail("No exception throws.");
    }
}