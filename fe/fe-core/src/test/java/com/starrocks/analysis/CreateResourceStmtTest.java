// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateResourceStmtTest.java

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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Resource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
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

import java.util.Map;

public class CreateResourceStmtTest {
    private Analyzer analyzer;

    @Before()
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer();
    }

    @Test
    public void testNormal(@Mocked GlobalStateMgr globalStateMgr, @Injectable Auth auth) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // spark resource
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "spark");
        CreateResourceStmt stmt = new CreateResourceStmt(true, "spark0", properties);
        stmt.analyze(analyzer);
        Assert.assertEquals("spark0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.SPARK, stmt.getResourceType());
        Assert.assertEquals("CREATE EXTERNAL RESOURCE 'spark0' PROPERTIES(\"type\"  =  \"spark\")", stmt.toSql());

        // hive resource
        properties.clear();
        properties.put("type", "hive");
        stmt = new CreateResourceStmt(true, "hive0", properties);
        stmt.analyze(analyzer);
        Assert.assertEquals("hive0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.HIVE, stmt.getResourceType());
        Assert.assertEquals("CREATE EXTERNAL RESOURCE 'hive0' PROPERTIES(\"type\"  =  \"hive\")", stmt.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testUnsupportedResourceType(@Mocked GlobalStateMgr globalStateMgr, @Injectable Auth auth)
            throws UserException {
        new Expectations() {
            {
                globalStateMgr.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hadoop");
        CreateResourceStmt stmt = new CreateResourceStmt(true, "hadoop0", properties);
        stmt.analyze(analyzer);
    }
}