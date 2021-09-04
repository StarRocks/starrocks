// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowMaterializedViewTest.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowMaterializedViewTest {
    private Analyzer analyzer;
    @Mocked
    Auth auth;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        new Expectations() {
            {
                auth.checkDbPriv(ConnectContext.get(), anyString, PrivPredicate.SHOW);
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException {
        // use default database
        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("test");
        try {
            stmt.analyze(analyzer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertEquals("testCluster:test", stmt.getDb());
        Assert.assertEquals("SHOW MATERIALIZED VIEW FROM testCluster:test", stmt.toString());
    }
}
