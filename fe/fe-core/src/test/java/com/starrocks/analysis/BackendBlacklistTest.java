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

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class BackendBlacklistTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testBackendBlacklist() {
        AddBackendBlackListStmt st = (AddBackendBlackListStmt) analyzeSuccess("ADD BACKEND BLACKLIST 1, 2, 3");
        Assert.assertEquals(1L, st.getBackendIds().get(0).longValue());
        Assert.assertEquals(3L, st.getBackendIds().get(2).longValue());

        DelBackendBlackListStmt del = (DelBackendBlackListStmt) analyzeSuccess("DELETE BACKEND BLACKLIST 1, 2, 3");
        Assert.assertEquals(1L, del.getBackendIds().get(0).longValue());
        Assert.assertEquals(3L, del.getBackendIds().get(2).longValue());
        analyzeSuccess("SHOW BACKEND BLACKLIST");

        analyzeFail("ADD BACKEND BLACKLIST aa");
        analyzeFail("ADD BACKEND BLACKLIST '1'");
        analyzeFail("ADD BACKEND BLACKLIST 1.0");
        analyzeFail("DELETE BACKEND BLACKLIST 'a',");
    }
}
