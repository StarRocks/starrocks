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
import com.starrocks.sql.ast.AddComputeNodeBlackListStmt;
import com.starrocks.sql.ast.DelComputeNodeBlackListStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ComputeNodeBlacklistTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testComputeNodeBlacklist() {
        AddComputeNodeBlackListStmt st = (AddComputeNodeBlackListStmt) analyzeSuccess("ADD COMPUTE NODE BLACKLIST 1, 2, 3");
        Assertions.assertEquals(1L, st.getComputeNodeIds().get(0).longValue());
        Assertions.assertEquals(3L, st.getComputeNodeIds().get(2).longValue());

        DelComputeNodeBlackListStmt del = (DelComputeNodeBlackListStmt) analyzeSuccess("DELETE COMPUTE NODE BLACKLIST 1, 2, 3");
        Assertions.assertEquals(1L, del.getComputeNodeIds().get(0).longValue());
        Assertions.assertEquals(3L, del.getComputeNodeIds().get(2).longValue());
        analyzeSuccess("SHOW COMPUTE NODE BLACKLIST");

        analyzeFail("ADD COMPUTE NODE BLACKLIST aa");
        analyzeFail("ADD COMPUTE NODE BLACKLIST '1'");
        analyzeFail("ADD COMPUTE NODE BLACKLIST 1.0");
        analyzeFail("DELETE COMPUTE NODE BLACKLIST 'a',");
    }
}
