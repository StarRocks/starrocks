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

package com.starrocks.epack.policy;

import com.starrocks.common.DdlException;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.epack.policy.TestUtils.assertThrows;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CreatePolicyTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testCreatePolicy() throws Exception {
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) analyzeSuccess(
                "create masking policy mp as ( v1 bigint) returns bigint -> v1 + 1");
        DDLStmtExecutor.execute(createPolicyStmt, starRocksAssert.getCtx());

        assertThrows("Policy mp already exists", DdlException.class, () ->
                DDLStmtExecutor.execute(createPolicyStmt, starRocksAssert.getCtx()));

        DDLStmtExecutor.execute(analyzeSuccess(
                        "create masking policy if not exists mp as ( v1 bigint) returns bigint -> v1 + 1"),
                starRocksAssert.getCtx());
    }
}
