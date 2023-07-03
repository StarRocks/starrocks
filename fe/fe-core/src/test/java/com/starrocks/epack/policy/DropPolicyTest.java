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

import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.epack.policy.TestUtils.assertThrows;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class DropPolicyTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testDropPolicy() throws Exception {
        CreatePolicyStmt createPolicyStmt = (CreatePolicyStmt) analyzeSuccess(
                "create masking policy mp as ( v1 bigint) returns bigint -> v1 + 1");
        DDLStmtExecutor.execute(createPolicyStmt, starRocksAssert.getCtx());

        assertThrows("Can't find policy mp.", SemanticException.class, () ->
                DDLStmtExecutor.execute(analyzeSuccess("drop row access policy mp"),
                        starRocksAssert.getCtx()));

        DDLStmtExecutor.execute(analyzeSuccess("drop masking policy mp"), starRocksAssert.getCtx());

        assertThrows("Can't find policy mp.", SemanticException.class, () ->
                DDLStmtExecutor.execute(analyzeSuccess("drop masking policy mp"), starRocksAssert.getCtx()));

        DDLStmtExecutor.execute(analyzeSuccess("drop masking policy if exists mp"), starRocksAssert.getCtx());

        createPolicyStmt = (CreatePolicyStmt) analyzeSuccess(
                "create row access policy rp as (v1 bigint) returns boolean -> true;");
        DDLStmtExecutor.execute(createPolicyStmt, starRocksAssert.getCtx());

        assertThrows("Can't find policy rp.", SemanticException.class, () ->
                DDLStmtExecutor.execute(analyzeSuccess("drop masking policy rp"),
                        starRocksAssert.getCtx()));

        DDLStmtExecutor.execute(analyzeSuccess("drop row access policy rp"), starRocksAssert.getCtx());

        assertThrows("Can't find policy rp.", SemanticException.class, () ->
                DDLStmtExecutor.execute(analyzeSuccess("drop row access  policy rp"), starRocksAssert.getCtx()));

        DDLStmtExecutor.execute(analyzeSuccess("drop row access  policy if exists rp"), starRocksAssert.getCtx());

        ShowExecutor showExecutor = new ShowExecutor(starRocksAssert.getCtx(),
                (ShowStmt) analyzeSuccess("show masking policies"));
        Assert.assertEquals(0, showExecutor.execute().getResultRows().size());

        showExecutor = new ShowExecutor(starRocksAssert.getCtx(),
                (ShowStmt) analyzeSuccess("show row access policies"));
        Assert.assertEquals(0, showExecutor.execute().getResultRows().size());
    }
}
