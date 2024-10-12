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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CancelExternalCooldownStmt;
import com.starrocks.sql.ast.CreateExternalCooldownStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateExternalCooldownStmtAnalyzerTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        AnalyzeTestUtil.init();
    }

    @Test(expected = Throwable.class)
    public void testCreateExternalCooldownStmt() {
        CreateExternalCooldownStmt stmt = (CreateExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "COOLDOWN TABLE tbl1", 32).get(0);
        ExternalCooldownAnalyzer.analyze(stmt, connectContext);
    }

    @Test(expected = Throwable.class)
    public void testCancelExternalCooldownStmt() {
        CancelExternalCooldownStmt stmt = (CancelExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "CANCEL COOLDOWN tbl1", 32).get(0);
        ExternalCooldownAnalyzer.analyze(stmt, connectContext);
    }
}
