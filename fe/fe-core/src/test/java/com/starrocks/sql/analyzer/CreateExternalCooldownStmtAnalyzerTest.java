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
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.NotSupportedException;


public class CreateExternalCooldownStmtAnalyzerTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateExternalCooldownStmt() {
        CreateExternalCooldownStmt stmt = (CreateExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt, connectContext));

        CreateExternalCooldownStmt stmt1 = (CreateExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt1, new ConnectContext()));
    }

    @Test
    public void testCancelExternalCooldownStmt() {
        CancelExternalCooldownStmt stmt = (CancelExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "CANCEL COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt, connectContext));

        CancelExternalCooldownStmt stmt1 = (CancelExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "CANCEL COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt1, connectContext));
    }

    @Test
    public void testConstructor() {
        Assert.assertThrows(NotSupportedException.class, ExternalCooldownAnalyzer::new);
    }

    @Test
    public void testDbNotSet() {
        ConnectContext context = new ConnectContext();
        CreateExternalCooldownStmt stmt = (CreateExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt, context));

        CancelExternalCooldownStmt stmt1 = (CancelExternalCooldownStmt) com.starrocks.sql.parser.SqlParser.parse(
                "CANCEL COOLDOWN TABLE tbl1", 32).get(0);
        Assert.assertThrows(SemanticException.class, () -> ExternalCooldownAnalyzer.analyze(stmt1, context));
    }
}
