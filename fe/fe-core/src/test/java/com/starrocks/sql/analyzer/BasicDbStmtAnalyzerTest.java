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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BasicDbStmtAnalyzerTest {

    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterAll
    public static void afterClass() throws Exception {
        // Cleanup is handled automatically by the test framework
    }

    @Test
    public void testUseDbStatementWithNullCatalog() throws Exception {
        // Test case to cover line 45 in BasicDbStmtAnalyzer.java
        // This should trigger the noCatalogSelected exception when current catalog is null
        UseDbStmt stmt = (UseDbStmt) SqlParser.parse("USE test_db", connectContext.getSessionVariable()).get(0);

        // Create a new context with null catalog to trigger the exception
        ConnectContext testContext = new ConnectContext();
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        // Set catalog to null to trigger the exception
        testContext.setCurrentCatalog(null);

        try {
            BasicDbStmtAnalyzer.analyze(stmt, testContext);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No catalog selected"));
        }
    }

    @Test
    public void testRecoverDbStatementWithNullCatalog() throws Exception {
        // Test case to cover line 53 in BasicDbStmtAnalyzer.java
        // This should trigger the noCatalogSelected exception when current catalog is null
        RecoverDbStmt stmt =
                (RecoverDbStmt) SqlParser.parse("RECOVER DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Create a new context with null catalog to trigger the exception
        ConnectContext testContext = new ConnectContext();
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        // Set catalog to null to trigger the exception
        testContext.setCurrentCatalog(null);

        try {
            BasicDbStmtAnalyzer.analyze(stmt, testContext);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No catalog selected"));
        }
    }

    @Test
    public void testShowCreateDbStatementWithNullCatalog() throws Exception {
        // Test case to cover line 61 in BasicDbStmtAnalyzer.java
        // This should trigger the noCatalogSelected exception when current catalog is null
        ShowCreateDbStmt stmt =
                (ShowCreateDbStmt) SqlParser.parse("SHOW CREATE DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Create a new context with null catalog to trigger the exception
        ConnectContext testContext = new ConnectContext();
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        // Set catalog to null to trigger the exception
        testContext.setCurrentCatalog(null);

        try {
            BasicDbStmtAnalyzer.analyze(stmt, testContext);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No catalog selected"));
        }
    }

    @Test
    public void testUseDbStatementWithValidCatalog() throws Exception {
        // Test case to ensure normal flow works when catalog is set
        UseDbStmt stmt = (UseDbStmt) SqlParser.parse("USE test_db", connectContext.getSessionVariable()).get(0);

        // Use the context with valid catalog
        connectContext.setCurrentCatalog("default_catalog");

        try {
            BasicDbStmtAnalyzer.analyze(stmt, connectContext);
            // Should not throw exception
        } catch (Exception e) {
            Assertions.fail("Should not throw exception when catalog is set: " + e.getMessage());
        }
    }

    @Test
    public void testRecoverDbStatementWithValidCatalog() throws Exception {
        // Test case to ensure normal flow works when catalog is set
        RecoverDbStmt stmt =
                (RecoverDbStmt) SqlParser.parse("RECOVER DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Use the context with valid catalog
        connectContext.setCurrentCatalog("default_catalog");

        try {
            BasicDbStmtAnalyzer.analyze(stmt, connectContext);
            // Should not throw exception
        } catch (Exception e) {
            Assertions.fail("Should not throw exception when catalog is set: " + e.getMessage());
        }
    }

    @Test
    public void testShowCreateDbStatementWithValidCatalog() throws Exception {
        // Test case to ensure normal flow works when catalog is set
        ShowCreateDbStmt stmt =
                (ShowCreateDbStmt) SqlParser.parse("SHOW CREATE DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Use the context with valid catalog
        connectContext.setCurrentCatalog("default_catalog");

        try {
            BasicDbStmtAnalyzer.analyze(stmt, connectContext);
            // Should not throw exception
        } catch (Exception e) {
            Assertions.fail("Should not throw exception when catalog is set: " + e.getMessage());
        }
    }
}