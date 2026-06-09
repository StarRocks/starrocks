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

import com.starrocks.authorization.ColumnPrivilege;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.plan.ConnectorPlanTestBase.newFolder;

public class AuthorizerStmtVisitorTest {

    private static ConnectContext connectContext;

    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
    }

    @AfterAll
    public static void afterClass() throws Exception {
        // Cleanup is handled automatically by the test framework
    }

    @Test
    public void testShowCreateDbStatementWithNullCatalog() throws Exception {
        // Test case to cover line 549 in AuthorizerStmtVisitor.java
        // This should trigger the noCatalogSelected exception when current catalog is null
        ShowCreateDbStmt stmt =
                (ShowCreateDbStmt) SqlParser.parse("SHOW CREATE DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Create a new context with null catalog to trigger the exception
        ConnectContext testContext = new ConnectContext();
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        // Set catalog to null to trigger the exception
        testContext.setCurrentCatalog(null);

        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();

        try {
            visitor.visitShowCreateDbStatement(stmt, testContext);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No catalog selected"));
        }
    }

    @Test
    public void testRecoverDbStatementWithNullCatalog() throws Exception {
        // Test case to cover line 567 in AuthorizerStmtVisitor.java
        // This should trigger the noCatalogSelected exception when current catalog is null
        RecoverDbStmt stmt =
                (RecoverDbStmt) SqlParser.parse("RECOVER DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Create a new context with null catalog to trigger the exception
        ConnectContext testContext = new ConnectContext();
        testContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        testContext.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        // Set catalog to null to trigger the exception
        testContext.setCurrentCatalog(null);

        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();

        try {
            visitor.visitRecoverDbStatement(stmt, testContext);
            Assertions.fail("Expected SemanticException to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No catalog selected"));
        }
    }

    @Test
    public void testShowCreateDbStatementWithValidCatalog() throws Exception {
        // Test case to ensure normal flow works when catalog is set
        ShowCreateDbStmt stmt =
                (ShowCreateDbStmt) SqlParser.parse("SHOW CREATE DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Use the context with valid catalog
        connectContext.setCurrentCatalog("default_catalog");

        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();

        try {
            visitor.visitShowCreateDbStatement(stmt, connectContext);
            // Should not throw noCatalogSelected exception (may throw other exceptions due to missing database)
        } catch (Exception e) {
            // Should not be a noCatalogSelected exception
            Assertions.assertFalse(e.getMessage().contains("No catalog selected"),
                    "Should not throw noCatalogSelected exception when catalog is set: " + e.getMessage());
        }
    }

    @Test
    public void testRecoverDbStatementWithValidCatalog() throws Exception {
        // Test case to ensure normal flow works when catalog is set
        RecoverDbStmt stmt =
                (RecoverDbStmt) SqlParser.parse("RECOVER DATABASE test_db", connectContext.getSessionVariable()).get(0);

        // Use the context with valid catalog
        connectContext.setCurrentCatalog("default_catalog");

        AuthorizerStmtVisitor visitor = new AuthorizerStmtVisitor();

        try {
            visitor.visitRecoverDbStatement(stmt, connectContext);
            // Should not throw noCatalogSelected exception (may throw other exceptions due to missing database)
        } catch (Exception e) {
            // Should not be a noCatalogSelected exception
            Assertions.assertFalse(e.getMessage().contains("No catalog selected"),
                    "Should not throw noCatalogSelected exception when catalog is set: " + e.getMessage());
        }
    }

    @Test
    public void testMergeIntoPureInsertChecksOnlyInsertPrivilege() {
        // Source is a real table so checkSelectTableAction has a referenced
        // relation to fire SELECT against. Without an analyzed QueryStatement
        // the SELECT/column path silently no-ops, masking regressions in that
        // branch even though INSERT is still recorded.
        MergeIntoStmt stmt = (MergeIntoStmt) SqlParser.parse(
                "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                        "USING iceberg0.partitioned_db.t1_v2 AS s " +
                        "ON t.id = s.id " +
                        "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)",
                connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);

        List<PrivilegeType> checkedPrivileges = new ArrayList<>();

        try (MockedStatic<Authorizer> authorizerMockedStatic = Mockito.mockStatic(Authorizer.class);
                MockedStatic<ColumnPrivilege> columnPrivilegeMockedStatic = Mockito.mockStatic(ColumnPrivilege.class)) {
            authorizerMockedStatic.when(() -> Authorizer.checkTableAction(
                    Mockito.any(ConnectContext.class), Mockito.any(TableName.class), Mockito.any(PrivilegeType.class)))
                    .thenAnswer(invocation -> {
                        checkedPrivileges.add(invocation.getArgument(2));
                        return null;
                    });

            new AuthorizerStmtVisitor().visitMergeIntoStatement(stmt, connectContext);

            // Verify the SELECT/column-privilege path actually executed —
            // without Analyzer.analyze the visitor sees a null QueryStatement
            // and silently skips this call.
            columnPrivilegeMockedStatic.verify(() -> ColumnPrivilege.check(
                    Mockito.any(ConnectContext.class),
                    Mockito.any(),
                    Mockito.anyList()));
        }

        // Pure NOT MATCHED INSERT: target needs INSERT only; neither UPDATE
        // nor DELETE may be checked.
        Assertions.assertTrue(checkedPrivileges.contains(PrivilegeType.INSERT),
                "INSERT must be checked on the target table");
        Assertions.assertFalse(checkedPrivileges.contains(PrivilegeType.UPDATE),
                "UPDATE must NOT be checked for a pure NOT MATCHED INSERT clause");
        Assertions.assertFalse(checkedPrivileges.contains(PrivilegeType.DELETE),
                "DELETE must NOT be checked for a pure NOT MATCHED INSERT clause");
    }

    @Test
    public void testMergeSelfMergeDoesNotExcludeTargetFromSelectCheck() {
        // Self-merge regression: when the source references the same table as
        // the target, the visitor must NOT exclude the target from the SELECT /
        // column-privilege walk — otherwise a user with MERGE action privilege
        // but no SELECT can read target columns through source expressions like
        // SET data = s.secret_col.
        MergeIntoStmt stmt = (MergeIntoStmt) SqlParser.parse(
                "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                        "USING iceberg0.unpartitioned_db.t0_v2 AS s " +
                        "ON t.id = s.id " +
                        "WHEN MATCHED THEN UPDATE SET data = s.data",
                connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, connectContext);

        List<List<TableName>> capturedExcludes = new ArrayList<>();

        try (MockedStatic<Authorizer> authorizerMockedStatic = Mockito.mockStatic(Authorizer.class);
                MockedStatic<ColumnPrivilege> columnPrivilegeMockedStatic = Mockito.mockStatic(ColumnPrivilege.class)) {
            // Authorizer.checkTableAction is a no-op via default static mocking.
            columnPrivilegeMockedStatic.when(() -> ColumnPrivilege.check(
                    Mockito.any(ConnectContext.class),
                    Mockito.any(QueryStatement.class),
                    Mockito.anyList()))
                    .thenAnswer(invocation -> {
                        capturedExcludes.add(invocation.getArgument(2));
                        return null;
                    });

            new AuthorizerStmtVisitor().visitMergeIntoStatement(stmt, connectContext);
        }

        Assertions.assertEquals(1, capturedExcludes.size(),
                "ColumnPrivilege.check must be invoked exactly once for MERGE");
        Assertions.assertTrue(capturedExcludes.get(0).isEmpty(),
                "Self-merge must not exclude the target table; got excludeTables=" + capturedExcludes.get(0));
    }
}
