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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Regression tests for the TRUNCATE TABLE authorization bypass:
 *
 * <p>Before the fix, {@link AuthorizerStmtVisitor#visitTruncateTableStatement} unconditionally used
 * {@code context.getCurrentCatalog()} to construct the {@link TableName} that was passed to
 * {@link Authorizer#checkTableAction}. This caused the privilege check to be routed against the
 * <i>session</i> catalog while the DDL executor still operated on the <i>statement</i> catalog,
 * which a user could exploit via:
 *
 * <pre>
 *   SET CATALOG ext;
 *   TRUNCATE TABLE default_catalog.db.t;
 * </pre>
 *
 * After the fix the visitor must derive the catalog from the statement itself
 * (and only fall back to the session catalog when the statement omits it).
 *
 * <p>These tests exercise the visitor as a white-box: they intercept
 * {@code Authorizer.checkTableAction(ctx, tableName, privType)} with JMockit and assert that the
 * {@link TableName} forwarded to the privilege layer matches the catalog/db/tbl of the SQL text
 * regardless of what the session catalog is set to.
 */
public class AuthorizerTruncateTableTest {

    private static ConnectContext connectContext;

    /** Captures every {@code TableName} that {@code Authorizer.checkTableAction} is invoked with. */
    private final List<TableName> capturedTableNames = new ArrayList<>();
    /** Captures every {@code PrivilegeType} that {@code Authorizer.checkTableAction} is invoked with. */
    private final List<PrivilegeType> capturedPrivilegeTypes = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() {
        capturedTableNames.clear();
        capturedPrivilegeTypes.clear();
        // Stub the privilege check so that we can observe what TableName the visitor forwards
        // without actually consulting AuthorizationMgr / objects-must-exist semantics.
        new MockUp<Authorizer>() {
            @Mock
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
                capturedTableNames.add(tableName);
                capturedPrivilegeTypes.add(privilegeType);
                // Return normally => "allowed". Individual test cases that need a deny
                // path will override this MockUp locally.
            }
        };
    }

    private TruncateTableStmt parseAndAnalyze(String sql, ConnectContext ctx) {
        TruncateTableStmt stmt = (TruncateTableStmt)
                SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        // Mirror the production pipeline (StatementPlanner.plan: analyze before authorize).
        // TruncateTableAnalyzer fully qualifies catalog/db/tbl on the statement's TableRef.
        TruncateTableAnalyzer.analyze(stmt, ctx);
        return stmt;
    }

    private ConnectContext newCtxWithCatalog(String catalog, String db) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        ctx.setCurrentRoleIds(connectContext.getCurrentRoleIds());
        ctx.setCurrentCatalog(catalog);
        ctx.setDatabase(db);
        return ctx;
    }

    // -----------------------------------------------------------------------------------------
    // Cross-catalog TRUNCATE must be authorized against the STATEMENT catalog,
    // never the session catalog.
    // -----------------------------------------------------------------------------------------

    /**
     * <pre>SET CATALOG ext_iceberg; TRUNCATE TABLE default_catalog.db1.t;</pre>
     * Expected: privilege check is routed to {@code default_catalog.db1.t}, not to
     * {@code ext_iceberg.db1.t}.
     */
    @Test
    public void testTruncate_fullyQualified_overridesSessionCatalog() {
        ConnectContext ctx = newCtxWithCatalog("ext_iceberg", "db1");
        TruncateTableStmt stmt = parseAndAnalyze("TRUNCATE TABLE default_catalog.db1.t", ctx);

        new AuthorizerStmtVisitor().visitTruncateTableStatement(stmt, ctx);

        Assertions.assertEquals(1, capturedTableNames.size(),
                "checkTableAction should be invoked exactly once");
        TableName tn = capturedTableNames.get(0);
        Assertions.assertEquals("default_catalog", tn.getCatalog(),
                "Privilege check MUST use the catalog that appears in the SQL text, not the session catalog");
        Assertions.assertEquals("db1", tn.getDb());
        Assertions.assertEquals("t", tn.getTbl());
        Assertions.assertEquals(PrivilegeType.DELETE, capturedPrivilegeTypes.get(0));
    }

    /**
     * Statement omits the catalog AND the db => fall back to session catalog/db.
     * This is the legacy behavior that must remain intact after the fix.
     */
    @Test
    public void testTruncate_unqualified_fallsBackToSessionCatalogAndDb() {
        ConnectContext ctx = newCtxWithCatalog("default_catalog", "db1");
        TruncateTableStmt stmt = parseAndAnalyze("TRUNCATE TABLE t", ctx);

        new AuthorizerStmtVisitor().visitTruncateTableStatement(stmt, ctx);

        Assertions.assertEquals(1, capturedTableNames.size());
        TableName tn = capturedTableNames.get(0);
        Assertions.assertEquals("default_catalog", tn.getCatalog());
        Assertions.assertEquals("db1", tn.getDb());
        Assertions.assertEquals("t", tn.getTbl());
    }

    /**
     * Statement specifies db but not catalog. Catalog must come from the session,
     * db must come from the statement (must NOT be overridden by session.db).
     */
    @Test
    public void testTruncate_dbQualified_keepsStatementDbButFallsBackForCatalog() {
        ConnectContext ctx = newCtxWithCatalog("default_catalog", "session_db");
        TruncateTableStmt stmt = parseAndAnalyze("TRUNCATE TABLE db_in_stmt.t", ctx);

        new AuthorizerStmtVisitor().visitTruncateTableStatement(stmt, ctx);

        TableName tn = capturedTableNames.get(0);
        Assertions.assertEquals("default_catalog", tn.getCatalog(),
                "catalog should fall back to session catalog when statement omits it");
        Assertions.assertEquals("db_in_stmt", tn.getDb(),
                "db must come from the statement, even if session.db is set to something else");
        Assertions.assertEquals("t", tn.getTbl());
    }

    /**
     * Deny path: when the user has no privilege on the table that appears in the SQL text, the
     * visitor must surface the AccessDenied report referring to the <i>statement</i> catalog
     * (so the audit/error trail is accurate), even if the session is currently on a different
     * catalog.
     *
     * <p>This is white-box: we mock {@link AccessDeniedException#reportAccessDenied} directly to
     * capture the catalog argument it actually receives, instead of relying on string matching
     * against the thrown {@link com.starrocks.common.ErrorReportException} message. This way the
     * test pins down the precise routing semantics on the deny branch.
     */
    @Test
    public void testTruncate_deniedReport_usesStatementCatalog() {
        // 1) Force checkTableAction to throw, so the visitor enters its catch branch.
        new MockUp<Authorizer>() {
            @Mock
            public void checkTableAction(ConnectContext context, TableName tableName,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
                capturedTableNames.add(tableName);
                capturedPrivilegeTypes.add(privilegeType);
                throw new AccessDeniedException();
            }
        };

        // 2) Intercept reportAccessDenied to directly observe the catalog/priv/objectType
        //    that the visitor forwards. We deliberately do NOT throw, so the assertions below
        //    can run regardless of upstream ErrorReportException plumbing changes.
        final String[] reportedCatalog   = {null};
        final String[] reportedPriv      = {null};
        final String[] reportedObjType   = {null};
        final String[] reportedObject    = {null};
        new MockUp<AccessDeniedException>() {
            @Mock
            public void reportAccessDenied(String catalog, UserIdentity userIdentity,
                                                  Set<Long> roleIds, String privilegeType,
                                                  String objectType, String object) {
                reportedCatalog[0] = catalog;
                reportedPriv[0]    = privilegeType;
                reportedObjType[0] = objectType;
                reportedObject[0]  = object;
                // intentionally not throwing -- let the visitor return normally for assertion.
            }
        };

        ConnectContext ctx = newCtxWithCatalog("ext_iceberg", "db1");
        TruncateTableStmt stmt = parseAndAnalyze("TRUNCATE TABLE default_catalog.db1.victim", ctx);

        new AuthorizerStmtVisitor().visitTruncateTableStatement(stmt, ctx);

        // Primary assertion: the deny-side catalog routing must use the STATEMENT catalog.
        Assertions.assertEquals("default_catalog", reportedCatalog[0],
                "reportAccessDenied MUST receive the statement catalog, not the session catalog. Got: "
                        + reportedCatalog[0]);
        Assertions.assertNotEquals("ext_iceberg", reportedCatalog[0],
                "reportAccessDenied MUST NOT leak the session catalog into the audit trail.");

        // Secondary assertions: privilege type / object type / object name are correctly
        // forwarded so that the audit message is well-formed.
        Assertions.assertEquals(PrivilegeType.DELETE.name(), reportedPriv[0]);
        Assertions.assertEquals(ObjectType.TABLE.name(),     reportedObjType[0]);
        Assertions.assertEquals("victim",                    reportedObject[0]);

        // Tertiary assertion: the visitor must have actually consulted checkTableAction with
        // the STATEMENT catalog before being denied (covers the pre-deny path symmetry).
        Assertions.assertFalse(capturedTableNames.isEmpty(),
                "the visitor must have invoked checkTableAction before being denied");
        TableName tn = capturedTableNames.get(capturedTableNames.size() - 1);
        Assertions.assertEquals("default_catalog", tn.getCatalog(),
                "checkTableAction MUST also receive the statement catalog");
    }

    /**
     * Defensive sanity check: if a user issues TRUNCATE against a fully-qualified table in the
     * default internal catalog while session is on the same internal catalog, behavior is the
     * same as before the fix.
     */
    @Test
    public void testTruncate_internalCatalog_sameAsSession() {
        ConnectContext ctx = newCtxWithCatalog(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1");
        TruncateTableStmt stmt = parseAndAnalyze("TRUNCATE TABLE "
                + InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME + ".db1.t", ctx);

        new AuthorizerStmtVisitor().visitTruncateTableStatement(stmt, ctx);

        TableName tn = capturedTableNames.get(0);
        Assertions.assertEquals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, tn.getCatalog());
        Assertions.assertEquals("db1", tn.getDb());
        Assertions.assertEquals("t", tn.getTbl());
    }
}
