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

package com.starrocks.context;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.context.AlterContextBaseStmt;
import com.starrocks.sql.ast.context.CreateContextBaseStmt;
import com.starrocks.sql.ast.context.CreateContextCollectionStmt;
import com.starrocks.sql.ast.context.CreateRetrievalProfileStmt;
import com.starrocks.sql.ast.context.CreateWorkspaceStmt;
import com.starrocks.sql.ast.context.DropContextBaseStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test that exercises the full FE context control-plane stack end-to-end:
 * <ol>
 *   <li>Grammar: SQL text parses into the expected AST nodes</li>
 *   <li>Analyzer: {@code ContextStmtAnalyzer} runs via the dispatcher</li>
 *   <li>Executor: {@code DDLStmtExecutor} dispatches to {@link ContextMgr}</li>
 *   <li>Manager: in-memory state is updated</li>
 * </ol>
 *
 * <p>Wider probe than the parser-only {@code ContextParserTest} — it boots the UT frame so
 * {@code GlobalStateMgr.getCurrentState()} resolves, the edit log accepts writes, etc., and then
 * runs semantic-context statements through the same path a production request would take.
 */
public class ContextMgrIntegrationTest {

    private static ConnectContext ctx;
    @SuppressWarnings("unused")
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(ctx);
    }

    @Test
    public void testCreateContextBaseRoundTrip() throws Exception {
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        int baseline = mgr.listContextBases().size();

        execute("CREATE CONTEXTBASE test_cb_roundtrip PROPERTIES (\"owner\" = \"alice\")");

        Assertions.assertEquals(baseline + 1, mgr.listContextBases().size());
        ContextMgr.ContextBaseMeta meta = mgr.getContextBase("test_cb_roundtrip");
        Assertions.assertNotNull(meta);
        Assertions.assertEquals("alice", meta.getProperties().get("owner"));

        execute("DROP CONTEXTBASE test_cb_roundtrip");
        Assertions.assertEquals(baseline, mgr.listContextBases().size());
        Assertions.assertNull(mgr.getContextBase("test_cb_roundtrip"));
    }

    @Test
    public void testCreateContextBaseRejectsRemovedProperties() throws Exception {
        Assertions.assertThrows(Exception.class,
                () -> execute("CREATE CONTEXTBASE test_cb_rejected "
                        + "PROPERTIES (\"default_consistency\" = \"STRICT\")"));
    }

    @Test
    public void testCreateContextBaseIfNotExistsIsIdempotent() throws Exception {
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();

        execute("CREATE CONTEXTBASE IF NOT EXISTS test_cb_ifne");
        long firstId = mgr.getContextBase("test_cb_ifne").getId();

        // Second call must be a no-op: same id returned, no new row created.
        execute("CREATE CONTEXTBASE IF NOT EXISTS test_cb_ifne");
        Assertions.assertEquals(firstId, mgr.getContextBase("test_cb_ifne").getId());

        execute("DROP CONTEXTBASE test_cb_ifne");
    }

    @Test
    public void testFullContextHierarchy() throws Exception {
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();

        execute("CREATE CONTEXTBASE test_full_hier");
        execute("CREATE CONTEXT COLLECTION test_full_hier.pipeline_rules "
                + "PROPERTIES (\"collection_type\" = \"knowledge\")");
        execute("CREATE RETRIEVAL PROFILE test_full_hier_profile "
                + "PROPERTIES (\"fusion_mode\" = \"RRF\")");

        Assertions.assertEquals(1, mgr.listCollections("test_full_hier").size());
        Assertions.assertNotNull(mgr.getRetrievalProfile("test_full_hier_profile"));

        execute("DROP CONTEXT COLLECTION test_full_hier.pipeline_rules");
        execute("DROP RETRIEVAL PROFILE test_full_hier_profile");
        execute("DROP CONTEXTBASE test_full_hier");

        Assertions.assertNull(mgr.getContextBase("test_full_hier"));
        Assertions.assertNull(mgr.getRetrievalProfile("test_full_hier_profile"));
    }

    @Test
    public void testAstShapesMatchGrammar() throws Exception {
        // Spot-check that the grammar produces the expected AST subtypes. This catches regressions
        // where a grammar edit silently starts routing a statement to the wrong AST node.
        StatementBase cb = UtFrameUtils.parseStmtWithNewParser(
                "CREATE CONTEXTBASE ast_shape_test", ctx);
        Assertions.assertTrue(cb instanceof CreateContextBaseStmt);

        StatementBase alter = UtFrameUtils.parseStmtWithNewParser(
                "ALTER CONTEXTBASE ast_shape_test SET (\"owner\" = \"alice\")", ctx);
        Assertions.assertTrue(alter instanceof AlterContextBaseStmt);

        StatementBase drop = UtFrameUtils.parseStmtWithNewParser(
                "DROP CONTEXTBASE ast_shape_test", ctx);
        Assertions.assertTrue(drop instanceof DropContextBaseStmt);

        StatementBase col = UtFrameUtils.parseStmtWithNewParser(
                "CREATE CONTEXT COLLECTION ast_shape_test.foo", ctx);
        Assertions.assertTrue(col instanceof CreateContextCollectionStmt);

        StatementBase ws = UtFrameUtils.parseStmtWithNewParser(
                "CREATE WORKSPACE ast_shape_test.foo.session_1", ctx);
        Assertions.assertTrue(ws instanceof CreateWorkspaceStmt);

        StatementBase prof = UtFrameUtils.parseStmtWithNewParser(
                "CREATE RETRIEVAL PROFILE ast_shape_profile", ctx);
        Assertions.assertTrue(prof instanceof CreateRetrievalProfileStmt);
    }

    private void execute(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Analyzer.analyze(stmt, ctx);
        DDLStmtExecutor.execute(stmt, ctx);
    }
}
