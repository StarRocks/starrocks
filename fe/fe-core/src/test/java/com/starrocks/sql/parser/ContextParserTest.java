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

package com.starrocks.sql.parser;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.context.AlterContextBaseRenameStmt;
import com.starrocks.sql.ast.context.AlterContextBaseStmt;
import com.starrocks.sql.ast.context.ContextDeleteStmt;
import com.starrocks.sql.ast.context.ContextUpsertStmt;
import com.starrocks.sql.ast.context.CreateContextBaseStmt;
import com.starrocks.sql.ast.context.CreateContextCollectionStmt;
import com.starrocks.sql.ast.context.CreateRetrievalProfileStmt;
import com.starrocks.sql.ast.context.CreateWorkspaceStmt;
import com.starrocks.sql.ast.context.DropContextBaseStmt;
import com.starrocks.sql.ast.context.ShowContextBasesStmt;
import com.starrocks.sql.ast.context.ShowContextCollectionsStmt;
import com.starrocks.sql.ast.context.ShowContextStatusStmt;
import com.starrocks.sql.ast.context.ShowContextWorkspacesStmt;
import com.starrocks.sql.ast.context.WorkspaceUpsertStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Parser coverage for the semantic-context DDL / DML / SHOW statements introduced in Milestone 1.
 */
public class ContextParserTest {

    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
    }

    private Object parse(String sql) {
        return SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode());
    }

    @Test
    public void testCreateContextBase() {
        CreateContextBaseStmt stmt = (CreateContextBaseStmt) parse(
                "CREATE CONTEXTBASE sales_ai PROPERTIES(\"default_consistency\" = \"STRICT\")");
        Assertions.assertEquals("sales_ai", stmt.getName().getName());
        Assertions.assertFalse(stmt.isIfNotExists());
        Assertions.assertEquals("STRICT", stmt.getProperties().get("default_consistency"));
    }

    @Test
    public void testCreateContextBaseIfNotExists() {
        CreateContextBaseStmt stmt = (CreateContextBaseStmt) parse(
                "CREATE CONTEXTBASE IF NOT EXISTS sales_ai");
        Assertions.assertTrue(stmt.isIfNotExists());
    }

    @Test
    public void testAlterContextBase() {
        AlterContextBaseStmt stmt = (AlterContextBaseStmt) parse(
                "ALTER CONTEXTBASE sales_ai SET (\"default_consistency\" = \"PRIMARY_CONSISTENT\")");
        Assertions.assertEquals("sales_ai", stmt.getName().getName());
        Assertions.assertEquals("PRIMARY_CONSISTENT", stmt.getProperties().get("default_consistency"));
    }

    @Test
    public void testAlterContextBaseRename() {
        AlterContextBaseRenameStmt stmt = (AlterContextBaseRenameStmt) parse(
                "ALTER CONTEXTBASE sales_ai RENAME TO sales_ai_v2");
        Assertions.assertEquals("sales_ai", stmt.getName().getName());
        Assertions.assertEquals("sales_ai_v2", stmt.getNewName());
    }

    @Test
    public void testDropContextBase() {
        DropContextBaseStmt stmt = (DropContextBaseStmt) parse(
                "DROP CONTEXTBASE IF EXISTS sales_ai");
        Assertions.assertEquals("sales_ai", stmt.getName().getName());
        Assertions.assertTrue(stmt.isIfExists());
    }

    @Test
    public void testCreateCollection() {
        CreateContextCollectionStmt stmt = (CreateContextCollectionStmt) parse(
                "CREATE CONTEXT COLLECTION sales_ai.pipeline_rules PROPERTIES("
                        + "\"collection_type\" = \"knowledge\", \"default_token_budget\" = \"4000\")");
        Assertions.assertEquals("sales_ai", stmt.getName().getContextBase());
        Assertions.assertEquals("pipeline_rules", stmt.getName().getCollection());
        Assertions.assertEquals("knowledge", stmt.getProperties().get("collection_type"));
    }

    @Test
    public void testCreateWorkspace() {
        CreateWorkspaceStmt stmt = (CreateWorkspaceStmt) parse(
                "CREATE WORKSPACE sales_ai.pipeline_rules.session_123 PROPERTIES(\"ttl_hours\" = \"24\")");
        Assertions.assertEquals("session_123", stmt.getName().getWorkspace());
        Assertions.assertEquals("24", stmt.getProperties().get("ttl_hours"));
    }

    @Test
    public void testCreateRetrievalProfile() {
        CreateRetrievalProfileStmt stmt = (CreateRetrievalProfileStmt) parse(
                "CREATE RETRIEVAL PROFILE balanced_v1 PROPERTIES("
                        + "\"fusion_mode\" = \"RRF\", \"text_weight\" = \"0.35\")");
        Assertions.assertEquals("balanced_v1", stmt.getName());
        Assertions.assertEquals("RRF", stmt.getProperties().get("fusion_mode"));
    }

    @Test
    public void testShowContextBases() {
        ShowContextBasesStmt stmt = (ShowContextBasesStmt) parse("SHOW CONTEXTBASES");
        Assertions.assertNotNull(stmt);
    }

    @Test
    public void testShowContextBasesLike() {
        ShowContextBasesStmt stmt = (ShowContextBasesStmt) parse("SHOW CONTEXTBASES LIKE 'sales%'");
        Assertions.assertEquals("sales%", stmt.getLikePattern());
    }

    @Test
    public void testShowCollections() {
        ShowContextCollectionsStmt stmt = (ShowContextCollectionsStmt) parse(
                "SHOW COLLECTIONS FROM sales_ai");
        Assertions.assertEquals("sales_ai", stmt.getContextBase());
    }

    @Test
    public void testShowWorkspaces() {
        ShowContextWorkspacesStmt stmt = (ShowContextWorkspacesStmt) parse(
                "SHOW WORKSPACES FROM sales_ai");
        Assertions.assertEquals("sales_ai", stmt.getContextBase());
    }

    @Test
    public void testShowContextStatus() {
        ShowContextStatusStmt stmt = (ShowContextStatusStmt) parse(
                "SHOW CONTEXT STATUS FROM sales_ai");
        Assertions.assertEquals("sales_ai", stmt.getContextBase());
    }

    @Test
    public void testContextUpsert() {
        ContextUpsertStmt stmt = (ContextUpsertStmt) parse(
                "CONTEXT UPSERT INTO sales_ai.pipeline_rules ENTITY ("
                        + "entity_key = 'smb_baseline', entity_type = 'page', preview = 'x', content = 'body') "
                        + "OPTIONS (consistency = 'STRICT')");
        Assertions.assertEquals("sales_ai", stmt.getCollection().getContextBase());
        Assertions.assertEquals("pipeline_rules", stmt.getCollection().getCollection());
        Assertions.assertTrue(stmt.getEntityArgs().containsKey("entity_key"));
        Assertions.assertTrue(stmt.getOptions().containsKey("consistency"));
    }

    @Test
    public void testContextUpsertWithEdgesAndOptions() {
        // Regression: when both EDGES and OPTIONS are present the AstBuilder used to index
        // namedArgumentList at the wrong slot, causing OPTIONS to be misread or NPE.
        ContextUpsertStmt stmt = (ContextUpsertStmt) parse(
                "CONTEXT UPSERT INTO sales_ai.pipeline_rules ENTITY ("
                        + "entity_key = 'smb_baseline', content = 'body') "
                        + "EDGES (101, 102, 103) "
                        + "OPTIONS (consistency = 'STRICT')");
        Assertions.assertEquals(3, stmt.getEdges().size());
        Assertions.assertTrue(stmt.getOptions().containsKey("consistency"));
    }

    @Test
    public void testContextDelete() {
        ContextDeleteStmt stmt = (ContextDeleteStmt) parse(
                "CONTEXT DELETE FROM sales_ai.pipeline_rules WHERE entity_key = 'smb_baseline'");
        Assertions.assertEquals("pipeline_rules", stmt.getCollection().getCollection());
        Assertions.assertNotNull(stmt.getPredicate());
    }

    @Test
    public void testWorkspaceUpsert() {
        WorkspaceUpsertStmt stmt = (WorkspaceUpsertStmt) parse(
                "WORKSPACE UPSERT INTO sales_ai.pipeline_rules.session_123 "
                        + "OBJECT (object_id = 'scratch.001', object_type = 'draft_summary', priority = 0.9)");
        Assertions.assertEquals("session_123", stmt.getWorkspace().getWorkspace());
        Assertions.assertTrue(stmt.getObjectArgs().containsKey("object_id"));
    }

    // ----- Context TVF parser surface -----
    // All context TVFs now stay as TableFunctionRelation at parse time and are materialized later
    // by QueryAnalyzer through ContextTvfRelationResolver so SQL and REST share one FE contract
    // implementation.

    @Test
    public void testReadCollectionStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(read_collection(123))"),
                "read_collection should stay a TVF at parse time");
    }

    @Test
    public void testReadContextBaseStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(read_contextbase(456))"),
                "read_contextbase should stay a TVF at parse time");
    }

    @Test
    public void testEntityHistoryStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(entity_history(789))"),
                "entity_history should stay a TVF at parse time");
    }

    @Test
    public void testContextGetStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(context_get(101))"),
                "context_get should stay a TVF at parse time");
    }

    @Test
    public void testGraphExpandRemainsTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(graph_expand(101, 1))"),
                "graph_expand should stay a TVF at parse time");
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(graph_expand(101, 2))"),
                "graph_expand should stay a TVF at parse time");
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(graph_expand(101, 3))"),
                "graph_expand should stay a TVF at parse time");
    }

    @Test
    public void testGraphExpandDepthOutOfRangeStillParsesAsTvf() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(graph_expand(101, 0))"));
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(graph_expand(101, 4))"));
    }

    @Test
    public void testTextSearchStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(text_search(1, 'kw'))"),
                "text_search should stay a TVF at parse time");
    }

    @Test
    public void testContextSearchStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(context_search(1, 'kw'))"),
                "context_search should stay a TVF at parse time");
    }

    @Test
    public void testVectorSearchStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(vector_search(1, 'kw'))"),
                "vector_search should stay a TVF at parse time");
    }

    @Test
    public void testContextPackStaysTvfAtParseTime() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(context_pack(1, 1024))"),
                "context_pack should stay a TVF at parse time");
    }

    @Test
    public void testRetrievalTvfWithExpressionArgStillParses() {
        Assertions.assertFalse(fromIsSubquery("SELECT * FROM TABLE(context_get(1+1))"),
                "context_get with non-literal arg must stay a TVF");
    }

    private boolean fromIsSubquery(String sql) {
        com.starrocks.sql.ast.QueryStatement stmt = (com.starrocks.sql.ast.QueryStatement) parse(sql);
        com.starrocks.sql.ast.QueryRelation root = stmt.getQueryRelation();
        if (!(root instanceof com.starrocks.sql.ast.SelectRelation)) {
            return false;
        }
        com.starrocks.sql.ast.Relation from = ((com.starrocks.sql.ast.SelectRelation) root).getRelation();
        return from instanceof com.starrocks.sql.ast.SubqueryRelation;
    }
}
