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

import com.starrocks.alter.AlterCancelException;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

public class AISyncMaterializedViewPrivilegeTest extends PlanTestNoneDBBase {
    private static final String DATABASE = "ai_sync_mv_auth";
    private static final String INDEX = "stored_ai_mv";
    private static final long INDEX_ID = 90000001L;
    private static String previousEndpoint;
    private static String previousModel;
    private static String previousProvider;

    @BeforeAll
    public static void setUpTable() throws Exception {
        previousEndpoint = Config.ai_default_chat_endpoint;
        previousModel = Config.ai_default_chat_model;
        previousProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "sync-mv-test-model";
        Config.ai_default_chat_provider = "openai_compatible";
        starRocksAssert.withDatabase(DATABASE).useDatabase(DATABASE);
        starRocksAssert.withTable("CREATE TABLE prompts (id BIGINT, prompt VARCHAR(100)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = previousEndpoint;
        Config.ai_default_chat_model = previousModel;
        Config.ai_default_chat_provider = previousProvider;
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT id, ai_query(prompt, parse_json('{}')) AS answer FROM prompts",
            "SELECT id, ai_complete(prompt) AS answer FROM prompts",
            "SELECT id, prompt FROM prompts WHERE ai_query(prompt, parse_json('{}')) = 'yes'"
    })
    public void testCreateRejectsAIWithoutAnExecutionIdentity(String query) {
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> analyze("CREATE MATERIALIZED VIEW ai_mv AS " + query));
        Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_query(prompt, parse_json('{}'))", "ai_complete(prompt)"})
    public void testPersistedRollupProjectionCannotExecuteAI(String expression) {
        Column answer = new Column("mv_answer", VarcharType.VARCHAR);
        answer.setDefineExpr(SqlParser.parseSqlToExpr(expression, connectContext.getSessionVariable().getSqlMode()));
        AlterCancelException error = Assertions.assertThrows(AlterCancelException.class,
                () -> RollupJobV2.analyzeAndCreateRollupJobV2Params(table(),
                        List.of(table().getColumn("id"), answer), null, DATABASE));
        Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
    }

    @Test
    public void testPersistedRollupFilterCannotExecuteAI() {
        Expr where = SqlParser.parseSqlToExpr("ai_query(prompt, parse_json('{}')) = 'yes'",
                connectContext.getSessionVariable().getSqlMode());
        AlterCancelException error = Assertions.assertThrows(AlterCancelException.class,
                () -> RollupJobV2.analyzeAndCreateRollupJobV2Params(table(), table().getBaseSchema(), where, DATABASE));
        Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
    }

    @Test
    public void testLoadSinkRejectsPersistedAIFilter() {
        OlapTable table = table();
        try {
            addIndex(table, table.getBaseSchema());
            table.getIndexMetaByMetaId(INDEX_ID).setWhereClause(SqlParser.parseSqlToExpr(
                    "ai_query(prompt, parse_json('{}')) = 'yes'", connectContext.getSessionVariable().getSqlMode()));
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> OlapTableSink.createSchema(database().getId(), table, outputTuple(table), null, true));
            Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
        } finally {
            table.deleteIndexInfo(INDEX);
        }
    }

    @Test
    public void testInsertCannotExpandPersistedAIProjectionAfterAuthorization() throws Exception {
        OlapTable table = table();
        Column answer = new Column("mv_answer", VarcharType.VARCHAR);
        answer.setDefineExpr(analyzedExpression("ai_query(prompt, parse_json('{}'))"));
        executeAsRoot("CREATE USER ai_sync_mv_writer");
        try {
            addIndex(table, List.of(table.getColumn("id"), answer));
            executeAsRoot("GRANT INSERT, SELECT ON TABLE " + DATABASE + ".prompts TO ai_sync_mv_writer");
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity("ai_sync_mv_writer", "%"));
            caller.setCurrentRoleIds(Set.of());
            caller.setDatabase(DATABASE);
            try (var ignored = caller.bindScope()) {
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> plan("INSERT INTO prompts VALUES (1, 'prompt')", caller));
                Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
                Assertions.assertDoesNotThrow(() -> plan("SELECT id, prompt FROM prompts", caller));
            }
        } finally {
            table.deleteIndexInfo(INDEX);
            executeAsRoot("DROP USER ai_sync_mv_writer");
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSqlUdfCannotHideAIInSyncMVDefinition() throws Exception {
        boolean previousEnableUdf = Config.enable_udf;
        Config.enable_udf = true;
        try {
            executeAsRoot("CREATE FUNCTION sync_ai_wrapper(x STRING) RETURNS ai_query(x, parse_json('{}'))");
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> analyze("CREATE MATERIALIZED VIEW ai_mv AS "
                            + "SELECT id, sync_ai_wrapper(prompt) AS answer FROM prompts"));
            Assertions.assertTrue(error.getMessage().contains("Synchronous materialized view"), error.getMessage());
        } finally {
            executeAsRoot("DROP FUNCTION IF EXISTS sync_ai_wrapper(STRING)");
            Config.enable_udf = previousEnableUdf;
        }
    }

    @Test
    public void testOrdinarySyncMVDefinitionsAndRollupRemainAllowed() {
        Assertions.assertDoesNotThrow(() -> analyze("CREATE MATERIALIZED VIEW ordinary_mv AS "
                + "SELECT id, upper(prompt) AS answer FROM prompts WHERE prompt != ''"));
        Column answer = new Column("mv_answer", VarcharType.VARCHAR);
        answer.setDefineExpr(SqlParser.parseSqlToExpr("upper(prompt)", connectContext.getSessionVariable().getSqlMode()));
        Assertions.assertDoesNotThrow(() -> RollupJobV2.analyzeAndCreateRollupJobV2Params(table(),
                List.of(table().getColumn("id"), answer), null, DATABASE));
    }

    private static void analyze(String sql) {
        Analyzer.analyze(SqlParser.parse(sql, connectContext.getSessionVariable()).get(0), connectContext);
    }

    private static Expr analyzedExpression(String expression) {
        QueryStatement query = (QueryStatement) SqlParser.parse("SELECT " + expression + " FROM prompts",
                connectContext.getSessionVariable()).get(0);
        Analyzer.analyze(query, connectContext);
        return query.getQueryRelation().getOutputExpression().get(0);
    }

    private static Database database() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DATABASE);
    }

    private static OlapTable table() {
        return (OlapTable) database().getTable("prompts");
    }

    private static void addIndex(OlapTable table, List<Column> schema) {
        table.setIndexMeta(INDEX_ID, INDEX, schema, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.rebuildFullSchema();
    }

    private static TupleDescriptor outputTuple(OlapTable table) {
        DescriptorTable descriptors = new DescriptorTable();
        TupleDescriptor tuple = descriptors.createTupleDescriptor();
        tuple.setTable(table);
        for (Column column : table.getBaseSchema()) {
            SlotDescriptor slot = descriptors.addSlotDescriptor(tuple);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            slot.setIsNullable(column.isAllowNull());
        }
        tuple.computeMemLayout();
        return tuple;
    }

    private static void plan(String sql, ConnectContext caller) throws Exception {
        caller.setQueryId(UUIDUtil.genUUID());
        caller.setExecutionId(UUIDUtil.toTUniqueId(caller.getQueryId()));
        StatementBase statement = SqlParser.parse(sql, caller.getSessionVariable()).get(0);
        try {
            StatementPlanner.plan(statement, caller);
        } finally {
            if (statement instanceof InsertStmt insert && insert.getTxnId() > 0) {
                TransactionState transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getTransactionState(database().getId(), insert.getTxnId());
                if (transaction != null && transaction.getTransactionStatus() == TransactionStatus.PREPARE) {
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .abortTransaction(database().getId(), insert.getTxnId(), "sync MV test cleanup");
                }
            }
        }
    }

    private static void executeAsRoot(String sql) throws Exception {
        try (var ignored = connectContext.bindScope()) {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        }
    }
}
