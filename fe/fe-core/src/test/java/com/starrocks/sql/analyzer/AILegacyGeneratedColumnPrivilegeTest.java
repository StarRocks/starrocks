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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

public class AILegacyGeneratedColumnPrivilegeTest extends PlanTestNoneDBBase {
    @BeforeAll
    public static void setUpTable() throws Exception {
        starRocksAssert.withDatabase("ai_generated_auth").useDatabase("ai_generated_auth");
        starRocksAssert.withTable("CREATE TABLE prompts (id BIGINT, prompt VARCHAR(100), "
                + "answer VARCHAR(65533) AS upper(prompt)) PRIMARY KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        starRocksAssert.withTable("CREATE TABLE ordinary_prompts (id BIGINT, prompt VARCHAR(100), "
                + "answer VARCHAR(65533) AS upper(prompt)) PRIMARY KEY(id) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        // Model metadata persisted before legacy ai_query required AI privileges. Function metadata is
        // intentionally unresolved, as it is when the generated-column SQL is loaded from an image.
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("ai_generated_auth").getTable("prompts");
        table.getColumn("answer").setGeneratedColumnExpr(
                ColumnIdExpr.fromSql("ai_query(prompt, parse_json('{}'))"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"INSERT INTO prompts(id, prompt) VALUES (1, 'prompt')",
            "UPDATE prompts SET prompt = 'new prompt' WHERE id = 1"})
    public void testPersistedLegacyGeneratedColumnCannotExecuteAfterWriteAuthorization(String sql) throws Exception {
        executeAsRoot("CREATE USER ai_generated_writer");
        try {
            executeAsRoot("GRANT SELECT, INSERT, UPDATE ON TABLE ai_generated_auth.prompts TO ai_generated_writer");
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity("ai_generated_writer", "%"));
            caller.setCurrentRoleIds(Set.of());
            caller.setDatabase("ai_generated_auth");
            try (var ignored = caller.bindScope()) {
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> plan(sql, caller));
                Assertions.assertTrue(error.getMessage().contains("Generated Column"), error.getMessage());
            }
        } finally {
            executeAsRoot("DROP USER ai_generated_writer");
            connectContext.setThreadLocalInfo();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CREATE TABLE new_ai_generated (id BIGINT, prompt VARCHAR(100), "
                    + "answer VARCHAR(65533) AS ai_query(prompt, parse_json('{}'))) "
                    + "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')",
            "ALTER TABLE ordinary_prompts ADD COLUMN answer2 VARCHAR(65533) AS ai_query(prompt, parse_json('{}'))",
            "ALTER TABLE ordinary_prompts MODIFY COLUMN answer VARCHAR(65533) AS ai_query(prompt, parse_json('{}'))"
    })
    public void testLegacyAIGeneratedColumnDefinitionsAreRejected(String sql) {
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> Analyzer.analyze(SqlParser.parse(sql, connectContext.getSessionVariable()).get(0), connectContext));
        Assertions.assertTrue(error.getMessage().contains("Generated Column"), error.getMessage());
    }

    @Test
    public void testReadingStoredAnswerAndOrdinaryGeneratedWritesRemainAllowed() throws Exception {
        executeAsRoot("CREATE USER ai_generated_reader");
        try {
            executeAsRoot("GRANT SELECT ON TABLE ai_generated_auth.prompts TO ai_generated_reader");
            executeAsRoot("GRANT INSERT ON TABLE ai_generated_auth.ordinary_prompts TO ai_generated_reader");
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity("ai_generated_reader", "%"));
            caller.setCurrentRoleIds(Set.of());
            caller.setDatabase("ai_generated_auth");
            try (var ignored = caller.bindScope()) {
                Assertions.assertDoesNotThrow(() -> plan("SELECT answer FROM prompts", caller));
                Assertions.assertDoesNotThrow(() -> plan(
                        "INSERT INTO ordinary_prompts(id, prompt) VALUES (1, 'prompt')", caller));
            }
        } finally {
            executeAsRoot("DROP USER ai_generated_reader");
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testPersistedSqlUdfCannotHideGeneratedAIExecution() throws Exception {
        boolean previousEnableUdf = Config.enable_udf;
        Config.enable_udf = true;
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("ai_generated_auth").getTable("ordinary_prompts");
        try {
            executeAsRoot("CREATE FUNCTION generated_ai_wrapper(x STRING) RETURNS ai_query(x, parse_json('{}'))");
            table.getColumn("answer").setGeneratedColumnExpr(ColumnIdExpr.fromSql("generated_ai_wrapper(prompt)"));
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> plan("INSERT INTO ordinary_prompts(id, prompt) VALUES (1, 'prompt')", connectContext));
            Assertions.assertTrue(error.getMessage().contains("Generated Column"), error.getMessage());
        } finally {
            table.getColumn("answer").setGeneratedColumnExpr(ColumnIdExpr.fromSql("upper(prompt)"));
            executeAsRoot("DROP FUNCTION IF EXISTS generated_ai_wrapper(STRING)");
            Config.enable_udf = previousEnableUdf;
        }
    }

    private static void plan(String sql, ConnectContext caller) throws Exception {
        caller.setQueryId(UUIDUtil.genUUID());
        caller.setExecutionId(UUIDUtil.toTUniqueId(caller.getQueryId()));
        StatementBase statement = SqlParser.parse(sql, caller.getSessionVariable()).get(0);
        try {
            StatementPlanner.plan(statement, caller);
        } finally {
            if (statement instanceof InsertStmt insert && insert.getTxnId() > 0) {
                long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("ai_generated_auth").getId();
                TransactionState transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .getTransactionState(dbId, insert.getTxnId());
                if (transaction != null && transaction.getTransactionStatus() == TransactionStatus.PREPARE) {
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .abortTransaction(dbId, insert.getTxnId(), "generated-column test cleanup");
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
