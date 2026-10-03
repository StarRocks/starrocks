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

package com.starrocks.scheduler;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Config;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshPlanBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AIAsyncExecutionBoundaryTest extends PlanTestBase {
    private String endpoint;
    private String model;
    private String provider;

    @BeforeEach
    public void setUpTaskContext() {
        endpoint = Config.ai_default_chat_endpoint;
        model = Config.ai_default_chat_model;
        provider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "test-model";
        Config.ai_default_chat_provider = "openai_compatible";
        connectContext.getState().reset();
        connectContext.setQuerySource(QueryDetail.QuerySource.TASK);
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void restoreContext() {
        Config.ai_default_chat_endpoint = endpoint;
        Config.ai_default_chat_model = model;
        Config.ai_default_chat_provider = provider;
        connectContext.setQuerySource(QueryDetail.QuerySource.EXTERNAL);
        connectContext.setGroups(Set.of());
        connectContext.setSecurityIntegration("native");
        connectContext.setDistinguishedName("");
    }

    @Test
    public void testLegacyAITaskIsRejectedEvenForExplain() throws Exception {
        execute("explain select ai_complete('prompt')");
        assertTrue(connectContext.getState().isError());
        assertTrue(connectContext.getState().getErrorMessage().contains("creator identity"));
    }

    @Test
    public void testLegacyAIInsertRejectionAbortsItsImplicitTransaction() throws Exception {
        InsertStmt statement = (InsertStmt) execute(
                "insert into t0 select cast(ai_complete('prompt') as bigint), 1, 1");
        assertTrue(connectContext.getState().isError());
        assertTrue(connectContext.getState().getErrorMessage().contains("creator identity"));
        long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
        TransactionState transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getTransactionState(dbId, statement.getTxnId());
        assertNotNull(transaction, "The real INSERT planner must have begun an implicit transaction");
        assertEquals(TransactionStatus.ABORTED, transaction.getTransactionStatus(),
                "Provenance refusal must not leave an INSERT transaction in PREPARE");
    }

    @Test
    public void testLegacyAIInsertRejectionDoesNotAbortCallerOwnedTransaction() throws Exception {
        boolean previousEnableSqlTransaction = connectContext.getSessionVariable().isEnableSqlTransaction();
        try {
            connectContext.getSessionVariable().setEnableSqlTransaction(true);
            execute("begin");
            long transactionId = connectContext.getTxnId();
            assertTrue(transactionId != 0, "BEGIN must establish a real caller-owned transaction");
            connectContext.getState().reset();
            execute("insert into t0 select cast(ai_complete('prompt') as bigint), 1, 1");
            assertTrue(connectContext.getState().getErrorMessage().contains("creator identity"));
            TransactionState transaction = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getExplicitTxnState(transactionId).getTransactionState();
            assertEquals(TransactionStatus.PREPARE, transaction.getTransactionStatus());
            assertEquals(transactionId, connectContext.getTxnId());
        } finally {
            connectContext.getState().reset();
            execute("rollback");
            connectContext.getSessionVariable().setEnableSqlTransaction(previousEnableSqlTransaction);
        }
    }

    @Test
    public void testLegacyAITaskIsRejectedBeforeCTASCreatesTable() throws Exception {
        AtomicBoolean reachedCreateTable = new AtomicBoolean();
        new MockUp<MetadataMgr>() {
            @Mock
            public boolean createTable(ConnectContext context, CreateTableStmt statement) {
                reachedCreateTable.set(true);
                return false;
            }
        };
        execute("create table ai_task_ctas_output as select ai_complete('prompt') as answer");
        assertFalse(reachedCreateTable.get(), "An untrusted AI task must fail before creating its CTAS target");
        assertTrue(connectContext.getState().getErrorMessage().contains("creator identity"));
    }

    @Test
    public void testLegacyTaskDetectsAIAddedToAViewLater() throws Exception {
        starRocksAssert.withView("create view ai_task_changed_view as select 'plain' as answer");
        execute("explain select answer from ai_task_changed_view");
        assertFalse(connectContext.getState().isError());
        starRocksAssert.withView("create or replace view ai_task_changed_view as select ai_complete('prompt') as answer");
        connectContext.getState().reset();
        execute("explain select answer from ai_task_changed_view");
        assertTrue(connectContext.getState().isError());
        assertTrue(connectContext.getState().getErrorMessage().contains("creator identity"));
    }

    @Test
    public void testDataCacheTaskSubcontextPreservesAuthorizationIdentity() {
        connectContext.setGroups(Set.of("task_group"));
        connectContext.setSecurityIntegration("task_integration");
        connectContext.setDistinguishedName("uid=task_owner,dc=example,dc=com");
        DataCacheSelectStatement statement = (DataCacheSelectStatement) SqlParser.parse(
                "cache select * from t0", connectContext.getSessionVariable()).get(0);
        ConnectContext child = DataCacheSelectExecutor.buildCacheSelectConnectContext(statement, connectContext, true);
        assertEquals(QueryDetail.QuerySource.TASK, child.getQuerySource());
        assertEquals(Set.of("task_group"), child.getGroups());
        assertEquals("task_integration", child.getSecurityIntegration());
        assertEquals("uid=task_owner,dc=example,dc=com", child.getDistinguishedName());
    }

    @Test
    public void testPCTRefreshRejectsResolvedAIWithoutDependingOnAuthorizer() {
        TaskRunContext taskContext = new TaskRunContext();
        taskContext.setCtx(connectContext);
        MvTaskRunContext mvContext = new MvTaskRunContext(taskContext);
        MaterializedView mv = new MaterializedView();
        mv.setName("legacy_ai_mv");
        MVPCTRefreshPlanBuilder builder = new MVPCTRefreshPlanBuilder(null, mv, mvContext, null);
        InsertStmt statement = (InsertStmt) SqlParser.parse(
                "insert into t0 select cast(ai_complete('prompt') as bigint), 1, 1",
                connectContext.getSessionVariable()).get(0);
        SemanticException error = assertThrows(SemanticException.class,
                () -> builder.analyzeAndBuildInsertPlan(statement, PCellSortedSet.of(), Map.of(), connectContext));
        assertTrue(error.getMessage().contains("AI functions are not supported in materialized view refresh"));
    }

    private static StatementBase execute(String sql) throws Exception {
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        statement.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = StmtExecutor.newInternalExecutor(connectContext, statement);
        connectContext.setExecutor(executor);
        executor.execute();
        return statement;
    }
}
