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

package com.starrocks.qe;

import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.AIModel;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.persist.DropAIModelLog;
import com.starrocks.server.AIModelMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

public class AIModelDDLShowTest {
    private ConnectContext context;
    private AIModelMgr mgr;
    private static final String PROPERTIES = " PROPERTIES ('capability'='CHAT', 'provider'='openai_compatible', "
            + "'endpoint'='https://models.example.test/v1/chat/completions', 'model'='chat-model', "
            + "'credential_ref'='TEST_MODEL')";

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        context = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        mgr = GlobalStateMgr.getCurrentState().getAIModelMgr();
        clearModels();
    }

    @AfterEach
    public void tearDown() {
        clearModels();
        ConnectContext.remove();
        UtFrameUtils.tearDownForPersisTest();
    }

    private void clearModels() {
        for (String name : List.of("DdlChat", "ddlChat")) {
            AIModel model = mgr.getByName(name);
            if (model != null) {
                mgr.replayDropModel(new DropAIModelLog(model.getId()));
            }
        }
    }

    private void execute(String sql) throws Exception {
        StatementBase statement = SqlParser.parseSingleStatement(sql, 0);
        Analyzer.analyze(statement, context);
        DDLStmtExecutor.execute(statement, context);
    }

    private ShowResultSet show(String sql) {
        ShowStmt statement = (ShowStmt) SqlParser.parseSingleStatement(sql, 0);
        Analyzer.analyze(statement, context);
        return ShowExecutor.execute(statement, context);
    }

    @Test
    public void testDdlShowAndDescription() throws Exception {
        execute("CREATE AI MODEL DdlChat COMMENT 'initial'" + PROPERTIES);
        AIModel before = mgr.getByName("DdlChat");
        execute("CREATE AI MODEL IF NOT EXISTS DdlChat" + PROPERTIES);
        Assertions.assertSame(before, mgr.getByName("DdlChat"));
        execute("CREATE AI MODEL ddlChat" + PROPERTIES);
        Assertions.assertEquals(List.of(List.of("DdlChat")), show("SHOW AI MODELS LIKE 'Ddl%'").getResultRows());
        Assertions.assertTrue(show("SHOW AI MODELS LIKE ''").getResultRows().isEmpty());
        ShowResultSet description = show("DESC AI MODEL DdlChat");
        Assertions.assertEquals(9, description.getMetaData().getColumnCount());
        Assertions.assertEquals(List.of(Long.toString(before.getId()), "DdlChat", "1", "CHAT", "openai_compatible",
                before.getEndpoint(), "chat-model", "TEST_MODEL", "initial"), description.getResultRows().get(0));
        execute("ALTER AI MODEL DdlChat COMMENT = ''");
        AIModel cleared = mgr.getByName("DdlChat");
        Assertions.assertEquals(2, cleared.getRevision());
        Assertions.assertEquals("", cleared.getComment());
        execute("ALTER AI MODEL DdlChat SET ('model'='next')");
        Assertions.assertEquals("next", mgr.getByName("DdlChat").getRemoteModel());
        Assertions.assertEquals(3, mgr.getByName("DdlChat").getRevision());
        execute("DROP AI MODEL DdlChat");
        Assertions.assertNull(mgr.getByName("DdlChat"));
        execute("DROP AI MODEL IF EXISTS DdlChat");
        execute("ALTER AI MODEL IF EXISTS DdlChat SET ('model'='ignored')");
        Assertions.assertThrows(SemanticException.class, () -> execute("DROP AI MODEL DdlChat"));
        Assertions.assertThrows(SemanticException.class, () -> show("DESC AI MODEL DdlChat"));
    }

    @Test
    public void testRejectedChangeLeavesMetadataUnchanged() throws Exception {
        execute("CREATE AI MODEL DdlChat" + PROPERTIES);
        AIModel before = mgr.getByName("DdlChat");
        Assertions.assertThrows(DdlException.class,
                () -> execute("ALTER AI MODEL DdlChat SET ('credential_ref'='OTHER')"));
        Assertions.assertThrows(DdlException.class,
                () -> execute("ALTER AI MODEL DdlChat SET ('capability'='TEXT_EMBEDDING')"));
        Assertions.assertThrows(SemanticException.class,
                () -> execute("ALTER AI MODEL DdlChat SET ('api_key'='do-not-store')"));
        Assertions.assertSame(before, mgr.getByName("DdlChat"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ALTER", "DROP"})
    public void testIfExistsDoesNotSwallowDropRecreateDuringAuthorization(String action) throws Exception {
        execute("CREATE AI MODEL DdlChat" + PROPERTIES);
        AIModel authorized = mgr.getByName("DdlChat");
        AtomicReference<AIModel> replacement = new AtomicReference<>();
        PrivilegeType privilege = action.equals("ALTER") ? PrivilegeType.ALTER : PrivilegeType.DROP;
        try (MockedStatic<Authorizer> authorizer = mockStatic(Authorizer.class, CALLS_REAL_METHODS)) {
            authorizer.when(() -> Authorizer.checkAIModelAction(context, authorized, privilege)).thenAnswer(invocation -> {
                invocation.callRealMethod();
                mgr.dropModel(authorized);
                replacement.set(mgr.createModel("DdlChat", Map.of("capability", "CHAT", "provider", "openai_compatible",
                        "endpoint", authorized.getEndpoint(), "model", "replacement", "credential_ref", "TEST_MODEL"),
                        "", false));
                return null;
            });
            authorizer.clearInvocations();
            DdlException error = Assertions.assertThrows(DdlException.class, () -> execute(ifExistsSql(action)));
            Assertions.assertTrue(error.getMessage().contains("changed concurrently"));
            authorizer.verify(() -> Authorizer.checkAIModelAction(context, authorized, privilege));
        }
        Assertions.assertNotNull(replacement.get());
        Assertions.assertNotEquals(authorized.getId(), replacement.get().getId());
        Assertions.assertSame(replacement.get(), mgr.getByName("DdlChat"));
        Assertions.assertEquals("replacement", mgr.getByName("DdlChat").getRemoteModel());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ALTER", "DROP"})
    public void testIfExistsDoesNotSwallowRevisionChangeDuringAuthorization(String action) throws Exception {
        execute("CREATE AI MODEL DdlChat" + PROPERTIES);
        AIModel authorized = mgr.getByName("DdlChat");
        AtomicReference<AIModel> replacement = new AtomicReference<>();
        PrivilegeType privilege = action.equals("ALTER") ? PrivilegeType.ALTER : PrivilegeType.DROP;
        try (MockedStatic<Authorizer> authorizer = mockStatic(Authorizer.class, CALLS_REAL_METHODS)) {
            authorizer.when(() -> Authorizer.checkAIModelAction(context, authorized, privilege)).thenAnswer(invocation -> {
                invocation.callRealMethod();
                replacement.set(mgr.alterModel(authorized, Map.of("model", "replacement"), null));
                return null;
            });
            authorizer.clearInvocations();
            DdlException error = Assertions.assertThrows(DdlException.class, () -> execute(ifExistsSql(action)));
            Assertions.assertTrue(error.getMessage().contains("changed concurrently"));
            authorizer.verify(() -> Authorizer.checkAIModelAction(context, authorized, privilege));
        }
        Assertions.assertNotNull(replacement.get());
        Assertions.assertEquals(authorized.getId(), replacement.get().getId());
        Assertions.assertEquals(authorized.getRevision() + 1, replacement.get().getRevision());
        Assertions.assertSame(replacement.get(), mgr.getByName("DdlChat"));
        Assertions.assertEquals("replacement", mgr.getByName("DdlChat").getRemoteModel());
    }

    private String ifExistsSql(String action) {
        return action.equals("ALTER") ? "ALTER AI MODEL IF EXISTS DdlChat SET ('model'='wrong')"
                : "DROP AI MODEL IF EXISTS DdlChat";
    }
}
