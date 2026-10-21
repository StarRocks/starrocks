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

package com.starrocks.summary;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.Frontend;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class QueryHistoryTest {
    private static final String ACCESS_KEY = "query-history-test-access";
    private static final String SECRET_KEY = "query-history-test-secret";
    private static final String SESSION_TOKEN = "query-history-test-token";
    private static final String FILE_PROPERTIES = "\"path\" = \"s3://query-history-test/input/*\", "
            + "\"format\" = \"parquet\", \"aws.s3.region\" = \"test-region\", "
            + "\"aws.s3.access_key\" = \"" + ACCESS_KEY + "\", "
            + "\"aws.s3.secret_key\" = \"" + SECRET_KEY + "\", "
            + "\"aws.s3.session_token\" = \"" + SESSION_TOKEN + "\"";

    @BeforeEach
    public void setUp() {
        Frontend frontend = Mockito.mock(Frontend.class);
        Mockito.when(frontend.getQueryPort()).thenReturn(9030);
        new MockUp<NodeMgr>() {
            @Mock
            public String getNodeName() {
                return "query-history-test";
            }

            @Mock
            public Frontend getMySelf() {
                return frontend;
            }
        };
    }

    @Test
    public void testRedactCredentialsBeforePersistingQueryHistory() {
        String sql = "SELECT * FROM t0 JOIN FILES(" + FILE_PROPERTIES + ") f ON t0.id = f.id";
        ConnectContext context = createContext(sql, 0);
        QueryHistory history = new QueryHistory(context, createPlan(context));

        assertCredentialsHidden(history.getOriginSQL());
        JsonObject persisted = JsonParser.parseString(history.toJSON()).getAsJsonObject();
        assertCredentialsHidden(persisted.get("sql").getAsString());
        assertCredentialsHidden(persisted.get("sql_digest").getAsString());
        Assertions.assertEquals(persisted.get("sql").getAsString(), history.getOriginSQL());
        Assertions.assertEquals(persisted.get("sql_digest").getAsString(), history.getSqlDigest());
        Assertions.assertEquals("default_catalog.test", persisted.get("db").getAsString());
        Assertions.assertEquals("query-history-test:9030", persisted.get("frontend").getAsString());
        Assertions.assertEquals(42.0, persisted.get("plan_costs").getAsDouble());
        Assertions.assertEquals(history.getQueryMs(), persisted.get("query_ms").getAsDouble());
        Assertions.assertTrue(persisted.get("plan").getAsString().contains("EMPTY"));
        Assertions.assertEquals(sql, context.getExecutor().getOriginStmtInString());
        Assertions.assertEquals(sql, context.getExecutor().getParsedStmt().getOrigStmt().originStmt);
    }

    @Test
    public void testRedactCredentialsInNeighboringStatement() {
        String sql = "INSERT INTO FILES(" + FILE_PROPERTIES + ") SELECT * FROM t0;\n"
                + "SELECT * FROM t0 JOIN t1 ON t0.id = t1.id";
        // Each parsed statement retains the complete client request as its original SQL.
        ConnectContext context = createContext(sql, 1);
        QueryHistory history = new QueryHistory(context, createPlan(context));

        JsonObject persisted = JsonParser.parseString(history.toJSON()).getAsJsonObject();
        String persistedSql = persisted.get("sql").getAsString();
        assertCredentialsHidden(persistedSql);
        Assertions.assertTrue(persistedSql.contains("INSERT INTO FILES("));
        Assertions.assertTrue(persistedSql.contains("SELECT * FROM t0 JOIN t1 ON t0.id = t1.id"));
        Assertions.assertEquals(sql, context.getExecutor().getOriginStmtInString());
    }

    @Test
    public void testPreserveOrdinarySqlAndQueryMetadata() {
        String sql = "SELECT *\nFROM t0 JOIN t1 ON t0.id = t1.id -- keep original formatting\n";
        ConnectContext context = createContext(sql, 0);
        QueryHistory history = new QueryHistory(context, createPlan(context));

        JsonObject persisted = JsonParser.parseString(history.toJSON()).getAsJsonObject();
        Assertions.assertEquals(sql, history.getOriginSQL());
        Assertions.assertEquals(sql, persisted.get("sql").getAsString());
        Assertions.assertTrue(persisted.get("sql_digest").getAsString().contains("t0"));
        Assertions.assertTrue(persisted.get("sql_digest").getAsString().contains("t1"));
        Assertions.assertEquals(sql, context.getExecutor().getOriginStmtInString());
    }

    @Test
    public void testRedactSqlAssignedToHistory() {
        ConnectContext context = createContext("SELECT * FROM t0", 0);
        QueryHistory history = new QueryHistory(context, createPlan(context));
        String sql = "SELECT * FROM FILES(" + FILE_PROPERTIES + ")";
        history.setOriginSQL(sql);

        assertCredentialsHidden(history.getOriginSQL());
        JsonObject persisted = JsonParser.parseString(history.toJSON()).getAsJsonObject();
        assertCredentialsHidden(persisted.get("sql").getAsString());
        Assertions.assertEquals("SELECT * FROM t0", context.getExecutor().getOriginStmtInString());

        QueryHistory loaded = new QueryHistory();
        loaded.setOriginSQL(sql);
        assertCredentialsHidden(loaded.getOriginSQL());
    }

    private static ConnectContext createContext(String sql, int statementIndex) {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StatementBase statement = SqlParser.parse(sql, new SessionVariable()).get(statementIndex);
        StmtExecutor executor = new StmtExecutor(context, statement);
        Mockito.when(context.getExecutor()).thenReturn(executor);
        Mockito.when(context.getCurrentCatalog()).thenReturn("default_catalog");
        Mockito.when(context.getDatabase()).thenReturn("test");
        Mockito.when(context.getStartTime()).thenReturn(System.currentTimeMillis() - 50);
        return context;
    }

    private static ExecPlan createPlan(ConnectContext context) {
        // Serialize a real physical plan without resolving a FILES schema or accessing object storage.
        OptExpression physicalPlan = OptExpression.create(
                new PhysicalValuesOperator(List.of(), List.of(), -1, null, null));
        physicalPlan.setStatistics(Statistics.builder().setOutputRowCount(0).build());
        physicalPlan.setCost(42);
        return new ExecPlan(context, List.of(), physicalPlan, List.of(), false);
    }

    private static void assertCredentialsHidden(String sql) {
        Assertions.assertFalse(sql.contains(ACCESS_KEY));
        Assertions.assertFalse(sql.contains(SECRET_KEY));
        Assertions.assertFalse(sql.contains(SESSION_TOKEN));
        Assertions.assertTrue(sql.contains("aws.s3.access_key"));
        Assertions.assertTrue(sql.contains("aws.s3.secret_key"));
        Assertions.assertTrue(sql.contains("aws.s3.session_token"));
        Assertions.assertTrue(sql.contains("***"));
        Assertions.assertTrue(sql.contains("s3://query-history-test/input/*"));
        Assertions.assertTrue(sql.contains("test-region"));
    }
}
