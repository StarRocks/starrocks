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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.catalog.TableName;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpConnectProcessor;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

public class ArrowFlightSqlConnectProcessorTest extends StarRocksTestBase {
    private static final String SQL = "SELECT k1, k2 FROM flight_policy.tbl";
    private static final TableName TABLE = new TableName("default_catalog", "flight_policy", "tbl");

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        new StarRocksAssert(UtFrameUtils.createDefaultCtx())
                .withDatabase("flight_policy")
                .withTable("CREATE TABLE flight_policy.tbl (k1 INT, k2 INT) " +
                        "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 " +
                        "PROPERTIES ('replication_num' = '1')");
    }

    private static ArrowFlightSqlConnectContext context() {
        ArrowFlightSqlConnectContext context = new ArrowFlightSqlConnectContext("token");
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        return context;
    }

    private static MockedStatic<Authorizer> policies() {
        MockedStatic<Authorizer> policies = mockStatic(Authorizer.class);
        policies.when(() -> Authorizer.getRowAccessPolicy(any(), eq(TABLE)))
                .thenAnswer(invocation -> SqlParser.parseSqlToExpr("k2 = 7", 0));
        policies.when(() -> Authorizer.getColumnMaskingPolicy(any(), eq(TABLE), any()))
                .thenAnswer(invocation -> Map.of("k1", SqlParser.parseSqlToExpr("'masked'", 0)));
        return policies;
    }

    private static void assertFilteredAndMasked(StatementBase statement) {
        SelectRelation outer = (SelectRelation) ((QueryStatement) statement).getQueryRelation();
        assertTrue(outer.getRelation() instanceof SubqueryRelation);
        SelectRelation policy = (SelectRelation) ((SubqueryRelation) outer.getRelation())
                .getQueryStatement().getQueryRelation();
        assertTrue(AstToStringBuilder.toString(policy.getWhereClause()).contains("k2"));
        assertTrue(AstToStringBuilder.toString(policy.getWhereClause()).contains("= 7"));
        assertEquals("'masked'", AstToStringBuilder.toString(policy.getOutputExpression().get(0)));
        assertTrue(outer.getOutputExpression().get(0).getType().isStringType());
    }

    @Test
    public void testFlightParseAppliesFilterAndMaskOnEachParse() throws Exception {
        ArrowFlightSqlConnectContext context = context();
        ArrowFlightSqlConnectProcessor processor = new ArrowFlightSqlConnectProcessor(context, SQL);
        try (var scope = context.bindScope(); var policies = policies()) {
            // Retry parses must receive policies again, not reuse an already analyzed tree.
            for (int i = 0; i < 2; i++) {
                StatementBase statement = processor.parse(SQL, context.getSessionVariable());
                Analyzer.analyze(statement, context);
                assertFilteredAndMasked(statement);
            }
        }
    }

    @Test
    public void testHttpExecutionAnalyzesFilterAndMask() throws Exception {
        HttpConnectContext context = new HttpConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        StatementBase statement = SqlParser.parse(SQL, context.getSessionVariable()).get(0);
        statement.setOrigStmt(new OriginStatement(SQL));
        context.setStatement(statement);
        HttpConnectProcessor processor = new HttpConnectProcessor(context) {
            @Override
            public void auditAfterExec(String sql, StatementBase stmt, PQueryStatistics statistics) {
                // Audit delivery is unrelated to policy analysis.
            }
        };
        try (var scope = context.bindScope(); var policies = policies();
                MockedConstruction<StmtExecutor> executors = mockConstruction(StmtExecutor.class,
                        (executor, construction) -> doAnswer(invocation -> {
                            Analyzer.analyze((StatementBase) construction.arguments().get(1), context);
                            return null;
                        }).when(executor).execute())) {
            processor.processOnce();
            verify(executors.constructed().get(0)).execute();
            assertFilteredAndMasked(statement);
        }
    }

    private static Schema schema(ArrowFlightSqlServiceImpl service, ArrowFlightSqlConnectContext context)
            throws Exception {
        Method method = ArrowFlightSqlServiceImpl.class
                .getDeclaredMethod("buildSchemaFromQuery", ArrowFlightSqlConnectContext.class, String.class);
        method.setAccessible(true);
        return (Schema) method.invoke(service, context, SQL);
    }

    @Test
    public void testPreparedSchemaUsesMaskedType() throws Exception {
        try (ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(null, null);
                var policies = policies()) {
            Schema schema = schema(service, context());
            assertEquals(2, schema.getFields().size());
            assertEquals("k1", schema.getFields().get(0).getName());
            assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(0).getType());
            policies.verify(() -> Authorizer.getRowAccessPolicy(any(), eq(TABLE)));
        }
    }

    @Test
    public void testPreparedSchemaFailsClosedOnPolicyErrorAndRestoresContext() throws Exception {
        ConnectContext previous = new ConnectContext();
        try (var scope = previous.bindScope();
                ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(null, null);
                var policies = policies()) {
            SemanticException failure = new SemanticException("policy unavailable");
            policies.when(() -> Authorizer.getRowAccessPolicy(any(), eq(TABLE))).thenThrow(failure);
            InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                    () -> schema(service, context()));
            assertSame(failure, thrown.getCause());
            assertSame(previous, ConnectContext.get());
        }
    }

    @Test
    public void testPreparedSchemaFailsClosedOnInvalidMask() throws Exception {
        try (ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(null, null);
                var policies = policies()) {
            policies.when(() -> Authorizer.getColumnMaskingPolicy(any(), eq(TABLE), any()))
                    .thenAnswer(invocation -> Map.of("k1", SqlParser.parseSqlToExpr("missing_column", 0)));
            InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                    () -> schema(service, context()));
            assertTrue(thrown.getCause() instanceof SemanticException);
        }
    }
}
