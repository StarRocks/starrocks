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

package com.starrocks.analysis;

import com.starrocks.connector.Procedure;
import com.starrocks.connector.iceberg.procedure.NamedArgument;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.starrocks.sql.ast.StatementBase;

import java.util.Arrays;
import java.util.Map;


public class CallProcedureStatementTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp(@Mocked MetadataMgr metadataMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState().getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getProcedure(anyString, anyString, anyString);
                result = mockProcedure();
                minTimes = 0;
            }
        };
    }

    private Procedure mockProcedure() {
        return new Procedure("test_db", "test_proc",
                Arrays.asList(
                        new NamedArgument("a", com.starrocks.catalog.Type.INT, true),
                        new NamedArgument("b", com.starrocks.catalog.Type.VARCHAR, true)
                )) {
            @Override
            public void execute(ConnectContext context, Map<String, ConstantOperator> args) {
                // no-op
            }
        };
    }

    @Test
    public void testCallProcedureStatementWithPositionalArgs() {
        String sql = "CALL test_db.test_proc(1, 'abc')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertTrue(stmt instanceof CallProcedureStatement);
        CallProcedureStatement callStmt = (CallProcedureStatement) stmt;
        Assertions.assertEquals("test_proc", callStmt.getQualifiedName().getParts().get(2));
        Assertions.assertEquals(2, callStmt.getArguments().size());
        Assertions.assertNull(callStmt.getArguments().get(0).getName().orElse(null));
        Assertions.assertNull(callStmt.getArguments().get(1).getName().orElse(null));
    }

    @Test
    public void testCallProcedureStatementWithNamedArgs() {
        String sql = "CALL test_db.test_proc(a = 1, b = 'abc')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(CallProcedureStatement.class, stmt);
        CallProcedureStatement callStmt = (CallProcedureStatement) stmt;
        Assertions.assertEquals("default_catalog", callStmt.getQualifiedName().getParts().get(0));
        Assertions.assertEquals("test_db", callStmt.getQualifiedName().getParts().get(1));
        Assertions.assertEquals("test_proc", callStmt.getQualifiedName().getParts().get(2));
        Assertions.assertEquals(2, callStmt.getArguments().size());
        Assertions.assertEquals("a", callStmt.getArguments().get(0).getName().orElse(null));
        Assertions.assertEquals("b", callStmt.getArguments().get(1).getName().orElse(null));

        sql = "CALL test_db.test_proc(a => 1, b => 'abc')";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertInstanceOf(CallProcedureStatement.class, stmt);
        callStmt = (CallProcedureStatement) stmt;
        Assertions.assertEquals("default_catalog", callStmt.getQualifiedName().getParts().get(0));
        Assertions.assertEquals("test_db", callStmt.getQualifiedName().getParts().get(1));
        Assertions.assertEquals("test_proc", callStmt.getQualifiedName().getParts().get(2));
        Assertions.assertEquals(2, callStmt.getArguments().size());
        Assertions.assertEquals("a", callStmt.getArguments().get(0).getName().orElse(null));
        Assertions.assertEquals("b", callStmt.getArguments().get(1).getName().orElse(null));
    }

    @Test
    public void testCallProcedureStatementWithNoArgs() {
        String sql = "CALL test_db.test_proc()";
        StatementBase statementBase = AnalyzeTestUtil.parseSql(sql);
        Assertions.assertInstanceOf(CallProcedureStatement.class, statementBase);
        CallProcedureStatement callStmt = (CallProcedureStatement) statementBase;
        Assertions.assertEquals("test_proc", callStmt.getQualifiedName().getParts().get(1));
        Assertions.assertEquals(0, callStmt.getArguments().size());
    }

    @Test
    public void testCallProcedureStatementWithMixArgsShouldFail() {
        String sql = "CALL test_db.test_proc(a = 1, 'abc')";
        AnalyzeTestUtil.analyzeFail(sql);
    }

    @Test
    public void testCallProcedureStatementWithSyntaxErrorShouldFail() {
        String sql = "CALL test_db.test_proc(a = )";
        AnalyzeTestUtil.analyzeFail(sql);
    }
}
