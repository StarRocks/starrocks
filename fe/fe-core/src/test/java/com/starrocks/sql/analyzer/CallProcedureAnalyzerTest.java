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

import com.starrocks.connector.Procedure;
import com.starrocks.connector.iceberg.procedure.NamedArgument;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.CallProcedureStatement;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CallProcedureAnalyzerTest {
    @BeforeEach
    public void setUp(@Mocked MetadataMgr metadataMgr) throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
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

    private ProcedureArgument arg(String name, int value) {
        return new ProcedureArgument(name, new IntLiteral(value));
    }

    private ProcedureArgument arg(int value) {
        return new ProcedureArgument(null, new IntLiteral(value));
    }

    private ProcedureArgument arg(String name, String value) {
        return new ProcedureArgument(name, new StringLiteral(value));
    }

    private ProcedureArgument arg(String value) {
        return new ProcedureArgument(null, new StringLiteral(value));
    }

    @Test
    public void testPositionalArgs() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg(1), arg("hello")),
                NodePosition.ZERO
        );
        CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        Map<String, ConstantOperator> analyzed = stmt.getAnalyzedArguments();
        Assertions.assertEquals(2, analyzed.size());
        Assertions.assertEquals(1, analyzed.get("a").getTinyInt());
        Assertions.assertEquals("hello", analyzed.get("b").getVarchar());
    }

    @Test
    public void testNamedArgs() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg("a", 2), arg("b", "world")),
                NodePosition.ZERO
        );
        CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        Map<String, ConstantOperator> analyzed = stmt.getAnalyzedArguments();
        Assertions.assertEquals(2, analyzed.size());
        Assertions.assertEquals(2, analyzed.get("a").getTinyInt());
        Assertions.assertEquals("world", analyzed.get("b").getVarchar());
    }

    @Test
    public void testMixNamedAndPositionalArgs() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg("a", 2), arg("world")),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Mixing named and positional arguments is not allowed"));
    }

    @Test
    public void testMissingRequiredArg() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Collections.singletonList(arg("world")),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Missing required argument: b"));
    }

    @Test
    public void testTooManyArgs() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg(1), arg("hello"), arg(3)),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Too many arguments provided, expected at most 2, got 3"));
    }

    @Test
    public void testTypeMismatch() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg("notInt"), arg("hello")),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Argument 'a' has invalid type"));
    }

    @Test
    public void testUnknownArgumentName() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg("unknown", 1), arg("b", "hello")),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Unknown argument name: unknown"));
    }

    @Test
    public void testDuplicateArgumentName() {
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(arg("a", 1), arg("a", 2)),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("Duplicate argument name: a"));
    }

    @Test
    public void testNonConstantArgument() {
        Expr nonConstExpr = new FunctionCallExpr("abs", List.of(new IntLiteral(-1)));
        CallProcedureStatement stmt = new CallProcedureStatement(
                QualifiedName.of(Arrays.asList("test_catalog", "test_db", "test_proc")),
                Arrays.asList(new ProcedureArgument("a", nonConstExpr), arg("b", "hello")),
                NodePosition.ZERO
        );
        Exception ex = Assertions.assertThrows(SemanticException.class, () -> {
            CallProcedureAnalyzer.analyze(stmt, ConnectContext.get());
        });
        Assertions.assertTrue(ex.getMessage().contains("expected const argument"));
    }
}
