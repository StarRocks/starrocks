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

import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UDFInternalClassLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class CreateFunctionStmtAnalyzerTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        AnalyzeTestUtil.init();
    }

    private CreateFunctionStmt createStmt(String symbol, String type) {
        String createFunctionSql = String.format("CREATE %s FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"%s\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://localhost:8080/\"\n"
                + ");", type, symbol);
        return (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
    }

    private CreateFunctionStmt createMapStmt(String symbol, String type) {
        String createFunctionSql = String.format("CREATE %s FUNCTION ABC.MY_UDAF_MAP(map<string,string>) \n"
                + "RETURNS map<string,string> \n"
                + "properties (\n"
                + "    \"symbol\" = \"%s\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://localhost:8080/\"\n"
                + ");", type, symbol);
        return (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
    }

    private CreateFunctionStmt createListStmt(String symbol, String type) {
        String createFunctionSql = String.format("CREATE %s FUNCTION ABC.MY_UDAF_LIST(array<string>) \n"
                + "RETURNS array<string> \n"
                + "properties (\n"
                + "    \"symbol\" = \"%s\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://localhost:8080/\"\n"
                + ");", type, symbol);
        return (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
    }

    private CreateFunctionStmt createPyStmt(String symbol, String type, String target) {
        Config.enable_udf = true;
        String createFunctionSql = String.format("CREATE FUNCTION ABC.MY_UDF_JSON_GET(string, string) \n"
                + "RETURNS string \n"
                + "properties (\n"
                + "    \"symbol\" = \"%s\",\n"
                + "    \"type\" = \"Python\",\n"
                + "    \"file\" = \"%s\"\n"
                + ") AS $$\n"
                + "def a(b):"
                + "   return b "
                + "$$;", symbol, target);
        return (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                createFunctionSql, 32).get(0);
    }

    @Test
    public void testJUDF() {
        assertThrows(Throwable.class, () -> {
            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createStmt("symbol", "");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    private static class NormalEval {
        public String evaluate(String a, String b) {
            return a + b;
        }
    }

    @Test
    public void testJScalarUDF() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString()))
                                            .thenReturn((Class) NormalEval.class);
                                })) {

            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createStmt("symbol", "");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    private static class ComplexEval {
        public List<?> evaluate(List<?> a, Map<?, ?> b) {
            return Lists.newArrayList();
        }
    }

    @Test
    public void testJScalarUDFNoScalarInputs() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString()))
                                            .thenReturn((Class) ComplexEval.class);
                                })) {

            try {
                Config.enable_udf = true;
                String createFunctionSql = String.format("CREATE %s FUNCTION ABC.Echo(array<string>,map<int, string>) \n"
                        + "RETURNS array<int> \n"
                        + "properties (\n"
                        + "    \"symbol\" = \"%s\",\n"
                        + "    \"type\" = \"StarrocksJar\",\n"
                        + "    \"file\" = \"http://localhost:8080/\"\n"
                        + ");", "", "symbol");

                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    private String buildFunction(String ret, String args) {
        String sql = String.format("CREATE FUNCTION ABC.Echo(%s) \n"
                + "RETURNS %s \n"
                + "properties (\n"
                + "    \"symbol\" = \"symbol\",\n"
                + "    \"type\" = \"StarrocksJar\",\n"
                + "    \"file\" = \"http://localhost:8080/\"\n"
                + ");", args, ret);
        return sql;
    }

    @Test
    public void testJScalarUDFNoScalarUnmatchedArgs() {
        assertThrows(SemanticException.class, () -> {
            try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                    mockConstruction(CreateFunctionAnalyzer.class,
                            withSettings().defaultAnswer(CALLS_REAL_METHODS),
                            (mock, context) -> {
                                doReturn("0xff").when(mock).computeMd5(any());
                            });
                    MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                            mockConstruction(UDFInternalClassLoader.class,
                                    (mock, context) -> {
                                        when(mock.loadClass(anyString()))
                                                .thenReturn((Class) ComplexEval.class);
                                    })) {

                try {
                    Config.enable_udf = true;
                    String createFunctionSql = buildFunction("array<string>", "array<int>, array<string>");
                    CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                            createFunctionSql, 32).get(0);
                    new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                    Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
                } finally {
                    Config.enable_udf = false;
                }
            }
        });
    }

    @Test
    public void testJScalarUDFNoScalarUnmatchedRetTypes() {
        assertThrows(SemanticException.class, () -> {
            try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                    mockConstruction(CreateFunctionAnalyzer.class,
                            withSettings().defaultAnswer(CALLS_REAL_METHODS),
                            (mock, context) -> {
                                doReturn("0xff").when(mock).computeMd5(any());
                            });
                    MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                            mockConstruction(UDFInternalClassLoader.class,
                                    (mock, context) -> {
                                        when(mock.loadClass(anyString()))
                                                .thenReturn((Class) ComplexEval.class);
                                    })) {

                try {
                    Config.enable_udf = true;
                    String createFunctionSql = buildFunction("string", "array<int>, map<string,string>");
                    CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                            createFunctionSql, 32).get(0);
                    new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                    Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
                } finally {
                    Config.enable_udf = false;
                }
            }
        });
    }

    public static class EmptyAggEval {
        public static class State {
            public int serializeLength() {
                return 0;
            }
        }

        public State create() {
            return new State();
        }

        public void destroy(State state) {
        }

        public final void update(State state, String columnA, String columnB) {

        }

        public void serialize(State state, java.nio.ByteBuffer buff) {

        }

        public void merge(State state, java.nio.ByteBuffer buffer) {

        }

        public String finalize(State state) {
            return null;
        }
    }

    public static class EmptyAggMapEval {
        public static class State {
            public int serializeLength() {
                return 0;
            }
        }

        public State create() {
            return new State();
        }

        public void destroy(State state) {
        }

        public final void update(State state, Map<String, String> val) {
        }

        public void serialize(State state, java.nio.ByteBuffer buff) {

        }

        public void merge(State state, java.nio.ByteBuffer buffer) {

        }

        public Map<String, String> finalize(State state) {
            return null;
        }
    }

    public static class EmptyAggListEval {
        public static class State {
            public int serializeLength() {
                return 0;
            }
        }

        public State create() {
            return new State();
        }

        public void destroy(State state) {
        }

        public final void update(State state, List<String> val) {
        }

        public void serialize(State state, java.nio.ByteBuffer buff) {

        }

        public void merge(State state, java.nio.ByteBuffer buffer) {

        }

        public List<String> finalize(State state) {
            return null;
        }
    }

    @Test
    public void testJUDAF() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString())).thenAnswer(invocation -> {
                                        String name = invocation.getArgument(0);
                                        if (name.contains("$")) {
                                            return EmptyAggEval.State.class;
                                        }
                                        return EmptyAggEval.class;
                                    });
                                })) {

            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createStmt("symbol", "AGGREGATE");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    @Test
    public void testJUDAFMap() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString())).thenAnswer(invocation -> {
                                        String name = invocation.getArgument(0);
                                        if (name.contains("$")) {
                                            return EmptyAggMapEval.State.class;
                                        }
                                        return EmptyAggMapEval.class;
                                    });
                                })) {

            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createMapStmt("symbol", "AGGREGATE");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    @Test
    public void testJUDAFList() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString())).thenAnswer(invocation -> {
                                        String name = invocation.getArgument(0);
                                        if (name.contains("$")) {
                                            return EmptyAggListEval.State.class;
                                        }
                                        return EmptyAggListEval.class;
                                    });
                                })) {

            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createListStmt("symbol", "AGGREGATE");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    public static class JUDTF {
        public String[] process(String s, String s2) {
            return null;
        }
    }

    @Test
    public void testJUDTF() {
        try (MockedConstruction<CreateFunctionAnalyzer> mockedAnalyzerConstruction =
                mockConstruction(CreateFunctionAnalyzer.class,
                        withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) -> {
                            doReturn("0xff").when(mock).computeMd5(any());
                        });
                MockedConstruction<UDFInternalClassLoader> mockedLoaderConstruction =
                        mockConstruction(UDFInternalClassLoader.class,
                                (mock, context) -> {
                                    when(mock.loadClass(anyString()))
                                            .thenReturn((Class) JUDTF.class);
                                })) {

            try {
                Config.enable_udf = true;
                CreateFunctionStmt stmt = createStmt("symbol", "TABLE");
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        }
    }

    @Test
    public void testPyUDF() {
        CreateFunctionStmt stmt = createPyStmt("a", "Python", "inline");
        Assertions.assertNotNull(stmt.getContent());
        new CreateFunctionAnalyzer().analyze(stmt, connectContext);
    }

    @Test
    public void testPyUDFSymbolEmpty() {
        assertThrows(SemanticException.class, () -> {
            CreateFunctionStmt stmt = createPyStmt("a", "Python", "http://a/a.py.gz");
            Assertions.assertNotNull(stmt.getContent());
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
        });
    }

}
