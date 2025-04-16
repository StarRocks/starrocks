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
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CreateFunctionStmtAnalyzerTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeClass
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

    @Test(expected = Throwable.class)
    public void testJUDF() {
        try {
            Config.enable_udf = true;
            CreateFunctionStmt stmt = createStmt("symbol", "");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
        } finally {
            Config.enable_udf = false;
        }
    }

    private static class NormalEval {
        public String evaluate(String a, String b) {
            return a + b;
        }
    }

    @Test
    public void testJScalarUDF() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<CreateFunctionAnalyzer.UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    return NormalEval.class;
                }
            };
            CreateFunctionStmt stmt = createStmt("symbol", "");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    private static class ComplexEval {
        public List<?> evaluate(List<?> a, Map<?, ?> b) {
            return Lists.newArrayList();
        }
    }

    @Test
    public void testJScalarUDFNoScalarInputs() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<CreateFunctionAnalyzer.UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    return ComplexEval.class;
                }
            };

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
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
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
    void mockClazz(Class<?> clazz) {
        new MockUp<CreateFunctionAnalyzer>() {
            @Mock
            public String computeMd5(CreateFunctionStmt stmt) {
                return "0xff";
            }
        };
        new MockUp<CreateFunctionAnalyzer.UDFInternalClassLoader>() {
            @Mock
            public final Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                return clazz;
            }
        };
    }

    @Test(expected = SemanticException.class)
    public void testJScalarUDFNoScalarUnmatchedArgs() {
        try {
            Config.enable_udf = true;
            mockClazz(ComplexEval.class);
            String createFunctionSql = buildFunction("array<string>", "array<int>, array<string>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test(expected = SemanticException.class)
    public void testJScalarUDFNoScalarUnmatchedRetTypes() {
        try {
            Config.enable_udf = true;
            mockClazz(ComplexEval.class);
            String createFunctionSql = buildFunction("string", "array<int>, map<string,string>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
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

    @Test
    public void testJUDAF() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<CreateFunctionAnalyzer.UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    if (name.contains("$")) {
                        return EmptyAggEval.State.class;
                    }
                    return EmptyAggEval.class;
                }
            };
            CreateFunctionStmt stmt = createStmt("symbol", "AGGREGATE");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    public static class JUDTF {
        public String[] process(String s, String s2) {
            return null;
        }
    }

    @Test
    public void testJUDTF() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<CreateFunctionAnalyzer.UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    return JUDTF.class;
                }
            };
            CreateFunctionStmt stmt = createStmt("symbol", "TABLE");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assert.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testPyUDF() {
        CreateFunctionStmt stmt = createPyStmt("a", "Python", "inline");
        Assert.assertNotNull(stmt.getContent());
        new CreateFunctionAnalyzer().analyze(stmt, connectContext);
    }

    @Test(expected = SemanticException.class)
    public void testPyUDFSymbolEmpty() {
        CreateFunctionStmt stmt = createPyStmt("a", "Python", "http://a/a.py.gz");
        Assert.assertNotNull(stmt.getContent());
        new CreateFunctionAnalyzer().analyze(stmt, connectContext);
    }

}
