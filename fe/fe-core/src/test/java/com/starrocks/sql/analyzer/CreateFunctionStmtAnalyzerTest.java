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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

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

    private CreateFunctionStmt createPyInlineStmtNoFile(String symbol) {
        Config.enable_udf = true;
        String createFunctionSql = String.format("CREATE FUNCTION ABC.MY_UDF_JSON_GET_NOFILE(string, string) \n"
                + "RETURNS string \n"
                + "type = 'Python'\n"
                + "symbol = '%s'\n"
                + "AS $$\n"
                + "def a(b):\n"
                + "   return b\n"
                + "$$;", symbol);
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
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    return NormalEval.class;
                }
            };
            CreateFunctionStmt stmt = createStmt("symbol", "");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
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
            new MockUp<UDFInternalClassLoader>() {
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
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
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
        new MockUp<UDFInternalClassLoader>() {
            @Mock
            public final Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                return clazz;
            }
        };
    }

    @Test
    public void testJScalarUDFNoScalarUnmatchedArgs() {
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(ComplexEval.class);
                String createFunctionSql = buildFunction("array<string>", "array<int>, array<string>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFNoScalarUnmatchedRetTypes() {
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(ComplexEval.class);
                String createFunctionSql = buildFunction("string", "array<int>, map<string,string>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
                Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            } finally {
                Config.enable_udf = false;
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
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
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
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJUDAFMap() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    if (name.contains("$")) {
                        return EmptyAggMapEval.State.class;
                    }
                    return EmptyAggMapEval.class;
                }
            };
            CreateFunctionStmt stmt = createMapStmt("symbol", "AGGREGATE");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJUDAFList() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    if (name.contains("$")) {
                        return EmptyAggListEval.State.class;
                    }
                    return EmptyAggListEval.class;
                }
            };
            CreateFunctionStmt stmt = createListStmt("symbol", "AGGREGATE");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
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
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    return JUDTF.class;
                }
            };
            CreateFunctionStmt stmt = createStmt("symbol", "TABLE");
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
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

    @Test
    public void testS3UDF() {

        new MockUp<CreateFunctionAnalyzer>() {
            @Mock
            public String computeMd5(CreateFunctionStmt stmt) {
                return "0xff";
            }
        };

        assertThrows(Throwable.class, () -> {
            try {
                Config.enable_udf = true;
                String createFunctionSql = String.format("CREATE %s FUNCTION decrypt_udf(string, string)  \n"
                                + "RETURNS string \n"
                                + "properties (\n"
                                + "    \"symbol\" = \"%s\",\n"
                                + "    \"type\" = \"StarrocksJar\",\n"
                                + "    \"aws.s3.access_key\" = \"ak\",\n"
                                + "    \"aws.s3.secret_key\" = \"sk\",\n"
                                + "    \"aws.s3.region\" = \"us-east-1\",\n"
                                + "    \"file\" = \"%s\"\n"
                                + ");", "GLOBAL", "com.starrocks.udf.decrypt",
                        "s3://test-bucket/starrocks/udf/test.jar");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    // Varargs UDF test classes
    private static class VarargsStringEval {
        public String evaluate(String... args) {
            if (args == null || args.length == 0) {
                return "";
            }
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                if (args[i] != null) {
                    if (i > 0) {
                        result.append(" ");
                    }
                    result.append(args[i]);
                }
            }
            return result.toString();
        }
    }

    private static class VarargsIntEval {
        public Integer evaluate(Integer... values) {
            if (values == null || values.length == 0) {
                return 0;
            }
            int sum = 0;
            for (Integer value : values) {
                if (value == null) {
                    return null;
                }
                sum += value;
            }
            return sum;
        }
    }

    private static class NonVarargsEval {
        public String evaluate(String a, String b) {
            return a + b;
        }
    }

    // Varargs UDAF test class
    public static class VarargsAggEval {
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

        public final void update(State state, Integer... values) {
            // Variable argument update method
        }

        public void serialize(State state, java.nio.ByteBuffer buff) {
        }

        public void merge(State state, java.nio.ByteBuffer buffer) {
        }

        public Integer finalize(State state) {
            return 0;
        }
    }

    // Varargs UDTF test class
    private static class VarargsTableFunctionEval {
        public String[] process(String... values) {
            return values;
        }
    }

    @Test
    public void testVarargsScalarUDF() {
        try {
            Config.enable_udf = true;
            mockClazz(VarargsStringEval.class);
            
            String createFunctionSql = "CREATE FUNCTION ABC.concat_varargs(string, ...) \n"
                    + "RETURNS string \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            Assertions.assertTrue(stmt.getFunction().hasVarArgs());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testVarargsIntScalarUDF() {
        try {
            Config.enable_udf = true;
            mockClazz(VarargsIntEval.class);
            
            String createFunctionSql = "CREATE FUNCTION ABC.sum_varargs(int, ...) \n"
                    + "RETURNS int \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            Assertions.assertTrue(stmt.getFunction().hasVarArgs());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testVarargsUDFMismatchNoVarargs() {
        // Test error when CREATE declares varargs but Java method doesn't use varargs
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NonVarargsEval.class);
                
                String createFunctionSql = "CREATE FUNCTION ABC.bad_varargs(string, ...) \n"
                        + "RETURNS string \n"
                        + "properties (\n"
                        + "    \"symbol\" = \"symbol\",\n"
                        + "    \"type\" = \"StarrocksJar\",\n"
                        + "    \"file\" = \"http://localhost:8080/\"\n"
                        + ");";
                
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testVarargsUDFTypeMismatch() {
        // Test error when varargs type doesn't match
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(VarargsStringEval.class);
                
                String createFunctionSql = "CREATE FUNCTION ABC.bad_type_varargs(int, ...) \n"
                        + "RETURNS string \n"
                        + "properties (\n"
                        + "    \"symbol\" = \"symbol\",\n"
                        + "    \"type\" = \"StarrocksJar\",\n"
                        + "    \"file\" = \"http://localhost:8080/\"\n"
                        + ");";
                
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testVarargsUDAF() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    if (name.contains("$")) {
                        return VarargsAggEval.State.class;
                    }
                    return VarargsAggEval.class;
                }
            };
            
            String createFunctionSql = "CREATE AGGREGATE FUNCTION ABC.sum_varargs_agg(int, ...) \n"
                    + "RETURNS int \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            Assertions.assertTrue(stmt.getFunction().hasVarArgs());
        } finally {
            Config.enable_udf = false;
        }
    }

    // DECIMAL / BigDecimal UDF test classes

    private static class DecimalScalarEval {
        public java.math.BigDecimal evaluate(java.math.BigDecimal a, java.math.BigDecimal b) {
            if (a == null || b == null) {
                return null;
            }
            return a.add(b);
        }
    }

    private static class DecimalScalarMismatchEval {
        // Wrong Java type: SQL declares DECIMAL but Java uses String.
        public String evaluate(String a) {
            return a;
        }
    }

    public static class DecimalAggEval {
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

        public final void update(State state, java.math.BigDecimal v) {
        }

        public void serialize(State state, java.nio.ByteBuffer buff) {
        }

        public void merge(State state, java.nio.ByteBuffer buffer) {
        }

        public java.math.BigDecimal finalize(State state) {
            return null;
        }
    }

    @Test
    public void testJScalarUDFDecimal() {
        try {
            Config.enable_udf = true;
            mockClazz(DecimalScalarEval.class);

            String sql = "CREATE FUNCTION ABC.dec_add(DECIMAL(10, 2), DECIMAL(10, 2)) \n"
                    + "RETURNS DECIMAL(11, 2) \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFDecimal256() {
        try {
            Config.enable_udf = true;
            mockClazz(DecimalScalarEval.class);

            String sql = "CREATE FUNCTION ABC.dec256_add(DECIMAL(76, 10), DECIMAL(76, 10)) \n"
                    + "RETURNS DECIMAL(76, 10) \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFDecimalTypeMismatch() {
        // Declaring DECIMAL but providing a Java method that takes String should be rejected.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(DecimalScalarMismatchEval.class);

                String sql = "CREATE FUNCTION ABC.bad_dec(DECIMAL(10, 2)) \n"
                        + "RETURNS DECIMAL(10, 2) \n"
                        + "properties (\n"
                        + "    \"symbol\" = \"symbol\",\n"
                        + "    \"type\" = \"StarrocksJar\",\n"
                        + "    \"file\" = \"http://localhost:8080/\"\n"
                        + ");";
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJUDAFDecimal() {
        try {
            Config.enable_udf = true;
            new MockUp<CreateFunctionAnalyzer>() {
                @Mock
                public String computeMd5(CreateFunctionStmt stmt) {
                    return "0xff";
                }
            };
            new MockUp<UDFInternalClassLoader>() {
                @Mock
                public final Class<?> loadClass(String name, boolean resolve)
                        throws ClassNotFoundException {
                    if (name.contains("$")) {
                        return DecimalAggEval.State.class;
                    }
                    return DecimalAggEval.class;
                }
            };

            String sql = "CREATE AGGREGATE FUNCTION ABC.dec_sum(DECIMAL(18, 4)) \n"
                    + "RETURNS DECIMAL(38, 4) \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testVarargsUDTF() {
        try {
            Config.enable_udf = true;
            mockClazz(VarargsTableFunctionEval.class);

            String createFunctionSql = "CREATE TABLE FUNCTION ABC.process_varargs(string, ...) \n"
                    + "RETURNS array<string> \n"
                    + "properties (\n"
                    + "    \"symbol\" = \"symbol\",\n"
                    + "    \"type\" = \"StarrocksJar\",\n"
                    + "    \"file\" = \"http://localhost:8080/\"\n"
                    + ");";

            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
            Assertions.assertTrue(stmt.getFunction().hasVarArgs());
        } finally {
            Config.enable_udf = false;
        }
    }

    // Java type erasure means nested generics (e.g. List<List<Integer>>) collapse to the raw
    // List/Map at reflection time, so the UDF's evaluate signature only carries List/Map. The
    // FE validates the SQL-side nested shape; runtime conversion is driven by the SQL signature.
    private static class NestedArrayEval {
        public List<?> evaluate(List<?> a) {
            return a;
        }
    }

    private static class NestedMapEval {
        public Map<?, ?> evaluate(Map<?, ?> a) {
            return a;
        }
    }

    @Test
    public void testJScalarUDFNestedArrayOfArray() {
        try {
            Config.enable_udf = true;
            mockClazz(NestedArrayEval.class);
            String sql = buildFunction("array<array<int>>", "array<array<int>>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFNestedArrayOfMap() {
        try {
            Config.enable_udf = true;
            mockClazz(NestedArrayEval.class);
            String sql = buildFunction("array<map<int,string>>", "array<map<int,string>>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFNestedMapWithArrayValue() {
        try {
            Config.enable_udf = true;
            mockClazz(NestedMapEval.class);
            String sql = buildFunction("map<int,array<string>>", "map<int,array<string>>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFDeeplyNestedArray() {
        try {
            Config.enable_udf = true;
            mockClazz(NestedArrayEval.class);
            String sql = buildFunction("array<array<array<int>>>", "array<array<array<int>>>");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFNestedTypeMismatch() {
        // Nested SQL type still requires Java raw List/Map: passing a String parameter must fail.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NormalEval.class);
                String sql = buildFunction("array<array<int>>", "array<array<int>>, string");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    // The negative cases below exercise each error branch of checkScalarUdfType so
    // every "does not support type" / "type does not match" / "non-scalar type" path
    // is covered, plus the recursive `return false` in isSupportedScalarUdfType.

    @Test
    public void testJScalarUDFUnsupportedScalarReturnType() {
        // SQL return type is JSON, which has no entry in PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE,
        // so checkScalarUdfType hits the `cls == null` branch on the ScalarType arm.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NormalEval.class);
                String sql = buildFunction("json", "string, string");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFUnsupportedScalarArgType() {
        // SQL arg type LARGEINT (BIGINT-mapped Java is Long, but PrimitiveType.LARGEINT
        // is NOT in PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE) — the ScalarType branch raises
        // "does not support type".
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NormalEval.class);
                String sql = buildFunction("string", "largeint, string");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFArrayUnsupportedItem() {
        // array<json>: ArrayType branch passes the List Java-type check, then
        // isSupportedScalarUdfType recurses into JSON and returns false — the
        // "does not support type 'array<...>'" branch fires.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NestedArrayEval.class);
                String sql = buildFunction("string", "array<json>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFMapWithNonMapJava() {
        // SQL map<int,int> declared but Java parameter is String, not Map: the
        // MapType branch raises "type does not match Map".
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NormalEval.class);
                String sql = buildFunction("string", "map<int,int>, string");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFMapUnsupportedKey() {
        // map<largeint,int>: Java type is Map (passes), but the key recurses to
        // LARGEINT — it parses as a base type for map keys yet has no entry in
        // PRIMITIVE_TYPE_TO_JAVA_CLASS_TYPE, so isSupportedScalarUdfType returns
        // false and the MapType branch raises "does not support type 'map<...>'".
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NestedMapEval.class);
                String sql = buildFunction("string", "map<largeint,int>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFMapUnsupportedValue() {
        // map<int,json>: same recursive failure on the value side. JSON is allowed
        // as a map value (just not as a key) so this still reaches the analyzer.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NestedMapEval.class);
                String sql = buildFunction("string", "map<int,json>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFTopLevelStruct() {
        // STRUCT is not ScalarType / ArrayType / MapType — checkScalarUdfType
        // falls through to the catch-all "does not support non-scalar type" report.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NormalEval.class);
                String sql = buildFunction("string", "struct<a int>, string");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testJScalarUDFArrayOfStruct() {
        // array<struct<...>>: ArrayType branch recurses to the struct child, which is
        // neither scalar nor array nor map, so isSupportedScalarUdfType drops through
        // to its trailing `return false` — the only path that exercises that line.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(NestedArrayEval.class);
                String sql = buildFunction("string", "array<struct<a int>>");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(sql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }

    @Test
    public void testPyInlineUDFWithoutFileProperty() {
        CreateFunctionStmt stmt = createPyInlineStmtNoFile("a");
        Assertions.assertNotNull(stmt.getContent(), "Inline body must be parsed into content");
        new CreateFunctionAnalyzer().analyze(stmt, connectContext);
        Assertions.assertEquals("inline", stmt.getFunction().getLocation().toString(),
                "Analyzer must auto-set location to 'inline' when file property is omitted");
    }
    public static class DateEval {
        public java.time.LocalDateTime evaluate(java.time.LocalDate d, java.time.LocalDateTime ts) {
            return ts;
        }
    }

    @Test
    public void testJScalarUDFDateDateTime() {
        try {
            Config.enable_udf = true;
            mockClazz(DateEval.class);
            String createFunctionSql = buildFunction("datetime", "date, datetime");
            CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                    createFunctionSql, 32).get(0);
            new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            Assertions.assertEquals("0xff", stmt.getFunction().getChecksum());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testJScalarUDFDateMismatchedJavaType() {
        // Java method takes LocalDate but SQL declares STRING; analyzer must reject the mismatch.
        assertThrows(SemanticException.class, () -> {
            try {
                Config.enable_udf = true;
                mockClazz(DateEval.class);
                String createFunctionSql = buildFunction("datetime", "string, datetime");
                CreateFunctionStmt stmt = (CreateFunctionStmt) com.starrocks.sql.parser.SqlParser.parse(
                        createFunctionSql, 32).get(0);
                new CreateFunctionAnalyzer().analyze(stmt, connectContext);
            } finally {
                Config.enable_udf = false;
            }
        });
    }
}
