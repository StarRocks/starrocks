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

package com.starrocks.jdbcbridge;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class JDBCScannerArrayTest {
    @Test
    public void testOpenInitializesArrayAndScalarColumnsTogether() throws Exception {
        assertOpenArrayColumns(true);
    }

    @Test
    public void testNonPostgresListMetadataKeepsStringFallback() throws Exception {
        assertOpenArrayColumns(false);
    }

    private void assertOpenArrayColumns(boolean postgres) throws Exception {
        List<String> names = List.of("_text", "date", "int4", "_varchar");
        String arrayClass = postgres ? "java.sql.Array" : "java.util.List";
        List<String> classes = List.of(arrayClass, "java.sql.Date", "java.lang.Integer", arrayClass);
        int[] types = {Types.ARRAY, Types.DATE, Types.INTEGER, Types.ARRAY};
        AtomicInteger row = new AtomicInteger(-1);
        AtomicInteger freed = new AtomicInteger();
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (obj, method, args) -> {
            switch (method.getName()) {
                case "getColumnCount": return 4;
                case "getColumnTypeName": return names.get((int) args[0] - 1);
                case "getColumnClassName": return classes.get((int) args[0] - 1);
                case "getColumnType": return types[(int) args[0] - 1];
                default: return defaultValue(method);
            }
        });
        ResultSet rs = proxy(ResultSet.class, (obj, method, args) -> {
            switch (method.getName()) {
                case "getMetaData": return metadata;
                case "next": return row.incrementAndGet() < 2;
                case "getArray":
                    Assertions.assertTrue(postgres, "Other drivers retain getString fallback");
                    if (row.get() == 1) {
                        return null;
                    }
                    return proxy(java.sql.Array.class, (array, arrayMethod, arrayArgs) -> {
                        if (arrayMethod.getName().equals("free")) {
                            freed.incrementAndGet();
                            return null;
                        }
                        if (arrayMethod.getName().equals("getArray")) {
                            return new String[] {"abc", null, "行🙂"};
                        }
                        throw new AssertionError(arrayMethod.getName());
                    });
                case "getObject":
                    int column = (int) args[0];
                    if (column == 1 || column == 4) {
                        Assertions.assertFalse(postgres, "PostgreSQL arrays must use getArray");
                        return row.get() == 1 ? null : Arrays.asList("abc", null, "行🙂");
                    }
                    if (column == 3) {
                        return row.get() + 7;
                    }
                    Assertions.assertEquals(2, column, "Array columns must not use getObject");
                    return args.length == 2 ? java.time.LocalDate.of(2026, 1, 1) : java.sql.Date.valueOf("2026-01-01");
                case "getString":
                    Assertions.assertFalse(postgres);
                    return "[abc, null, 行🙂]";
                default: return defaultValue(method);
            }
        });
        PreparedStatement statement = proxy(PreparedStatement.class, (obj, method, args) -> {
            if (method.getName().equals("executeQuery") || method.getName().equals("getResultSet")) {
                return rs;
            }
            return defaultValue(method);
        });
        Connection connection = proxy(Connection.class, (obj, method, args) -> {
            return method.getName().equals("prepareStatement") ? statement : defaultValue(method);
        });
        JDBCScanContext context = new JDBCScanContext(postgres ? "org.postgresql.Driver" : "com.mysql.cj.jdbc.Driver",
                "jdbc:test:" + UUID.randomUUID(),
                "test", "", "SELECT items, d, id, labels FROM t", "UTC", 2, 1, 0, 10000, 10000, 60000, 0);
        String key = context.getUser() + "/" + context.getPassword() + "/" + context.getJdbcURL();
        HikariDataSource source = new HikariDataSource() {
            @Override
            public Connection getConnection() {
                return connection;
            }
        };
        DataSourceCache cache = DataSourceCache.getInstance();
        cache.getSource(key, () -> new DataSourceCache.DataSourceCacheItem(source, getClass().getClassLoader()));
        JDBCScanner scanner = new JDBCScanner("unused", context);
        try {
            scanner.open();
            Assertions.assertEquals(List.of("java.util.List", "java.sql.Date", "java.lang.Integer", "java.util.List"),
                    scanner.getResultColumnClassNames());
            Assertions.assertTrue(scanner.hasNext());
            List<Object[]> chunk = scanner.getNextChunk();
            Assertions.assertEquals(2, scanner.getResultNumRows());
            for (int column : new int[] {0, 3}) {
                Assertions.assertTrue((postgres ? List[].class : String[].class).isInstance(chunk.get(column)));
                Assertions.assertEquals(postgres ? Arrays.asList("abc", null, "行🙂") : "[abc, null, 行🙂]",
                        chunk.get(column)[0]);
                Assertions.assertNull(chunk.get(column)[1]);
            }
            Assertions.assertEquals("2026-01-01", chunk.get(1)[0].toString());
            Assertions.assertArrayEquals(new Integer[] {7, 8}, chunk.get(2));
            Assertions.assertEquals(postgres ? 2 : 0, freed.get());
            Assertions.assertFalse(scanner.hasNext());
        } finally {
            try {
                scanner.close();
            } finally {
                Field sources = DataSourceCache.class.getDeclaredField("sources");
                sources.setAccessible(true);
                ((Map<?, ?>) sources.get(cache)).remove(key);
                source.close();
            }
        }
    }

    private <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {type}, handler));
    }

    private Object defaultValue(Method method) {
        if (method.getReturnType() == boolean.class) {
            return false;
        }
        if (method.getReturnType() == int.class) {
            return 0;
        }
        return null;
    }

    @Test
    public void testOnlyPostgresBuiltinStringArraysAreConverted() throws Exception {
        JDBCScanner pg = scanner("org.postgresql.Driver", 2);
        Method method = JDBCScanner.class.getDeclaredMethod("isPostgresStringArrayColumn", int.class, String.class);
        method.setAccessible(true);
        for (String name : List.of("_text", "_varchar", "_TEXT", "_VARCHAR")) {
            Assertions.assertEquals(true, method.invoke(pg, Types.ARRAY, name));
        }
        for (String name : Arrays.asList("_int4", "_bpchar", "_jsonb", "text", "text[]", "custom._text", null)) {
            Assertions.assertEquals(false, method.invoke(pg, Types.ARRAY, name));
        }
        Assertions.assertEquals(false, method.invoke(pg, Types.VARCHAR, "_text"));
        Assertions.assertEquals(false, method.invoke(scanner("com.mysql.cj.jdbc.Driver", 2), Types.ARRAY, "_text"));
    }

    @Test
    public void testNullEmptyNullElementsAndEscapingAcrossChunks() throws Exception {
        String[] strings = {"", "NULL", null, "a,b", "{braces}", "quote\"", "slash\\", "行🙂", "line\nbreak"};
        AtomicInteger freed = new AtomicInteger();
        JDBCScanner scanner = preparedScanner(new Object[] {null, new String[0], strings, new String[] {null},
                new String[] {"last"}}, 2, freed);
        List<Object> rows = new ArrayList<>();
        List<Integer> chunkSizes = new ArrayList<>();
        while (scanner.hasNext()) {
            Object[] values = scanner.getNextChunk().get(0);
            Assertions.assertTrue(values instanceof List<?>[]);
            int size = scanner.getResultNumRows();
            chunkSizes.add(size);
            rows.addAll(Arrays.asList(values).subList(0, size));
        }
        Assertions.assertEquals(Arrays.asList(null, List.of(), Arrays.asList(strings),
                Arrays.asList((String) null), List.of("last")), rows);
        Assertions.assertEquals(List.of(2, 2, 1), chunkSizes);
        Assertions.assertEquals(4, freed.get());
    }

    @Test
    public void testRejectMultidimensionalArraysAndFree() throws Exception {
        for (Object array : List.of(new String[][] {{"a", null}, {"b", "c"}}, new String[0][0],
                new String[][][] {{{"x"}}})) {
            AtomicInteger freed = new AtomicInteger();
            JDBCScanner scanner = preparedScanner(new Object[] {array}, 1, freed);
            Assertions.assertTrue(scanner.hasNext());
            SQLException error = Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
            Assertions.assertTrue(error.getMessage().contains("column[1]"));
            Assertions.assertTrue(error.getMessage().contains("one-dimensional"));
            Assertions.assertEquals(1, freed.get());
        }
    }

    @Test
    public void testRejectUnexpectedElementRepresentationAndFree() throws Exception {
        AtomicInteger freed = new AtomicInteger();
        JDBCScanner scanner = preparedScanner(new Object[] {new Integer[] {1, 2}}, 1, freed);
        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
        Assertions.assertEquals(1, freed.get());
    }

    @Test
    public void testFreeArrayOnDriverFailure() throws Exception {
        AtomicInteger freed = new AtomicInteger();
        SQLException failure = new SQLException("driver read failed");
        JDBCScanner scanner = preparedScanner(new Object[] {failure}, 1, freed);
        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertSame(failure, Assertions.assertThrows(SQLException.class, scanner::getNextChunk));
        Assertions.assertEquals(1, freed.get());
    }

    private JDBCScanner scanner(String driver, int fetchSize) {
        JDBCScanContext context = new JDBCScanContext(driver, "jdbc:postgresql://localhost/db", "user", "password",
                "SELECT a FROM t", "UTC", fetchSize, 1, 0, 10000, 10000, 60000, 0);
        return new JDBCScanner("unused.jar", context);
    }

    private JDBCScanner preparedScanner(Object[] rows, int fetchSize, AtomicInteger freed) throws Exception {
        JDBCScanner scanner = scanner("org.postgresql.Driver", fetchSize);
        AtomicInteger row = new AtomicInteger(-1);
        ResultSet rs = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("next")) {
                        return row.incrementAndGet() < rows.length;
                    }
                    if (method.getName().equals("getArray")) {
                        Object value = rows[row.get()];
                        if (value == null) {
                            return null;
                        }
                        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {java.sql.Array.class},
                                (arrayProxy, arrayMethod, arrayArgs) -> {
                                    if (arrayMethod.getName().equals("free")) {
                                        freed.incrementAndGet();
                                        return null;
                                    }
                                    if (arrayMethod.getName().equals("getArray")) {
                                        if (value instanceof SQLException) {
                                            throw (SQLException) value;
                                        }
                                        return value;
                                    }
                                    throw new AssertionError(arrayMethod.getName());
                                });
                    }
                    throw new AssertionError("Array reads must use getArray, received: " + method.getName());
                });
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {ResultSetMetaData.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getColumnCount")) {
                        return 1;
                    }
                    throw new AssertionError(method.getName());
                });
        set(scanner, "resultSet", rs);
        set(scanner, "resultSetMetaData", metadata);
        set(scanner, "postgresStringArrayColumns", List.of(true));
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new List<?>[fetchSize]);
        set(scanner, "resultChunk", chunk);
        return scanner;
    }

    private void set(JDBCScanner scanner, String name, Object value) throws Exception {
        Field field = JDBCScanner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(scanner, value);
    }
}
