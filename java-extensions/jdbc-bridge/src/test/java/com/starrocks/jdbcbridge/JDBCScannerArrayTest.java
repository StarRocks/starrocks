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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
    public void testOnlyPostgresBuiltinArrayElementTypesAreConverted() throws Exception {
        JDBCScanner pg = scanner("org.postgresql.Driver", 2);
        Method method = JDBCScanner.class.getDeclaredMethod("postgresArrayElementTypeName", int.class, String.class);
        method.setAccessible(true);
        Map<String, String> accepted = new LinkedHashMap<>();
        accepted.put("_bool", "bool");
        accepted.put("_int2", "int2");
        accepted.put("_int4", "int4");
        accepted.put("_int8", "int8");
        accepted.put("_float4", "float4");
        accepted.put("_float8", "float8");
        accepted.put("_text", "text");
        accepted.put("_varchar", "varchar");
        accepted.put("_bpchar", "bpchar");
        accepted.put("_date", "date");
        accepted.put("_timestamp", "timestamp");
        for (Map.Entry<String, String> entry : accepted.entrySet()) {
            Assertions.assertEquals(entry.getValue(), method.invoke(pg, Types.ARRAY, entry.getKey()));
            Assertions.assertEquals(entry.getValue(),
                    method.invoke(pg, Types.ARRAY, entry.getKey().toUpperCase(Locale.ROOT)));
        }
        // _timestamptz is the one that cannot be told from an accepted name by its Java class: the
        // driver returns java.sql.Timestamp[] for both it and _timestamp.
        for (String name : Arrays.asList("_timestamptz", "_TIMESTAMPTZ", "_numeric", "_uuid", "_time", "_timetz",
                "_bytea", "_jsonb", "text", "text[]", "custom._text", null)) {
            Assertions.assertNull(method.invoke(pg, Types.ARRAY, name), name);
        }
        Assertions.assertNull(method.invoke(pg, Types.VARCHAR, "_text"));
        Assertions.assertNull(method.invoke(scanner("com.mysql.cj.jdbc.Driver", 2), Types.ARRAY, "_text"));
    }

    @Test
    public void testNonStringElementsKeepTheirDriverClass() throws Exception {
        AtomicInteger freed = new AtomicInteger();
        // The BE array writer builds its element array from UDFHelper.clazzs, so each element has
        // to arrive already as the class that column's logical type expects.
        Object[] arrays = {new Integer[] {1, null, 3}, new Long[] {4L}, new Short[] {(short) 5},
                new Boolean[] {true, false}, new Double[] {1.5d}, new Float[] {0.5f},
                new String[] {"ab  "}};
        JDBCScanner scanner = preparedScanner(arrays, arrays.length, freed);
        Assertions.assertTrue(scanner.hasNext());
        Object[] values = scanner.getNextChunk().get(0);
        Assertions.assertEquals(arrays.length, scanner.getResultNumRows());
        Assertions.assertEquals(Arrays.asList(1, null, 3), values[0]);
        Assertions.assertEquals(List.of(4L), values[1]);
        Assertions.assertEquals(List.of((short) 5), values[2]);
        Assertions.assertEquals(List.of(true, false), values[3]);
        Assertions.assertEquals(List.of(1.5d), values[4]);
        Assertions.assertEquals(List.of(0.5f), values[5]);
        Assertions.assertEquals(List.of("ab  "), values[6]);
        Assertions.assertEquals(arrays.length, freed.get());
    }

    @Test
    public void testTemporalElementsAreReadAsJavaTimeWithoutGoingThroughSqlDate() throws Exception {
        // java.sql.Date/Timestamp carry a hybrid Julian calendar and rebuild their fields in the
        // JVM default zone, so 1582-10-10 comes back as 1582-10-20 and a value in a
        // daylight-saving gap moves an hour. The reader must ask the driver for java.time per
        // element instead; temporalScanner's array fails the test if getArray() is called at all.
        AtomicInteger freed = new AtomicInteger();
        List<Object> dates = Arrays.asList(LocalDate.of(1582, 10, 10), null, LocalDate.of(9999, 12, 31));
        JDBCScanner scanner = temporalScanner(List.of(dates), LocalDate.class, Types.DATE, freed);
        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertEquals(dates, scanner.getNextChunk().get(0)[0]);
        Assertions.assertEquals(1, freed.get());

        freed.set(0);
        List<Object> timestamps = Arrays.asList(LocalDateTime.of(1582, 10, 10, 12, 34, 56, 123456000),
                LocalDateTime.of(2026, 3, 8, 2, 30, 0, 123456000), null);
        scanner = temporalScanner(List.of(timestamps), LocalDateTime.class, Types.TIMESTAMP, freed);
        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertEquals(timestamps, scanner.getNextChunk().get(0)[0]);
        Assertions.assertEquals(1, freed.get());
    }

    @Test
    public void testTemporalElementsOutsideTheSupportedRangeFail() throws Exception {
        // A BC value arrives proleptic (year 0 and below) and the driver represents +/-infinity
        // with java.time's extreme years. An array element cannot be nulled without moving the
        // parent offsets, so the row has to fail the way the scalar read fails.
        for (Object value : List.of(LocalDate.of(0, 1, 1), LocalDate.of(-4713, 1, 1), LocalDate.MAX, LocalDate.MIN)) {
            AtomicInteger freed = new AtomicInteger();
            JDBCScanner scanner = temporalScanner(List.of(Arrays.asList(LocalDate.of(2026, 1, 1), value)),
                    LocalDate.class, Types.DATE, freed);
            Assertions.assertTrue(scanner.hasNext());
            SQLException error = Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
            Assertions.assertTrue(error.getMessage().contains("0001-01-01 through 9999-12-31"), error.getMessage());
            Assertions.assertEquals(1, freed.get());
        }
    }

    @Test
    public void testRejectMultidimensionalTemporalArraysAndFree() throws Exception {
        // A multidimensional value reports its elements as arrays in turn, which is what the
        // element metadata says before any element is converted -- an empty outer value has no
        // element to fail on.
        for (List<Object> elements : List.of(List.of(), Arrays.asList((Object) null))) {
            AtomicInteger freed = new AtomicInteger();
            JDBCScanner scanner = temporalScanner(List.of(elements), LocalDate.class, Types.ARRAY, freed);
            Assertions.assertTrue(scanner.hasNext());
            SQLException error = Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
            Assertions.assertTrue(error.getMessage().contains("one-dimensional"), error.getMessage());
            Assertions.assertEquals(1, freed.get());
        }
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
        // A primitive array is not an Object[] at all, and byte[][] is the shape a bytea[] would
        // arrive in: its component type is itself an array, which is how a multidimensional value
        // is recognised, so it must not slip through as a one-dimensional value of byte arrays.
        for (Object array : List.of(new int[] {1, 2}, new byte[][] {{1}, {2}})) {
            AtomicInteger freed = new AtomicInteger();
            JDBCScanner scanner = preparedScanner(new Object[] {array}, 1, freed);
            Assertions.assertTrue(scanner.hasNext());
            SQLException error = Assertions.assertThrows(SQLException.class, scanner::getNextChunk);
            Assertions.assertTrue(error.getMessage().contains("one-dimensional"), error.getMessage());
            Assertions.assertEquals(1, freed.get());
        }
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
        set(scanner, "postgresArrayColumns", List.of(true));
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new List<?>[fetchSize]);
        set(scanner, "resultChunk", chunk);
        return scanner;
    }

    /**
     * A scanner over one PostgreSQL date[] / timestamp[] column whose array answers
     * getResultSet() and fails the test on getArray(), so a reader that went through
     * java.sql.Date or java.sql.Timestamp cannot pass.
     */
    private JDBCScanner temporalScanner(List<List<Object>> rows, Class<?> temporalClass, int elementJdbcType,
                                        AtomicInteger freed) throws Exception {
        JDBCScanner scanner = scanner("org.postgresql.Driver", rows.size());
        AtomicInteger row = new AtomicInteger(-1);
        ResultSetMetaData elementMetadata = proxy(ResultSetMetaData.class, (p, method, args) ->
                method.getName().equals("getColumnType") ? elementJdbcType : defaultValue(method));
        ResultSet rs = proxy(ResultSet.class, (p, method, args) -> {
            switch (method.getName()) {
                case "next": return row.incrementAndGet() < rows.size();
                case "getArray":
                    List<Object> elements = rows.get(row.get());
                    AtomicInteger cursor = new AtomicInteger(-1);
                    return proxy(java.sql.Array.class, (arrayProxy, arrayMethod, arrayArgs) -> {
                        switch (arrayMethod.getName()) {
                            case "free": freed.incrementAndGet(); return null;
                            case "getResultSet":
                                return proxy(ResultSet.class, (elemProxy, elemMethod, elemArgs) -> {
                                    switch (elemMethod.getName()) {
                                        case "getMetaData": return elementMetadata;
                                        case "next": return cursor.incrementAndGet() < elements.size();
                                        case "getObject":
                                            Assertions.assertEquals(2, elemArgs[0]);
                                            Assertions.assertSame(temporalClass, elemArgs[1],
                                                    "Elements must be requested as java.time");
                                            return elements.get(cursor.get());
                                        default: return defaultValue(elemMethod);
                                    }
                                });
                            default:
                                throw new AssertionError("A temporal array must not be read through "
                                        + arrayMethod.getName() + ": java.sql.Date and java.sql.Timestamp "
                                        + "shift the value");
                        }
                    });
                default: throw new AssertionError(method.getName());
            }
        });
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (p, method, args) ->
                method.getName().equals("getColumnCount") ? 1 : defaultValue(method));
        set(scanner, "resultSet", rs);
        set(scanner, "resultSetMetaData", metadata);
        set(scanner, "postgresArrayColumns", List.of(true));
        set(scanner, "postgresLocalTemporalColumns", Collections.singletonList(temporalClass));
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new List<?>[rows.size()]);
        set(scanner, "resultChunk", chunk);
        return scanner;
    }

    private void set(JDBCScanner scanner, String name, Object value) throws Exception {
        Field field = JDBCScanner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(scanner, value);
    }
}
