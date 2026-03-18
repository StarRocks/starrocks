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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class JDBCScannerTest {

    @Test
    public void testOracleVarcharColumnDoesNotUseTemporalConversion() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("java.lang.String"));

        Assertions.assertFalse(invokeShouldConvertOracleTemporalStringColumn(scanner, 0));
    }

    @Test
    public void testOracleTimestamptzColumnConvertsToQueryTimeZone() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMPTZ"));

        Assertions.assertTrue(invokeShouldConvertOracleTemporalStringColumn(scanner, 0));
        String converted = invokeConvertOracleTemporalValueToString(scanner, 0,
                "2026-03-12 09:30:15.123456 +00:00");
        Assertions.assertEquals("2026-03-12 17:30:15.123456", converted);
    }

    @Test
    public void testOracleTimestampWithoutTimezoneIsNotNormalized() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMP"));

        Assertions.assertTrue(invokeShouldConvertOracleTemporalStringColumn(scanner, 0));
        String source = "2026-03-12 09:30:15.123456";
        String converted = invokeConvertOracleTemporalValueToString(scanner, 0, source);
        Assertions.assertEquals(source, converted);
    }

    @Test
    public void testOracleTemporalConversionDisabledWithoutQueryTimeZone() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", null, 1);
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMPTZ"));

        Assertions.assertFalse(invokeShouldConvertOracleTemporalStringColumn(scanner, 0));
    }

    @Test
    public void testOracleQueryTimeZoneShortIdAndInvalidValue() {
        JDBCScanner shortIdScanner = createScanner("oracle.jdbc.driver.OracleDriver", "EST", 1);
        Assertions.assertNotNull(shortIdScanner);

        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> createScanner("oracle.jdbc.driver.OracleDriver", "Invalid/QueryTZ", 1));
        Assertions.assertTrue(ex.getMessage().contains("invalid query time zone"));
    }

    @Test
    public void testResolveOracleSessionTimeZoneBranches() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);

        Assertions.assertNull(invokeResolveOracleSessionTimeZone(scanner, null));
        Assertions.assertNull(invokeResolveOracleSessionTimeZone(scanner, ""));
        Assertions.assertEquals(ZoneOffset.of("+08:00"), invokeResolveOracleSessionTimeZone(scanner, "+08:00"));
        Assertions.assertEquals(ZoneOffset.of("+09:30"), invokeResolveOracleSessionTimeZone(scanner, "09:30"));
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"), invokeResolveOracleSessionTimeZone(scanner, "Asia/Shanghai"));
        Assertions.assertNotNull(invokeResolveOracleSessionTimeZone(scanner, "EST"));

        InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class,
                () -> invokeResolveOracleSessionTimeZoneRaw(scanner, "Invalid/SessionTZ"));
        Assertions.assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    @Test
    public void testInitOracleSessionTimeZoneIfNeededNoopForNonOracle() throws Exception {
        JDBCScanner scanner = createScanner("com.mysql.jdbc.Driver", "Asia/Shanghai", 1);
        Connection connection = proxy(Connection.class, (method, args) -> {
            if ("prepareStatement".equals(method.getName())) {
                throw new AssertionError("prepareStatement should not be called for non-oracle scanner");
            }
            return defaultValue(method);
        });
        setField(scanner, "connection", connection);

        invokeInitOracleSessionTimeZoneIfNeeded(scanner);
        Assertions.assertNull(getField(scanner, "oracleSessionTimeZone"));
    }

    @Test
    public void testInitOracleSessionTimeZoneIfNeededLoadsFromDual() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);

        ResultSet rs = proxy(ResultSet.class, (method, args) -> {
            if ("next".equals(method.getName())) {
                return true;
            }
            if ("getString".equals(method.getName())) {
                return "+08:00";
            }
            return defaultValue(method);
        });

        PreparedStatement ps = proxy(PreparedStatement.class, (method, args) -> {
            if ("executeQuery".equals(method.getName())) {
                return rs;
            }
            return defaultValue(method);
        });

        Connection connection = proxy(Connection.class, (method, args) -> {
            if ("prepareStatement".equals(method.getName())) {
                return ps;
            }
            return defaultValue(method);
        });

        setField(scanner, "connection", connection);
        invokeInitOracleSessionTimeZoneIfNeeded(scanner);
        Assertions.assertEquals(ZoneOffset.of("+08:00"), getField(scanner, "oracleSessionTimeZone"));
    }

    @Test
    public void testConvertOracleTimestampLtzHandlesNullAndConvertsValue() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMPLTZ"));

        ResultSet nullTsRs = proxy(ResultSet.class, (method, args) -> {
            if ("getTimestamp".equals(method.getName())) {
                return null;
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", nullTsRs);
        Assertions.assertNull(invokeConvertOracleTemporalValueToString(scanner, 0, new Object()));

        ResultSet valueTsRs = proxy(ResultSet.class, (method, args) -> {
            if ("getTimestamp".equals(method.getName())) {
                return Timestamp.valueOf("2026-03-12 09:30:15");
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", valueTsRs);
        setField(scanner, "oracleSessionTimeZone", ZoneOffset.of("+00:00"));
        String converted = invokeConvertOracleTemporalValueToString(scanner, 0, new Object());
        Assertions.assertEquals("2026-03-12 17:30:15.000000", converted);
    }

    @Test
    public void testConvertOracleTemporalValueUsesResultSetGetStringForNonStringObject() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMPTZ"));

        ResultSet rs = proxy(ResultSet.class, (method, args) -> {
            if ("getString".equals(method.getName())) {
                return "2026-03-12 09:30:15.123456 +00:00";
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", rs);

        String converted = invokeConvertOracleTemporalValueToString(scanner, 0, new Object());
        Assertions.assertEquals("2026-03-12 17:30:15.123456", converted);
    }

    @Test
    public void testNormalizeTimestampStringWithZoneFormatAndFallback() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        String zoneFormatValue = "2026-03-12 09:30:15.123456 UTC";
        String converted = invokeNormalizeTimestampStringToQueryTimeZone(scanner, zoneFormatValue);
        Assertions.assertEquals("2026-03-12 17:30:15.123456", converted);

        String fallback = invokeNormalizeTimestampStringToQueryTimeZone(scanner, "not-a-timestamp");
        Assertions.assertEquals("not-a-timestamp", fallback);
    }

    @Test
    public void testNormalizeTimestampStringReturnsOriginalWhenQueryTimeZoneMissing() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", null, 1);
        String value = invokeNormalizeTimestampStringToQueryTimeZone(scanner, "abc");
        Assertions.assertEquals("abc", value);
    }

    @Test
    public void testGetNextChunkStringColumnPaths() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setField(scanner, "resultSetMetaData", singleColumnMetaData());
        setField(scanner, "resultChunk", singleStringColumnChunk());
        setResultColumnClassNames(scanner, List.of("java.lang.String"));

        ResultSet directStringRs = proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                return "plain-text";
            }
            if ("getString".equals(method.getName())) {
                return "fallback";
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", directStringRs);
        List<Object[]> chunk = scanner.getNextChunk();
        Assertions.assertEquals("plain-text", ((String[]) chunk.get(0))[0]);

        setField(scanner, "resultChunk", singleStringColumnChunk());
        ResultSet fallbackStringRs = proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                return 123;
            }
            if ("getString".equals(method.getName())) {
                return "fallback-value";
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", fallbackStringRs);
        chunk = scanner.getNextChunk();
        Assertions.assertEquals("fallback-value", ((String[]) chunk.get(0))[0]);

        setField(scanner, "resultChunk", singleStringColumnChunk());
        setResultColumnClassNames(scanner, List.of("oracle.sql.TIMESTAMPTZ"));
        ResultSet temporalStringRs = proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                return "2026-03-12 09:30:15.123456 +00:00";
            }
            if ("getString".equals(method.getName())) {
                return "2026-03-12 09:30:15.123456 +00:00";
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", temporalStringRs);
        chunk = scanner.getNextChunk();
        Assertions.assertEquals("2026-03-12 17:30:15.123456", ((String[]) chunk.get(0))[0]);
    }

    private JDBCScanner createScanner(String driverClassName, String queryTimeZone, int fetchSize) {
        JDBCScanContext scanContext = new JDBCScanContext();
        scanContext.setDriverClassName(driverClassName);
        scanContext.setQueryTimeZone(queryTimeZone);
        scanContext.setStatementFetchSize(fetchSize);
        return new JDBCScanner("unused", scanContext);
    }

    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> iface, Invocation methodInvocation) {
        return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {iface}, (proxy, method, args) -> {
            if ("toString".equals(method.getName())) {
                return iface.getSimpleName() + "Proxy";
            }
            if ("hashCode".equals(method.getName())) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(method.getName())) {
                return args != null && args.length > 0 && proxy == args[0];
            }
            return methodInvocation.invoke(method, args);
        });
    }

    private Object defaultValue(Method method) {
        Class<?> returnType = method.getReturnType();
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0.0f;
        }
        if (returnType == double.class) {
            return 0.0d;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }

    private ResultSetMetaData singleColumnMetaData() {
        return proxy(ResultSetMetaData.class, (method, args) -> {
            if ("getColumnCount".equals(method.getName())) {
                return 1;
            }
            return defaultValue(method);
        });
    }

    private List<Object[]> singleStringColumnChunk() {
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new String[1]);
        return chunk;
    }

    private void setResultColumnClassNames(JDBCScanner scanner, List<String> classNames) throws Exception {
        setField(scanner, "resultColumnClassNames", classNames);
    }

    private ZoneId invokeResolveOracleSessionTimeZone(JDBCScanner scanner, String value) throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("resolveOracleSessionTimeZone", String.class);
        method.setAccessible(true);
        return (ZoneId) method.invoke(scanner, value);
    }

    private void invokeResolveOracleSessionTimeZoneRaw(JDBCScanner scanner, String value) throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("resolveOracleSessionTimeZone", String.class);
        method.setAccessible(true);
        method.invoke(scanner, value);
    }

    private void invokeInitOracleSessionTimeZoneIfNeeded(JDBCScanner scanner) throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("initOracleSessionTimeZoneIfNeeded");
        method.setAccessible(true);
        method.invoke(scanner);
    }

    private boolean invokeShouldConvertOracleTemporalStringColumn(JDBCScanner scanner, int columnIndex) throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("shouldConvertOracleTemporalStringColumn", int.class);
        method.setAccessible(true);
        return (boolean) method.invoke(scanner, columnIndex);
    }

    private String invokeConvertOracleTemporalValueToString(JDBCScanner scanner, int columnIndex, Object resultObject)
            throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod(
                "convertOracleTemporalValueToString", int.class, Object.class);
        method.setAccessible(true);
        return (String) method.invoke(scanner, columnIndex, resultObject);
    }

    private String invokeNormalizeTimestampStringToQueryTimeZone(JDBCScanner scanner, String value) throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("normalizeTimestampStringToQueryTimeZone", String.class);
        method.setAccessible(true);
        return (String) method.invoke(scanner, value);
    }

    private void setField(JDBCScanner scanner, String fieldName, Object value) throws Exception {
        Field field = JDBCScanner.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(scanner, value);
    }

    private Object getField(JDBCScanner scanner, String fieldName) throws Exception {
        Field field = JDBCScanner.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(scanner);
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(Method method, Object[] args) throws Exception;
    }
}
