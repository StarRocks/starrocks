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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class JDBCScannerTest {

    @Test
    public void testOpenInitializesPostgresTemporalStorageAndRetainsJdbcClasses() throws Exception {
        assertOpenTemporalColumns("org.postgresql.Driver", true);
    }

    @Test
    public void testOpenKeepsNonPostgresTemporalStorage() throws Exception {
        assertOpenTemporalColumns("com.mysql.cj.jdbc.Driver", false);
    }

    private void assertOpenTemporalColumns(String driverClassName, boolean postgres) throws Exception {
        List<String> typeNames = List.of("date", "timestamp", "int4");
        List<String> classNames = List.of("java.sql.Date", "java.sql.Timestamp", "java.lang.Integer");
        LocalDate date = LocalDate.of(1582, 10, 10);
        LocalDateTime timestamp = LocalDateTime.of(2026, 3, 8, 2, 30, 0, 123456000);
        AtomicInteger row = new AtomicInteger(-1);
        AtomicInteger typedReads = new AtomicInteger();
        List<String> closed = new ArrayList<>();
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (method, args) -> {
            switch (method.getName()) {
                case "getColumnCount":
                    return typeNames.size();
                case "getColumnTypeName":
                    return typeNames.get((int) args[0] - 1);
                case "getColumnClassName":
                    return classNames.get((int) args[0] - 1);
                default:
                    return defaultValue(method);
            }
        });
        ResultSet resultSet = proxy(ResultSet.class, (method, args) -> {
            switch (method.getName()) {
                case "getMetaData":
                    return metadata;
                case "next":
                    return row.incrementAndGet() < 2;
                case "getObject":
                    int column = (int) args[0];
                    if (column == 3) {
                        Assertions.assertEquals(1, args.length);
                        return row.get() + 7;
                    }
                    if (postgres) {
                        Assertions.assertEquals(2, args.length, "PG temporal reads must use java.time");
                        Assertions.assertEquals(column == 1 ? LocalDate.class : LocalDateTime.class, args[1]);
                        typedReads.incrementAndGet();
                        return row.get() == 1 ? null : (column == 1 ? date : timestamp);
                    }
                    Assertions.assertEquals(1, args.length, "Other drivers retain ordinary JDBC reads");
                    return row.get() == 1 ? null : (column == 1
                            ? java.sql.Date.valueOf(date) : Timestamp.valueOf(timestamp));
                case "close":
                    closed.add("resultSet");
                    return null;
                default:
                    return defaultValue(method);
            }
        });
        PreparedStatement statement = proxy(PreparedStatement.class, (method, args) -> {
            if ("executeQuery".equals(method.getName()) || "getResultSet".equals(method.getName())) {
                return resultSet;
            }
            if ("close".equals(method.getName())) {
                closed.add("statement");
            }
            return defaultValue(method);
        });
        Connection connection = proxy(Connection.class, (method, args) -> {
            if ("prepareStatement".equals(method.getName())) {
                return statement;
            }
            if ("close".equals(method.getName())) {
                closed.add("connection");
            }
            return defaultValue(method);
        });
        JDBCScanContext context = new JDBCScanContext();
        context.setDriverClassName(driverClassName);
        context.setJdbcURL("jdbc:test:" + UUID.randomUUID());
        context.setUser("test");
        context.setPassword("");
        context.setSql("SELECT d, ts, id FROM temporal_test");
        context.setQueryTimeZone("America/New_York");
        context.setStatementFetchSize(2);
        String cacheKey = context.getUser() + "/" + context.getPassword() + "/" + context.getJdbcURL();
        HikariDataSource source = new HikariDataSource() {
            @Override
            public Connection getConnection() {
                return connection;
            }
        };
        DataSourceCache cache = DataSourceCache.getInstance();
        cache.getSource(cacheKey, () -> new DataSourceCache.DataSourceCacheItem(source, getClass().getClassLoader()));
        JDBCScanner scanner = new JDBCScanner("unused", context);
        try {
            scanner.open();
            Assertions.assertEquals(classNames, scanner.getResultColumnClassNames(),
                    "BE type validation must still see the original JDBC classes");
            Assertions.assertTrue(scanner.hasNext());
            List<Object[]> chunk = scanner.getNextChunk();
            Assertions.assertEquals(2, scanner.getResultNumRows());
            if (postgres) {
                Assertions.assertInstanceOf(String[].class, chunk.get(0));
                Assertions.assertInstanceOf(String[].class, chunk.get(1));
                Assertions.assertEquals("1582-10-10", chunk.get(0)[0]);
                Assertions.assertEquals("2026-03-08 02:30:00.123456", chunk.get(1)[0]);
                Assertions.assertEquals(4, typedReads.get());
            } else {
                Assertions.assertInstanceOf(java.sql.Date[].class, chunk.get(0));
                Assertions.assertInstanceOf(Timestamp[].class, chunk.get(1));
                Assertions.assertEquals(java.sql.Date.valueOf(date), chunk.get(0)[0]);
                Assertions.assertEquals(Timestamp.valueOf(timestamp), chunk.get(1)[0]);
                Assertions.assertEquals(0, typedReads.get());
            }
            Assertions.assertNull(chunk.get(0)[1]);
            Assertions.assertNull(chunk.get(1)[1]);
            Assertions.assertArrayEquals(new Integer[] {7, 8}, chunk.get(2));
            Assertions.assertFalse(scanner.hasNext());
        } finally {
            try {
                scanner.close();
            } finally {
                Field sources = DataSourceCache.class.getDeclaredField("sources");
                sources.setAccessible(true);
                ((Map<?, ?>) sources.get(cache)).remove(cacheKey);
                source.close();
            }
        }
        Assertions.assertEquals(List.of("resultSet", "statement", "connection"), closed);
    }

    @Test
    public void testPostgresLocalTemporalReadPreservesWallClockAndFraction() throws Exception {
        for (String zone : List.of("UTC", "America/New_York", "Asia/Shanghai")) {
            JDBCScanner scanner = localTemporalScanner(LocalDateTime.class,
                    LocalDateTime.of(2026, 3, 8, 2, 30, 0, 123456000), zone);
            Assertions.assertEquals("2026-03-08 02:30:00.123456", scanner.getNextChunk().get(0)[0]);
            Assertions.assertEquals(1, scanner.getResultNumRows());
            scanner = localTemporalScanner(LocalDate.class, LocalDate.of(1582, 10, 10), zone);
            Assertions.assertEquals("1582-10-10", scanner.getNextChunk().get(0)[0]);
            scanner = localTemporalScanner(LocalDateTime.class, LocalDateTime.of(1, 1, 1, 0, 0), zone);
            Assertions.assertEquals("0001-01-01 00:00:00.000000", scanner.getNextChunk().get(0)[0]);
        }
    }

    @Test
    public void testPostgresLocalTemporalReadRejectsEraLossAndInfinity() throws Exception {
        for (LocalDate date : List.of(LocalDate.of(0, 1, 1), LocalDate.of(-1, 1, 1),
                LocalDate.of(10000, 1, 1), LocalDate.MIN, LocalDate.MAX)) {
            JDBCScanner dateScanner = localTemporalScanner(LocalDate.class, date, "UTC");
            SQLException failure = Assertions.assertThrows(SQLException.class, dateScanner::getNextChunk);
            Assertions.assertTrue(failure.getMessage().contains("outside the supported range"));
            JDBCScanner timestampScanner = localTemporalScanner(LocalDateTime.class, date.atStartOfDay(), "UTC");
            Assertions.assertThrows(SQLException.class, timestampScanner::getNextChunk);
        }
    }

    @Test
    public void testPostgresLocalTemporalReadPreservesNull() throws Exception {
        for (Class<?> type : List.of(LocalDate.class, LocalDateTime.class)) {
            JDBCScanner scanner = localTemporalScanner(type, null, "UTC");
            Assertions.assertNull(scanner.getNextChunk().get(0)[0]);
            Assertions.assertEquals(1, scanner.getResultNumRows());
        }
    }

    @Test
    public void testPostgresLocalTemporalReadIsRestrictedToUnzonedPostgresTypes() throws Exception {
        Method classify = JDBCScanner.class.getDeclaredMethod("getPostgresLocalTemporalClass", String.class);
        classify.setAccessible(true);
        JDBCScanner pgScanner = createScanner("org.postgresql.Driver", "UTC", 1);
        Assertions.assertEquals(LocalDate.class, classify.invoke(pgScanner, "date"));
        Assertions.assertEquals(LocalDateTime.class, classify.invoke(pgScanner, "TIMESTAMP"));
        Assertions.assertEquals(LocalDateTime.class, classify.invoke(pgScanner, "timestamp without time zone"));
        for (String typeName : List.of("timestamptz", "timestamp with time zone", "timetz", "time", "text")) {
            Assertions.assertNull(classify.invoke(pgScanner, typeName));
        }
        for (String driver : List.of("oracle.jdbc.OracleDriver", "com.mysql.cj.jdbc.Driver")) {
            JDBCScanner scanner = createScanner(driver, "UTC", 1);
            Assertions.assertNull(classify.invoke(scanner, "date"));
            Assertions.assertNull(classify.invoke(scanner, "timestamp"));
        }
    }

    private JDBCScanner localTemporalScanner(Class<?> type, Object value, String timeZone) throws Exception {
        JDBCScanner scanner = createScanner("org.postgresql.Driver", timeZone, 1);
        setField(scanner, "resultSetMetaData", singleColumnMetaData());
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new String[1]);
        setField(scanner, "resultChunk", chunk);
        setField(scanner, "postgresLocalTemporalColumns", List.of(type));
        setField(scanner, "resultSet", proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                Assertions.assertEquals(2, args.length, "Must bypass legacy Calendar-based getObject");
                Assertions.assertEquals(type, args[1]);
                return value;
            }
            return defaultValue(method);
        }));
        return scanner;
    }

    @Test
    public void testOracleVarcharColumnDoesNotUseTemporalConversion() throws Exception {
        JDBCScanner scanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        setResultColumnClassNames(scanner, List.of("java.lang.String"));

        Assertions.assertFalse(invokeShouldConvertOracleTemporalStringColumn(scanner, 0));
    }

    @Test
    public void testPostgresTimeWithTimezoneTypeNameDetected() throws Exception {
        JDBCScanner scanner = createScanner("org.postgresql.Driver", "Asia/Shanghai", 1);
        Assertions.assertTrue(invokeIsPostgresTimeWithTimezoneTypeName(scanner, "timetz"));
        Assertions.assertTrue(invokeIsPostgresTimeWithTimezoneTypeName(scanner, "time with time zone"));
        Assertions.assertFalse(invokeIsPostgresTimeWithTimezoneTypeName(scanner, "timestamptz"));
        Assertions.assertFalse(invokeIsPostgresTimeWithTimezoneTypeName(scanner, "timestamp with time zone"));
        Assertions.assertFalse(invokeIsPostgresTimeWithTimezoneTypeName(scanner, "TIMESTAMPTZ(6)"));
    }

    @Test
    public void testPostgresTemporalWithTimezoneDetectDoesNotAffectOtherDrivers() throws Exception {
        JDBCScanner oracleScanner = createScanner("oracle.jdbc.driver.OracleDriver", "Asia/Shanghai", 1);
        Assertions.assertFalse(invokeIsPostgresTimeWithTimezoneTypeName(oracleScanner, "timetz"));

        JDBCScanner mysqlScanner = createScanner("com.mysql.jdbc.Driver", "Asia/Shanghai", 1);
        Assertions.assertFalse(invokeIsPostgresTimeWithTimezoneTypeName(mysqlScanner, "timestamptz"));
    }

    @Test
    public void testPostgresTimestampWithTimezoneTypeNameDetected() throws Exception {
        JDBCScanner scanner = createScanner("org.postgresql.Driver", "Asia/Shanghai", 1);
        Assertions.assertTrue(invokeIsPostgresTimestampWithTimezoneTypeName(scanner, "timestamptz"));
        Assertions.assertTrue(invokeIsPostgresTimestampWithTimezoneTypeName(scanner, "timestamp with time zone"));
        Assertions.assertTrue(invokeIsPostgresTimestampWithTimezoneTypeName(scanner, "TIMESTAMPTZ(6)"));
        Assertions.assertFalse(invokeIsPostgresTimestampWithTimezoneTypeName(scanner, "timestamp"));
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

    @Test
    public void testGetNextChunkConvertsPostgresTimestampWithTimezoneByQueryTimeZone() throws Exception {
        JDBCScanner scanner = createScanner("org.postgresql.Driver", "Asia/Shanghai", 1);
        setField(scanner, "resultSetMetaData", singleColumnMetaData());
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new Timestamp[1]);
        setField(scanner, "resultChunk", chunk);
        setResultColumnClassNames(scanner, List.of("java.sql.Timestamp"));
        setField(scanner, "postgresTimestampWithTimezoneColumns", List.of(true));

        // Use epoch millis for 2026-03-12 09:30:15 UTC to make test timezone-independent
        long epochMillis = java.time.Instant.parse("2026-03-12T09:30:15Z").toEpochMilli();
        Timestamp sourceTimestamp = new Timestamp(epochMillis);
        ResultSet rs = proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                return sourceTimestamp;
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", rs);

        chunk = scanner.getNextChunk();
        // 2026-03-12 09:30:15 UTC -> 2026-03-12 17:30:15 Asia/Shanghai (+08:00)
        Assertions.assertEquals(Timestamp.valueOf("2026-03-12 17:30:15"), ((Timestamp[]) chunk.get(0))[0]);
    }

    @Test
    public void testGetNextChunkConvertsPostgresTimeWithTimezoneByQueryTimeZone() throws Exception {
        JDBCScanner scanner = createScanner("org.postgresql.Driver", "Asia/Shanghai", 1);
        setField(scanner, "resultSetMetaData", singleColumnMetaData());
        List<Object[]> chunk = new ArrayList<>();
        chunk.add(new Time[1]);
        setField(scanner, "resultChunk", chunk);
        setResultColumnClassNames(scanner, List.of("java.sql.Time"));
        setField(scanner, "postgresTimeWithTimezoneColumns", List.of(true));

        // Use epoch millis for 06:30:15.789 UTC to test sub-second precision preservation
        long epochMillis = java.time.Instant.parse("1970-01-01T06:30:15.789Z").toEpochMilli();
        Time sourceTime = new Time(epochMillis);
        ResultSet rs = proxy(ResultSet.class, (method, args) -> {
            if ("getObject".equals(method.getName())) {
                return sourceTime;
            }
            return defaultValue(method);
        });
        setField(scanner, "resultSet", rs);

        chunk = scanner.getNextChunk();
        // 06:30:15.789 UTC -> 14:30:15.789 Asia/Shanghai (+08:00)
        Time result = ((Time[]) chunk.get(0))[0];
        long expectedMillis = java.time.Instant.parse("1970-01-01T06:30:15.789Z").toEpochMilli()
                + java.util.TimeZone.getTimeZone("Asia/Shanghai").getOffset(epochMillis)
                - java.util.TimeZone.getDefault().getOffset(epochMillis);
        Assertions.assertEquals(expectedMillis, result.getTime());
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

    private boolean invokeIsPostgresTimeWithTimezoneTypeName(JDBCScanner scanner, String typeName)
            throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("isPostgresTimeWithTimezoneTypeName", String.class);
        method.setAccessible(true);
        return (boolean) method.invoke(scanner, typeName);
    }

    private boolean invokeIsPostgresTimestampWithTimezoneTypeName(JDBCScanner scanner, String typeName)
            throws Exception {
        Method method = JDBCScanner.class.getDeclaredMethod("isPostgresTimestampWithTimezoneTypeName", String.class);
        method.setAccessible(true);
        return (boolean) method.invoke(scanner, typeName);
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
