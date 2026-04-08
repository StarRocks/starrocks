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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public class JDBCScanner {
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private String driverLocation;
    private HikariDataSource dataSource;
    private JDBCScanContext scanContext;
    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private ResultSetMetaData resultSetMetaData;
    private List<String> resultColumnClassNames;
    private List<Boolean> postgresTimeWithTimezoneColumns;
    private List<Boolean> postgresTimestampWithTimezoneColumns;
    private List<Object[]> resultChunk;
    private int resultNumRows = 0;
    private final boolean isOracleDriver;
    private final boolean isPostgresDriver;
    private final ZoneId queryTimeZone;
    private ZoneId oracleSessionTimeZone;
    ClassLoader classLoader;

    public JDBCScanner(String driverLocation, JDBCScanContext scanContext) {
        this.driverLocation = driverLocation;
        this.scanContext = scanContext;
        this.isOracleDriver = scanContext.getDriverClassName().toLowerCase(Locale.ROOT).contains("oracle");
        this.isPostgresDriver = scanContext.getDriverClassName().toLowerCase(Locale.ROOT).contains("postgresql");
        this.queryTimeZone = resolveQueryTimeZone(scanContext.getQueryTimeZone());
    }

    public void open() throws Exception {
        String cacheKey = computeCacheKey(scanContext.getUser(), scanContext.getPassword(), scanContext.getJdbcURL());
        URL driverURL = new File(driverLocation).toURI().toURL();
        DataSourceCache.DataSourceCacheItem cacheItem = DataSourceCache.getInstance().getSource(cacheKey, () -> {
            ClassLoader classLoader = URLClassLoader.newInstance(new URL[] {driverURL});
            Thread.currentThread().setContextClassLoader(classLoader);
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(scanContext.getDriverClassName());
            config.setJdbcUrl(scanContext.getJdbcURL());
            config.setUsername(scanContext.getUser());
            config.setPassword(scanContext.getPassword());
            config.setMaximumPoolSize(scanContext.getConnectionPoolSize());
            config.setMinimumIdle(scanContext.getMinimumIdleConnections());
            config.setIdleTimeout(scanContext.getConnectionIdleTimeoutMs());
            config.setConnectionTimeout(scanContext.getConnectionTimeoutMs());
            // Connection lifecycle values (maxLifetime, keepaliveTime) are pre-validated by
            // BE's jdbc_scanner.cpp before being passed via JNI. The BE enforces:
            //   maxLifetime >= 30000 (or defaults to 300000)
            //   keepaliveTime == 0 (disabled) or (>= 30000 and < maxLifetime)
            // No additional validation needed here.
            config.setMaxLifetime(scanContext.getConnectionMaxLifetimeMs());
            config.setKeepaliveTime(scanContext.getConnectionKeepaliveTimeMs());
            HikariDataSource hikariDataSource = new HikariDataSource(config);
            // hikari doesn't support user-provided class loader, we should save them ourselves to ensure that
            // the classes of result data are loaded by the same class loader, otherwise we may encounter
            // ArrayStoreException in getNextChunk
            return new DataSourceCache.DataSourceCacheItem(hikariDataSource, classLoader);
        });
        dataSource = cacheItem.getHikariDataSource();
        classLoader = cacheItem.getClassLoader();

        connection = dataSource.getConnection();
        initOracleSessionTimeZoneIfNeeded();
        connection.setAutoCommit(false);
        statement = connection.prepareStatement(scanContext.getSql(), ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        if (scanContext.getDriverClassName().toLowerCase(Locale.ROOT).contains("mysql")) {
            statement.setFetchSize(Integer.MIN_VALUE);
        } else {
            statement.setFetchSize(scanContext.getStatementFetchSize());
        }
        statement.executeQuery();
        resultSet = statement.getResultSet();
        resultSetMetaData = resultSet.getMetaData();
        resultColumnClassNames = new ArrayList<>(resultSetMetaData.getColumnCount());
        postgresTimeWithTimezoneColumns = new ArrayList<>(resultSetMetaData.getColumnCount());
        postgresTimestampWithTimezoneColumns = new ArrayList<>(resultSetMetaData.getColumnCount());
        resultChunk = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String typeName = resultSetMetaData.getColumnTypeName(i);
            String className = resultSetMetaData.getColumnClassName(i);
            boolean isPostgresTimeWithTimezone = isPostgresTimeWithTimezoneTypeName(typeName);
            boolean isPostgresTimestampWithTimezone = isPostgresTimestampWithTimezoneTypeName(typeName);
            postgresTimeWithTimezoneColumns.add(isPostgresTimeWithTimezone);
            postgresTimestampWithTimezoneColumns.add(isPostgresTimestampWithTimezone);
            // Keep the original className for type checking (getResultColumnClassNames),
            // but use the appropriate array type for data storage.
            resultColumnClassNames.add(className);
            String arrayClassName = className;
            if (isPostgresTimestampWithTimezone) {
                arrayClassName = Timestamp.class.getName();
            } else if (isPostgresTimeWithTimezone) {
                arrayClassName = Time.class.getName();
            }
            if (arrayClassName.equals("byte[]") || arrayClassName.equals("[B")) {
                resultChunk.add((Object[]) Array.newInstance(byte[].class, scanContext.getStatementFetchSize()));
                continue;
            }
            Class<?> clazz = classLoader.loadClass(arrayClassName);
            if (isGeneralJDBCClassType(clazz)) {
                resultChunk.add((Object[]) Array.newInstance(clazz, scanContext.getStatementFetchSize()));
            } else if (null != mapEngineSpecificClassType(clazz)) {
                Class targetClass = mapEngineSpecificClassType(clazz);
                resultChunk.add((Object[]) Array.newInstance(targetClass, scanContext.getStatementFetchSize()));
            } else {
                resultChunk.add((Object[]) Array.newInstance(String.class, scanContext.getStatementFetchSize()));
            }
        }
    }

    private static String computeCacheKey(String username, String password, String jdbcUrl) {
        return username + "/" + password + "/" + jdbcUrl;
    }

    private void initOracleSessionTimeZoneIfNeeded() throws Exception {
        if (!isOracleDriver) {
            return;
        }
        try (PreparedStatement getSessionTimeZoneStmt = connection.prepareStatement("SELECT SESSIONTIMEZONE FROM DUAL");
                ResultSet rs = getSessionTimeZoneStmt.executeQuery()) {
            if (rs.next()) {
                oracleSessionTimeZone = resolveOracleSessionTimeZone(rs.getString(1));
            }
        }
    }

    private static final Set<Class<?>> GENERAL_JDBC_CLASS_SET = new HashSet<>(
            Arrays.asList(Boolean.class, Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
                    BigInteger.class, BigDecimal.class, java.sql.Date.class, Timestamp.class, LocalDate.class,
                    LocalDateTime.class, Time.class, String.class, UUID.class));

    private boolean isGeneralJDBCClassType(Class<?> clazz) {
        return GENERAL_JDBC_CLASS_SET.contains(clazz);
    }

    private static final Set<String> POSTGRES_TIME_WITH_TIMEZONE_TYPE_NAMES = new HashSet<>(Arrays.asList(
            "timetz",
            "time with time zone"));

    private static final Set<String> POSTGRES_TIMESTAMP_WITH_TIMEZONE_TYPE_NAMES = new HashSet<>(Arrays.asList(
            "timestamptz",
            "timestamp with time zone"));

    private String normalizeTypeName(String typeName) {
        if (typeName == null) {
            return "";
        }
        String normalizedTypeName = typeName.toLowerCase(Locale.ROOT).trim();
        int precisionStartIndex = normalizedTypeName.indexOf('(');
        if (precisionStartIndex > 0) {
            normalizedTypeName = normalizedTypeName.substring(0, precisionStartIndex).trim();
        }
        return normalizedTypeName;
    }

    private boolean isPostgresTimeWithTimezoneTypeName(String typeName) {
        if (!isPostgresDriver || typeName == null) {
            return false;
        }
        return POSTGRES_TIME_WITH_TIMEZONE_TYPE_NAMES.contains(normalizeTypeName(typeName));
    }

    private boolean isPostgresTimestampWithTimezoneTypeName(String typeName) {
        if (!isPostgresDriver || typeName == null) {
            return false;
        }
        return POSTGRES_TIMESTAMP_WITH_TIMEZONE_TYPE_NAMES.contains(normalizeTypeName(typeName));
    }

    private boolean shouldConvertPostgresTimestampWithTimezoneColumn(int columnIndex) {
        if (!isPostgresDriver || queryTimeZone == null || postgresTimestampWithTimezoneColumns == null) {
            return false;
        }
        return columnIndex >= 0 && columnIndex < postgresTimestampWithTimezoneColumns.size()
                && postgresTimestampWithTimezoneColumns.get(columnIndex);
    }

    private boolean shouldConvertPostgresTimeWithTimezoneColumn(int columnIndex) {
        if (!isPostgresDriver || postgresTimeWithTimezoneColumns == null) {
            return false;
        }
        return columnIndex >= 0 && columnIndex < postgresTimeWithTimezoneColumns.size()
                && postgresTimeWithTimezoneColumns.get(columnIndex);
    }

    private static final Map<String, Class> ENGINE_SPECIFIC_CLASS_MAPPING = new HashMap<String, Class>() {{
            put("com.clickhouse.data.value.UnsignedByte", Short.class);
            put("com.clickhouse.data.value.UnsignedShort", Integer.class);
            put("com.clickhouse.data.value.UnsignedInteger", Long.class);
            put("com.clickhouse.data.value.UnsignedLong", BigInteger.class);
            put("oracle.jdbc.OracleBlob", Blob.class);
        }};

    private Class mapEngineSpecificClassType(Class<?> clazz) {
        String className = clazz.getName();
        return ENGINE_SPECIFIC_CLASS_MAPPING.get(className);
    }

    // used for cpp interface
    public List<String> getResultColumnClassNames() {
        return resultColumnClassNames;
    }

    public boolean hasNext() throws Exception {
        return resultSet.next();
    }

    // return columnar chunk
    public List<Object[]> getNextChunk() throws Exception {
        int chunkSize = scanContext.getStatementFetchSize();
        int columnCount = resultSetMetaData.getColumnCount();
        resultNumRows = 0;
        do {
            for (int i = 0; i < columnCount; i++) {
                Object[] dataColumn = resultChunk.get(i);
                Object resultObject = resultSet.getObject(i + 1);
                // in some cases, the real java class type of result is not consistent with the type from
                // resultSetMetadata,
                // for example,FLOAT type in oracle gives java.lang.Double type in resultSetMetaData,
                // but the result type is BigDecimal when we getObject from resultSet.
                // So we choose to convert the value to the target type here.
                if (resultObject == null) {
                    dataColumn[resultNumRows] = null;
                } else if (dataColumn instanceof Short[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).shortValue();
                } else if (dataColumn instanceof Integer[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).intValue();
                } else if (dataColumn instanceof Long[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).longValue();
                } else if (dataColumn instanceof Float[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).floatValue();
                } else if (dataColumn instanceof Double[]) {
                    dataColumn[resultNumRows] = ((Number) resultObject).doubleValue();
                } else if (dataColumn instanceof Time[]) {
                    if (shouldConvertPostgresTimeWithTimezoneColumn(i)) {
                        Time timeValue = resultObject instanceof Time ? (Time) resultObject : resultSet.getTime(i + 1);
                        dataColumn[resultNumRows] = convertPostgresTimeWithTimezoneValue(timeValue);
                    } else {
                        dataColumn[resultNumRows] = resultObject;
                    }
                } else if (dataColumn instanceof Timestamp[]) {
                    if (shouldConvertPostgresTimestampWithTimezoneColumn(i)) {
                        Timestamp timestampValue =
                                resultObject instanceof Timestamp ? (Timestamp) resultObject : resultSet.getTimestamp(i + 1);
                        dataColumn[resultNumRows] = convertPostgresTimestampWithTimezoneValue(timestampValue);
                    } else {
                        dataColumn[resultNumRows] = resultObject;
                    }
                } else if (resultObject instanceof byte[]) {
                    dataColumn[resultNumRows] = resultObject;
                } else if (resultObject instanceof Blob) {
                    dataColumn[resultNumRows] = resultObject;
                } else if (dataColumn instanceof String[]) {
                    if (shouldConvertOracleTemporalStringColumn(i)) {
                        dataColumn[resultNumRows] = convertOracleTemporalValueToString(i, resultObject);
                    } else if (resultObject instanceof String) {
                        // if both sides are String, assign value directly to avoid additional calls to getString
                        dataColumn[resultNumRows] = resultObject;
                    } else {
                        dataColumn[resultNumRows] = resultSet.getString(i + 1);
                    }
                } else {
                    if (dataColumn instanceof BigInteger[] && resultObject instanceof Number) {
                        dataColumn[resultNumRows] = new BigInteger(resultObject.toString());
                    } else {
                        // for other general class type, assign value directly
                        dataColumn[resultNumRows] = resultObject;
                    }
                }
            }
            resultNumRows++;
        } while (resultNumRows < chunkSize && resultSet.next());
        return resultChunk;
    }

    public int getResultNumRows() {
        return resultNumRows;
    }

    public void close() throws Exception {
        if (resultSet != null) {
            resultSet.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private static final DateTimeFormatter OFFSET_TS_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendLiteral(' ')
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter();

    private static final DateTimeFormatter ZONE_TS_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendLiteral(' ')
            .appendZoneId()
            .toFormatter();

    private ZoneId resolveQueryTimeZone(String queryTimeZoneValue) {
        if ((!isOracleDriver && !isPostgresDriver) || queryTimeZoneValue == null || queryTimeZoneValue.isEmpty()) {
            return null;
        }
        try {
            return ZoneId.of(queryTimeZoneValue);
        } catch (DateTimeException ignored) {
            try {
                return ZoneId.of(queryTimeZoneValue, ZoneId.SHORT_IDS);
            } catch (DateTimeException ex) {
                throw new IllegalArgumentException("invalid query time zone: " + queryTimeZoneValue, ex);
            }
        }
    }

    private ZoneId resolveOracleSessionTimeZone(String sessionTimeZoneValue) {
        if (sessionTimeZoneValue == null || sessionTimeZoneValue.isEmpty()) {
            return null;
        }
        String normalized = sessionTimeZoneValue.trim();
        try {
            if (normalized.startsWith("+") || normalized.startsWith("-")) {
                return ZoneOffset.of(normalized);
            }
            if (normalized.matches("^\\d{2}:\\d{2}$")) {
                return ZoneOffset.of("+" + normalized);
            }
            return ZoneId.of(normalized);
        } catch (DateTimeException ignored) {
            try {
                return ZoneId.of(normalized, ZoneId.SHORT_IDS);
            } catch (DateTimeException ex) {
                throw new IllegalArgumentException("invalid oracle session time zone: " + sessionTimeZoneValue, ex);
            }
        }
    }

    private long computeTimezoneOffsetMillis(long sourceMillis) {
        // Build the query-zone local datetime from sourceMillis, then resolve it in JVM default timezone.
        // This matches Timestamp.valueOf(LocalDateTime) behavior, including DST gap/overlap resolution.
        Calendar queryCalendar = Calendar.getInstance(TimeZone.getTimeZone(queryTimeZone));
        queryCalendar.setTimeInMillis(sourceMillis);

        Calendar defaultCalendar = Calendar.getInstance(TimeZone.getDefault());
        defaultCalendar.clear();
        defaultCalendar.setLenient(true);
        defaultCalendar.set(Calendar.ERA, queryCalendar.get(Calendar.ERA));
        defaultCalendar.set(queryCalendar.get(Calendar.YEAR),
                queryCalendar.get(Calendar.MONTH),
                queryCalendar.get(Calendar.DAY_OF_MONTH),
                queryCalendar.get(Calendar.HOUR_OF_DAY),
                queryCalendar.get(Calendar.MINUTE),
                queryCalendar.get(Calendar.SECOND));
        defaultCalendar.set(Calendar.MILLISECOND, queryCalendar.get(Calendar.MILLISECOND));
        return defaultCalendar.getTimeInMillis() - sourceMillis;
    }

    private Time convertPostgresTimeWithTimezoneValue(Time sourceValue) {
        if (sourceValue == null || queryTimeZone == null) {
            return sourceValue;
        }
        // java.sql.Time.toInstant() throws UnsupportedOperationException,
        // so convert via epoch millis manually.
        // Avoid Time.valueOf(LocalTime) as it drops sub-second precision.
        long sourceMillis = sourceValue.getTime();
        long offsetMillis = TimeZone.getTimeZone(queryTimeZone).getOffset(sourceMillis)
                - TimeZone.getDefault().getOffset(sourceMillis);
        return new Time(sourceMillis + offsetMillis);
    }

    private Timestamp convertPostgresTimestampWithTimezoneValue(Timestamp sourceValue) {
        if (sourceValue == null || queryTimeZone == null) {
            return sourceValue;
        }
        // Timestamp.toInstant() returns the absolute instant.
        // Convert directly to the target timezone without relying on JVM/PG session timezone.
        LocalDateTime converted = sourceValue.toInstant().atZone(queryTimeZone).toLocalDateTime();
        return Timestamp.valueOf(converted);
    }

    private boolean isOracleTimestampWithLocalTimeZoneClass(String className) {
        return "oracle.sql.TIMESTAMPLTZ".equals(className);
    }

    private boolean isOracleTimestampWithTimeZoneClass(String className) {
        return "oracle.sql.TIMESTAMPTZ".equals(className);
    }

    private boolean isOracleTemporalClass(String className) {
        return "oracle.sql.TIMESTAMP".equals(className) ||
                isOracleTimestampWithLocalTimeZoneClass(className) ||
                isOracleTimestampWithTimeZoneClass(className);
    }

    private boolean shouldConvertOracleTemporalStringColumn(int columnIndex) {
        return isOracleDriver && queryTimeZone != null && isOracleTemporalClass(resultColumnClassNames.get(columnIndex));
    }

    private String convertOracleTemporalValueToString(int columnIndex, Object resultObject) throws Exception {
        String className = resultColumnClassNames.get(columnIndex);
        int jdbcColumnIndex = columnIndex + 1;

        if (isOracleTimestampWithLocalTimeZoneClass(className) && queryTimeZone != null) {
            Timestamp ts = resultSet.getTimestamp(jdbcColumnIndex);
            if (ts == null) {
                return null;
            }
            ZoneId sourceTimeZone = oracleSessionTimeZone != null ? oracleSessionTimeZone : ZoneId.systemDefault();
            LocalDateTime converted = ts.toLocalDateTime().atZone(sourceTimeZone)
                    .withZoneSameInstant(queryTimeZone).toLocalDateTime();
            return converted.format(DATETIME_FORMATTER);
        }

        String value;
        if (resultObject instanceof String) {
            value = (String) resultObject;
        } else {
            value = resultSet.getString(jdbcColumnIndex);
        }
        if (!isOracleTimestampWithTimeZoneClass(className)) {
            return value;
        }
        return normalizeTimestampStringToQueryTimeZone(value);
    }

    private String normalizeTimestampStringToQueryTimeZone(String value) {
        if (value == null || queryTimeZone == null) {
            return value;
        }

        // Try offset format first: 2026-03-12 09:30:15.123456 +08:00
        try {
            OffsetDateTime odt = OffsetDateTime.parse(value, OFFSET_TS_FORMATTER);
            return odt.atZoneSameInstant(queryTimeZone).toLocalDateTime().format(DATETIME_FORMATTER);
        } catch (DateTimeParseException ignored) {
        }

        // Try zone format: 2026-03-12 09:30:15.123456 Asia/Shanghai
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(value, ZONE_TS_FORMATTER);
            return zdt.withZoneSameInstant(queryTimeZone).toLocalDateTime().format(DATETIME_FORMATTER);
        } catch (DateTimeParseException ignored) {
        }

        // Fallback to original if not parseable
        return value;
    }
}
