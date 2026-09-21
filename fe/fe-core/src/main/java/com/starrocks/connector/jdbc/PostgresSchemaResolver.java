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


package com.starrocks.connector.jdbc;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaConstants;
import com.starrocks.type.ArrayType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static java.lang.Math.max;

public class PostgresSchemaResolver extends JDBCSchemaResolver {

    private static final Logger LOG = LogManager.getLogger(PostgresSchemaResolver.class);

    public PostgresSchemaResolver() {
        this.defaultTableTypes = new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null, defaultTableTypes);
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, tblName, defaultTableTypes);
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet, Map<String, Integer> originalJdbcTypes,
                                         Map<String, String> originalJdbcTypeNames,
                                         Set<String> unboundedNumericColumns) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            int dataType = columnSet.getInt("DATA_TYPE");
            String columnName = columnSet.getString("COLUMN_NAME");
            String typeName = columnSet.getString("TYPE_NAME");
            if (unboundedNumericColumns != null && isUnboundedNumeric(dataType, columnSet.getInt("COLUMN_SIZE"))) {
                unboundedNumericColumns.add(normalizeColumnName(columnName));
            }
            Type type = convertColumnType(dataType,
                    typeName,
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            if (originalJdbcTypes != null) {
                originalJdbcTypes.put(columnName.toLowerCase(java.util.Locale.ROOT), dataType);
            }
            if (originalJdbcTypeNames != null && typeName != null) {
                originalJdbcTypeNames.put(normalizeColumnName(columnName), typeName);
            }

            String comment = "";
            // Add try-cache to prevent exceptions when the metadata of some databases does not contain REMARKS
            try {
                if (columnSet.getString("REMARKS") != null) {
                    comment = columnSet.getString("REMARKS");
                }
            } catch (SQLException ignored) { }

            columnName = normalizeColumnName(columnSet.getString("COLUMN_NAME"));
            fullSchema.add(new Column(columnName, type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES), comment));
        }
        return fullSchema;
    }

    @Override
    public List<Column> convertToSRTable(ResultSetMetaData metaData, Map<String, Integer> originalJdbcTypes,
                                         Map<String, String> originalJdbcTypeNames,
                                         Set<String> unboundedNumericColumns) throws SQLException {
        List<Column> columns = super.convertToSRTable(metaData, originalJdbcTypes, originalJdbcTypeNames, null);
        if (unboundedNumericColumns != null) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (isUnboundedNumeric(metaData.getColumnType(i), metaData.getPrecision(i))) {
                    unboundedNumericColumns.add(columns.get(i - 1).getName());
                }
            }
        }
        return columns;
    }

    private static boolean isUnboundedNumeric(int dataType, int precision) {
        return (dataType == Types.NUMERIC || dataType == Types.DECIMAL) && precision == 0;
    }

    @Override
    protected String normalizeColumnName(String columnName) {
        if (!columnName.equals(columnName.toLowerCase())) {
            return "\"" + columnName + "\"";
        }
        return columnName;
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                          Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.ARRAY:
                // PostgreSQL reports built-in array names with an underscore prefix.
                // Dimensions and lower bounds belong to individual values, not the column type.
                if ("_text".equalsIgnoreCase(typeName) || "_varchar".equalsIgnoreCase(typeName)) {
                    return new ArrayType(TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength()));
                }
                return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.REAL:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
                return TypeFactory.createCharType(columnSize);
            case Types.VARCHAR:
                if ("varchar".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(columnSize);
                } else if ("text".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                primitiveType = PrimitiveType.TIME;
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.OTHER:
                if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.JSON;
                    break;
                } else if ("uuid".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarbinary(columnSize);
                } else if ("time".equalsIgnoreCase(typeName)
                        || "time without time zone".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimeWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimestampWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.DATETIME;
                    break;
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if ("uuid".equalsIgnoreCase(typeName)) {
            return TypeFactory.createVarbinary(columnSize);
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return TypeFactory.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            // Unconstrained PostgreSQL numeric has no column-wide precision or scale.
            // The JDBC reader checks this bounded mapping without rounding each selected value.
            if (columnSize == 0) {
                return TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18);
            }
            return TypeFactory.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    @Override
    public long getTableRowCount(Connection connection, String dbName, String tableName) throws SQLException {
        return readRowCount(connection, dbName, tableName).orElse(-1L);
    }

    @Override
    public Optional<JdbcTableStats> getTableStatistics(Connection connection, String dbName, String tableName)
            throws SQLException {
        OptionalLong rowCount = readRowCount(connection, dbName, tableName);
        if (rowCount.isEmpty()) {
            return Optional.empty();
        }
        long rows = rowCount.getAsLong();
        if (rows <= 0) {
            // Nothing to scale the per-column ratios by, and pg_stats for an empty table carries
            // no usable numbers either. Skip the second round trip.
            return Optional.of(JdbcTableStats.ofRowCount(rows));
        }
        return Optional.of(new JdbcTableStats(OptionalLong.of(rows),
                readColumnStats(connection, dbName, tableName, rows)));
    }

    // ---------------------------------------------------------------------
    // Row count
    // ---------------------------------------------------------------------

    /**
     * PostgreSQL keeps no single trustworthy row count, so this walks a fallback chain. Every step
     * is a catalog lookup measured in tenths of a millisecond and independent of table size.
     *
     * <ol>
     *   <li>{@code pg_class.reltuples} — maintained by ANALYZE and autovacuum. No row at all means
     *       the table does not exist: give up, there is nothing better to read. A positive value is
     *       authoritative, including on a partition parent somebody ran ANALYZE on, where
     *       PostgreSQL stores the whole inheritance tree's total.</li>
     *   <li>Otherwise, on a declarative partition parent reltuples is meaningless: the parent holds
     *       no rows, and autovacuum analyzes the children but never the parent, so it sits at -1
     *       forever while the children carry perfectly good counts. Sum the children, choosing
     *       per child between its reltuples and its live-tuple counter — a parent normally has
     *       both analyzed and never-analyzed partitions at once, and one choice for the whole
     *       sum cannot serve both.</li>
     *   <li>Otherwise reltuples says nothing usable -- 0 is ambiguous (a genuinely empty table and
     *       a freshly created one look alike) and -1 says the table has never been analyzed -- so
     *       ask the statistics collector's live-tuple counter instead. That counter is maintained
     *       on every insert and delete rather than by ANALYZE, so it is populated for exactly the
     *       tables reltuples is not. It can drift (a crash or a statistics reset loses it), which
     *       is why it is consulted only after reltuples and only believed when positive.</li>
     * </ol>
     *
     * <p>Since PostgreSQL 14 a never-analyzed table reports -1 rather than 0, so a sum over
     * children can be negative but <em>not</em> -1 — two unanalyzed partitions sum to -2. The
     * per-child CASE keeps any single -1 out of the sum, but the {@code >= 0} guard stays for the
     * case where every child is both unanalyzed and unknown to the statistics collector, and the
     * final test is {@code < 0} rather than {@code == -1}. Testing for -1 alone would take -2 for
     * a real row count and report a 200-million-row table as smaller than a dictionary.
     *
     * @return the row count, or empty when PostgreSQL has none to give
     */
    private OptionalLong readRowCount(Connection connection, String schemaName, String tableName)
            throws SQLException {
        // (1) pg_class.reltuples. The cast to bigint avoids handing a float back to Java.
        OptionalLong relTuples = queryLong(connection,
                "SELECT c.reltuples::bigint FROM pg_class c " +
                        "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                        "WHERE n.nspname = ? AND c.relname = ?",
                schemaName, tableName);
        if (relTuples.isEmpty()) {
            return OptionalLong.empty();
        }
        long rows = relTuples.getAsLong();
        if (rows > 0) {
            return OptionalLong.of(rows);
        }

        // (2) Partition parent: sum over the children, deciding per child rather than per sum.
        // A partitioned table is analyzed one partition at a time, so a parent usually has a mix
        // of analyzed and never-analyzed children. Summing reltuples straight adds -1 for each
        // unanalyzed one, which is wrong twice over: it subtracts a row that does not exist, and
        // it silently contributes nothing for a partition that may hold millions. Measured on a
        // four-partition table with two analyzed at 5,000 rows each and two never analyzed,
        // also holding 5,000 each: the straight sum gives 9,998 against a true 20,000. Asking
        // each child separately -- reltuples when it is a count, the statistics collector's
        // n_live_tup when it is not -- gives 20,000 exactly. The LEFT JOIN keeps a child that
        // has no collector row at all contributing its own reltuples rather than dropping it
        // from the sum entirely.
        if (isPartitionedTable(connection, schemaName, tableName)) {
            OptionalLong childRows = queryLong(connection,
                    "SELECT SUM(CASE WHEN child.reltuples >= 0 THEN child.reltuples " +
                            "ELSE COALESCE(stat.n_live_tup, 0) END)::bigint FROM pg_inherits " +
                            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
                            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                            "JOIN pg_namespace n ON parent.relnamespace = n.oid " +
                            "LEFT JOIN pg_stat_all_tables stat ON stat.relid = child.oid " +
                            "WHERE n.nspname = ? AND parent.relname = ?",
                    schemaName, tableName);
            if (childRows.isPresent() && childRows.getAsLong() >= 0) {
                rows = childRows.getAsLong();
            }
        } else {
            // (3) reltuples is 0 or -1: neither is a row count, so ask the statistics collector.
            // The never-analyzed case (-1) is the one this matters most for: pg_stats may already
            // hold column statistics for such a table, since autovacuum's ANALYZE and reltuples are
            // maintained by different machinery, and giving up on the row count throws those away
            // too -- getTableStatistics stops before it reads pg_stats. Trino reads the same
            // counter in the same place.
            OptionalLong liveTuples = queryLong(connection,
                    "SELECT n_live_tup FROM pg_stat_all_tables WHERE schemaname = ? AND relname = ?",
                    schemaName, tableName);
            if (liveTuples.isPresent() && liveTuples.getAsLong() > 0) {
                rows = liveTuples.getAsLong();
            }
        }

        // Never analyzed (-1), or a sum over never-analyzed children (-N). Not a row count.
        return rows < 0 ? OptionalLong.empty() : OptionalLong.of(rows);
    }

    private boolean isPartitionedTable(Connection connection, String schemaName, String tableName)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT true FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid " +
                        "WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'p'")) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private OptionalLong queryLong(Connection connection, String sql, String... params) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long value = rs.getLong(1);
                    return rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(value);
                }
            }
        }
        return OptionalLong.empty();
    }

    // ---------------------------------------------------------------------
    // Column statistics
    // ---------------------------------------------------------------------

    /**
     * Read per-column statistics from {@code pg_stats}.
     *
     * <p>Two things about this view are worth knowing. It is defined with a
     * {@code row_security_active()} filter, so a table with row-level security returns no rows here
     * at all even though its reltuples reads fine — the result is simply an empty map, which the
     * caller maps to unknown column statistics. And it reports {@code n_distinct} as an absolute
     * count when non-negative but as a negative <em>ratio</em> of the row count when PostgreSQL
     * judged distinctness to scale with table size; the conversion to an absolute count happens
     * here, so nothing above this class has to know the convention.
     */
    private Map<String, JdbcColumnStats> readColumnStats(Connection connection, String schemaName,
                                                         String tableName, long rowCount) {
        Map<String, JdbcColumnStats> result = new HashMap<>();
        String sql = "SELECT attname, null_frac, n_distinct, avg_width " +
                "FROM pg_stats WHERE schemaname = ? AND tablename = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString(1);
                    if (columnName == null) {
                        continue;
                    }
                    double nullFraction = rs.getDouble(2);
                    OptionalDouble nulls = rs.wasNull() ? OptionalDouble.empty() : OptionalDouble.of(nullFraction);

                    double nDistinct = rs.getDouble(3);
                    OptionalDouble distinct = rs.wasNull() ? OptionalDouble.empty()
                            : toAbsoluteDistinctCount(nDistinct, rowCount);

                    int avgWidth = rs.getInt(4);
                    OptionalInt width = rs.wasNull() || avgWidth <= 0 ? OptionalInt.empty() : OptionalInt.of(avgWidth);

                    // pg_stats reports attname exactly as the column was created, but the schema
                    // StarRocks captured ran every name through normalizeColumnName -- which, for
                    // this dialect, wraps a name that is not already lower case in literal double
                    // quotes so the generated SQL can address it. The lookup on the other side is
                    // by Column#getName, i.e. the normalized form, and the difference is the quote
                    // characters, not the case, so JdbcTableStats' case-insensitive map cannot
                    // bridge it: every mixed-case column came back unknown. Normalize on the way
                    // in. A name that is already lower case is returned unchanged.
                    result.put(normalizeColumnName(columnName), new JdbcColumnStats(nulls, distinct, width));
                }
            }
        } catch (Exception e) {
            // A table whose row count we already have is still worth reporting: losing the column
            // statistics degrades the plan, losing the row count would reintroduce the bug this
            // whole path exists to fix.
            LOG.warn("Failed to read pg_stats for {}.{}: {}", schemaName, tableName, e.getMessage());
            return Map.of();
        }
        return result;
    }

    /**
     * {@code n_distinct >= 0} is already an absolute count. A negative value is minus the fraction
     * of rows that are distinct, so -1 means "unique" and -0.5 means "two rows per value".
     */
    private static OptionalDouble toAbsoluteDistinctCount(double nDistinct, long rowCount) {
        if (nDistinct == 0) {
            // PostgreSQL's "unknown" marker for this column.
            return OptionalDouble.empty();
        }
        double absolute = nDistinct > 0 ? nDistinct : -nDistinct * rowCount;
        return OptionalDouble.of(Math.max(1.0, Math.min(absolute, rowCount)));
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), System.currentTimeMillis()));
    }

    private static boolean isTimeWithTimezoneTypeName(String typeName) {
        return "timetz".equalsIgnoreCase(typeName) || "time with time zone".equalsIgnoreCase(typeName);
    }

    private static boolean isTimestampWithTimezoneTypeName(String typeName) {
        return "timestamptz".equalsIgnoreCase(typeName) || "timestamp with time zone".equalsIgnoreCase(typeName);
    }

}
