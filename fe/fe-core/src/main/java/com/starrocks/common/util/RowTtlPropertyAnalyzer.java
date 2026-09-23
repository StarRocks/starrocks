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

package com.starrocks.common.util;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Validation of the three row TTL table properties.
 *
 * <p>Row TTL names a moment at which a row expires, how often the table is examined for expired
 * rows, and the time zone the expiration moment is computed in. This class decides whether a given
 * set of property values may be stored on a given table; storing them, and everything that happens
 * afterwards, belongs to the callers.
 *
 * <p>Values are never rewritten here. Whatever the user wrote is what gets stored, so that the DDL
 * printed back by SHOW CREATE TABLE can be replayed to build the same table.
 */
public class RowTtlPropertyAnalyzer {
    private static final Logger LOG = LogManager.getLogger(RowTtlPropertyAnalyzer.class);

    /**
     * Units an expiration expression may add to the time column. Narrower than what the parser
     * accepts: MILLISECOND and MICROSECOND parse, but sub-second precision is meaningless for a
     * retention period, so they are rejected rather than silently honoured.
     */
    private static final Set<String> INTERVAL_UNITS = ImmutableSet.of(
            "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND");

    /**
     * Scales to_datetime accepts, meaning seconds, milliseconds and microseconds. Anything else has
     * to be rejected here because the function gives no usable signal at run time: a positive value
     * such as 2 makes it return NULL, so the table looks configured but never expires a row, while
     * a negative value is silently taken as 0 and the column is read as whole seconds.
     */
    private static final Set<Long> TO_DATETIME_SCALES = ImmutableSet.of(0L, 3L, 6L);

    /**
     * Every key the row TTL prefix may spell. The prefix is what the collecting side matches on, so
     * a misspelling such as row_ttl_check_interval would otherwise be gathered up, stored, and never
     * looked at again: it escapes the per-key validation below because none of it asks for that key,
     * and it escapes the generic unknown-property check because the prefix already consumed it.
     */
    private static final Set<String> KNOWN_KEYS = ImmutableSet.of(
            PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT,
            PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND,
            PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE);

    private static final long MAX_INTERVAL_VALUE = Integer.MAX_VALUE;

    private static final String TO_DATETIME = "to_datetime";
    private static final String TO_DATETIME_NTZ = "to_datetime_ntz";

    private static final String EXPECTED_FORMS =
            "Expected <column>, <column> + INTERVAL <n> <unit>, to_datetime(<column>[, scale]), "
                    + "or to_datetime(<column>[, scale]) + INTERVAL <n> <unit>";

    public static boolean containsRowTtlProperty(Map<String, String> properties) {
        if (properties == null) {
            return false;
        }
        return properties.keySet().stream()
                .anyMatch(key -> key.startsWith(PropertyAnalyzer.PROPERTIES_ROW_TTL_PREFIX));
    }

    /**
     * Rejects a statement that would newly enable row TTL while the cluster has it switched off.
     * Kept apart from {@link #analyze}: this is an admission policy about the cluster, not a
     * judgement about the values, and a statement that only changes an already configured table
     * passes it whatever the switch says.
     */
    public static void checkAdmission(Map<String, String> current, Map<String, String> incoming) {
        boolean addingExpireAt = incoming.containsKey(PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT)
                && !current.containsKey(PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        if (addingExpireAt && !Config.enable_row_ttl) {
            throw new SemanticException("Row TTL is disabled. Set the FE configuration enable_row_ttl to true "
                    + "before enabling row TTL on a table");
        }
    }

    /**
     * @return the row TTL properties the table carries right now, which is what tells a statement
     *         adding row TTL apart from one changing it
     */
    public static Map<String, String> currentProperties(OlapTable table) {
        if (table.getTableProperty() == null) {
            return Collections.emptyMap();
        }
        Map<String, String> current = new HashMap<>();
        for (Map.Entry<String, String> entry : table.getTableProperty().getProperties().entrySet()) {
            if (entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_ROW_TTL_PREFIX)) {
                current.put(entry.getKey(), entry.getValue());
            }
        }
        return current;
    }

    /**
     * Rejects a set of row TTL property values that the table may not take.
     *
     * @param table    the table the properties are headed for, already carrying its full schema
     * @param current  row TTL properties the table already has, empty while creating a table
     * @param incoming row TTL properties this statement supplies
     * @throws SemanticException on the first rule the values break
     */
    public static void analyze(OlapTable table, Map<String, String> current, Map<String, String> incoming) {
        if (!containsRowTtlProperty(incoming)) {
            return;
        }
        checkTableMayCarryRowTtl(table);
        checkNoUnknownKey(incoming);

        checkNoEmptyValue(incoming);

        String expireAt = incoming.get(PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        boolean alreadyConfigured = current.containsKey(PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        if (expireAt == null && !alreadyConfigured) {
            throw new SemanticException("Cannot set %s or %s on a table without %s. Set the expiration expression "
                            + "in the same statement",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND,
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE,
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        }

        if (expireAt != null) {
            analyzeExpireAt(table, expireAt);
        }
        String checkInterval = incoming.get(PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND);
        if (checkInterval != null) {
            analyzeCheckIntervalSecond(checkInterval);
        }
        String timeZone = incoming.get(PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE);
        if (timeZone != null) {
            analyzeTimeZone(timeZone);
        }
    }

    /**
     * @return the column an already validated expiration expression names, or empty when the table
     *         has no row TTL
     */
    public static Optional<String> expireAtColumn(String expireAt) {
        if (expireAt == null || expireAt.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(timeColumnOf(stripInterval(parse(expireAt)), expireAt).getColumnName());
        } catch (Exception e) {
            // Only reachable if a stored value bypassed analyze(), so the table is already in a
            // state this class cannot describe. Report no lock rather than fail the caller's DDL.
            LOG.warn("cannot read the time column out of row_ttl_expire_at [{}]", expireAt, e);
            return Optional.empty();
        }
    }

    /**
     * Rejects an operation on a column that the table's expiration expression names, since the
     * expression would be left pointing at a column that no longer exists or no longer holds a time.
     *
     * @param operation what is being attempted, named the way the user phrased it, such as "dropped"
     */
    public static void checkColumnNotUsedByRowTtl(OlapTable table, String columnName, String operation) {
        if (table == null || columnName == null) {
            return;
        }
        String expireAt = table.getTableProperty() == null ? null
                : table.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        Optional<String> timeColumn = expireAtColumn(expireAt);
        if (timeColumn.isPresent() && timeColumn.get().equalsIgnoreCase(columnName)) {
            throw new SemanticException("Column %s cannot be %s because %s refers to it. Point %s at another column, "
                            + "or run ALTER TABLE %s DROP ROW TTL first",
                    columnName, operation, PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT,
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, table.getName());
        }
    }

    /**
     * Rejects a table that row TTL does not apply to. Checked on the way in and on the way out
     * alike: DROP ROW TTL on a table of the wrong kind is a mistake worth naming, not a no-op.
     */
    public static void checkTableMayCarryRowTtl(OlapTable table) {
        if (!table.isCloudNativeTable()) {
            throw new SemanticException("Row TTL is only supported on shared-data tables");
        }
        if (table.getKeysType() != KeysType.PRIMARY_KEYS) {
            throw new SemanticException("Row TTL is only supported on primary key tables");
        }
    }

    private static void checkNoUnknownKey(Map<String, String> incoming) {
        for (String key : incoming.keySet()) {
            if (key.startsWith(PropertyAnalyzer.PROPERTIES_ROW_TTL_PREFIX) && !KNOWN_KEYS.contains(key)) {
                throw new SemanticException("Unknown table property %s. The row TTL properties are %s",
                        key, String.join(", ", KNOWN_KEYS));
            }
        }
    }

    private static void checkNoEmptyValue(Map<String, String> incoming) {
        for (Map.Entry<String, String> entry : incoming.entrySet()) {
            if (!entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_ROW_TTL_PREFIX)) {
                continue;
            }
            if (entry.getValue() == null || entry.getValue().trim().isEmpty()) {
                throw new SemanticException("%s does not accept an empty value", entry.getKey());
            }
        }
    }

    private static void analyzeExpireAt(OlapTable table, String expireAt) {
        Expr base = stripInterval(parse(expireAt));
        SlotRef timeColumnRef = timeColumnOf(base, expireAt);
        boolean readsUnixTimestamp = isToDatetime(base);

        if (timeColumnRef.getTblNameWithoutAnalyzed() != null) {
            throw new SemanticException("%s cannot qualify a column with a table name: %s",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, expireAt);
        }
        String columnName = timeColumnRef.getColumnName();
        Column column = table.getColumn(columnName);
        if (column == null) {
            throw new SemanticException("Column %s does not exist in table %s", columnName, table.getName());
        }
        if (column.isAutoIncrement()) {
            throw new SemanticException("Column %s is an AUTO_INCREMENT column and holds row numbers rather than "
                    + "times, so it cannot carry a row TTL", columnName);
        }
        checkColumnTypeMatchesForm(column, readsUnixTimestamp);
    }

    private static void checkColumnTypeMatchesForm(Column column, boolean readsUnixTimestamp) {
        Type type = column.getType();
        if (readsUnixTimestamp) {
            if (!type.isInt() && !type.isBigint()) {
                throw new SemanticException("to_datetime reads a Unix timestamp, so column %s has to be INT or "
                        + "BIGINT, but it is %s", column.getName(), type.toSql());
            }
            return;
        }
        if (type.isDate() || type.isDatetime()) {
            return;
        }
        if (type.isInt() || type.isBigint()) {
            throw new SemanticException("Column %s is %s. An INT or BIGINT column has to hold a Unix timestamp and "
                            + "be wrapped with to_datetime(%s[, scale])",
                    column.getName(), type.toSql(), column.getName());
        }
        throw new SemanticException("Column %s is %s, but %s without to_datetime requires a DATE or DATETIME column",
                column.getName(), type.toSql(), PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
    }

    private static Expr parse(String expireAt) {
        try {
            Expr expr = SqlParser.parseSqlToExpr(expireAt, SqlModeHelper.MODE_DEFAULT);
            if (expr == null) {
                throw new SemanticException("Cannot parse %s [%s]. %s",
                        PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, expireAt, EXPECTED_FORMS);
            }
            return expr;
        } catch (SemanticException e) {
            throw e;
        } catch (Exception e) {
            throw new SemanticException("Cannot parse %s [%s]. %s",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, expireAt, EXPECTED_FORMS);
        }
    }

    /**
     * @return the expression with its trailing {@code + INTERVAL n unit} removed, having rejected
     *         every arithmetic shape that is not exactly that
     */
    private static Expr stripInterval(Expr expr) {
        if (!(expr instanceof TimestampArithmeticExpr)) {
            return expr;
        }
        TimestampArithmeticExpr arithmetic = (TimestampArithmeticExpr) expr;
        if (arithmetic.getFuncName() != null
                || arithmetic.getOp() != ArithmeticExpr.Operator.ADD
                || arithmetic.isIntervalFirst()) {
            throw new SemanticException("%s only accepts a trailing `+ INTERVAL <n> <unit>`. %s",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, EXPECTED_FORMS);
        }
        String unit = arithmetic.getTimeUnitIdent().toUpperCase(Locale.ROOT);
        if (!INTERVAL_UNITS.contains(unit)) {
            throw new SemanticException("INTERVAL unit %s is not supported by %s. Supported units are %s",
                    unit, PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, String.join(", ", INTERVAL_UNITS));
        }
        long amount = intLiteralValue(arithmetic.getChild(1),
                "the INTERVAL amount in " + PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT);
        if (amount <= 0 || amount > MAX_INTERVAL_VALUE) {
            throw new SemanticException("INTERVAL amount %d is out of range, it has to be between 1 and %d",
                    amount, MAX_INTERVAL_VALUE);
        }
        return arithmetic.getChild(0);
    }

    private static boolean isToDatetime(Expr base) {
        return base instanceof FunctionCallExpr && TO_DATETIME.equals(functionName((FunctionCallExpr) base));
    }

    /**
     * @param base the expression with its interval already stripped
     * @return the column reference it ultimately reads, having rejected every shape outside the four
     *         the property accepts
     */
    private static SlotRef timeColumnOf(Expr base, String expireAt) {
        if (base instanceof SlotRef) {
            return (SlotRef) base;
        }
        if (base instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) base;
            String name = functionName(call);
            if (TO_DATETIME_NTZ.equals(name)) {
                throw new SemanticException("to_datetime_ntz is not supported here; use to_datetime with %s = 'UTC'",
                        PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE);
            }
            if (TO_DATETIME.equals(name)) {
                return toDatetimeColumnRef(call);
            }
        }
        throw new SemanticException("%s does not accept [%s]. %s",
                PropertyAnalyzer.PROPERTIES_ROW_TTL_EXPIRE_AT, expireAt, EXPECTED_FORMS);
    }


    private static SlotRef toDatetimeColumnRef(FunctionCallExpr call) {
        List<Expr> args = call.getChildren();
        if (args.size() < 1 || args.size() > 2) {
            throw new SemanticException("to_datetime takes a column and an optional scale, but got %d arguments",
                    args.size());
        }
        if (!(args.get(0) instanceof SlotRef)) {
            throw new SemanticException("to_datetime has to read a column name here");
        }
        if (args.size() == 2) {
            long scale = intLiteralValue(args.get(1), "the to_datetime scale");
            if (!TO_DATETIME_SCALES.contains(scale)) {
                throw new SemanticException("to_datetime scale %d is not supported, it has to be 0 (seconds), "
                        + "3 (milliseconds) or 6 (microseconds)", scale);
            }
        }
        return (SlotRef) args.get(0);
    }

    private static String functionName(FunctionCallExpr call) {
        List<String> parts = call.getFnName().getParts();
        if (parts.size() != 1) {
            return null;
        }
        return parts.get(0).toLowerCase(Locale.ROOT);
    }

    private static long intLiteralValue(Expr expr, String what) {
        if (!(expr instanceof IntLiteral)) {
            throw new SemanticException("%s has to be an integer literal", what);
        }
        return ((IntLiteral) expr).getValue();
    }

    private static void analyzeCheckIntervalSecond(String value) {
        long seconds;
        try {
            seconds = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new SemanticException("%s has to be an integer number of seconds, but got [%s]",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND, value);
        }
        if (seconds <= 0) {
            throw new SemanticException("%s has to be greater than 0, but got %d",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_CHECK_INTERVAL_SECOND, seconds);
        }
    }

    private static void analyzeTimeZone(String value) {
        try {
            // Validation only: the standardized form it returns is deliberately dropped, so that
            // SHOW CREATE TABLE prints back what the user wrote.
            TimeUtils.checkTimeZoneValidAndStandardize(value);
        } catch (Exception e) {
            throw new SemanticException("%s [%s] is not a valid time zone. Use a region name such as 'Asia/Shanghai' "
                    + "or an offset such as '+08:00'",
                    PropertyAnalyzer.PROPERTIES_ROW_TTL_TIME_ZONE, value);
        }
    }
}
