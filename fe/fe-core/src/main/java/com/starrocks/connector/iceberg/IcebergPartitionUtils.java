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

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergPartitionTransform.YEAR;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);

    // Normalize partition name to yyyy-MM-dd (Type is Date) or yyyy-MM-dd HH:mm:ss (Type is Datetime)
    // Iceberg partition field transform support year, month, day, hour now,
    // eg.
    // year(ts)  partitionName : 2023              return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // month(ts) partitionName : 2023-01           return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // day(ts)   partitionName : 2023-01-01        return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // hour(ts)  partitionName : 2023-01-01-12     return 2023-01-01 12:00:00 (Datetime)
    public static String normalizeTimePartitionName(String partitionName,
                                                    PartitionField partitionField,
                                                    Schema schema,
                                                    Type type) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        boolean parseFromDate = true;
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == YEAR) {
            Preconditions.checkArgument(partitionName.length() == 4, "Invalid partition name: %s", partitionName);
            partitionName += "-01-01";
        } else if (transform == IcebergPartitionTransform.MONTH) {
            Preconditions.checkArgument(partitionName.length() == 7, "Invalid partition name: %s", partitionName);
            partitionName += "-01";
        } else if (transform == IcebergPartitionTransform.DAY) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else if (transform == IcebergPartitionTransform.HOUR) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
            parseFromDate = false;
        } else {
            throw new StarRocksConnectorException("Unsupported partition transform to normalize: %s",
                    partitionField.transform().toString());
        }

        // partition name formatter
        DateTimeFormatter formatter = null;
        if (type.isDate()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }
        // If has timestamp with time zone, should compute the time zone offset to UTC
        ZoneId zoneId;
        if (schema.findType(partitionField.sourceId()).equals(Types.TimestampType.withZone())) {
            zoneId = TimeUtils.getTimeZone().toZoneId();
        } else {
            zoneId = ZoneOffset.UTC;
        }

        String result;
        try {
            LocalDateTime datetime;
            if (parseFromDate) {
                // since it's from date, it can be converted to LocalDateTime by atStartOfDay
                datetime = LocalDate.parse(partitionName, dateTimeFormatter).atStartOfDay();
            } else {
                // parse from datetime which contains hour
                datetime = LocalDateTime.parse(partitionName, dateTimeFormatter);
            }
            // convert from UTC to local time
            LocalDateTime localDateTime = convertTimezone(datetime, ZoneOffset.UTC, zoneId);
            // format to string
            result = localDateTime.format(formatter);
        } catch (Exception e) {
            LOG.warn("parse partition name failed, partitionName: {}, partitionField: {}, type: {}",
                    partitionName, partitionField, type);
            throw new StarRocksConnectorException("parse/format partition name failed", e);
        }
        return result;
    }

    public static LocalDateTime convertTimezone(LocalDateTime time, ZoneId from, ZoneId to) {
        return time.atZone(from).withZoneSameInstant(to).toLocalDateTime();
    }

    public static Term convertPartitionExprToTerm(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return Expressions.ref(slotRef.getColumnName());
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            String fn = functionCallExpr.getFunctionName();
            Expr child = functionCallExpr.getChild(0);
            if (child instanceof SlotRef) {
                String colName = ((SlotRef) child).getColumnName();
                switch (fn.toLowerCase(Locale.ROOT)) {
                    case "year":
                        return Expressions.year(colName);
                    case "month":
                        return Expressions.month(colName);
                    case "day":
                        return Expressions.day(colName);
                    case "hour":
                        return Expressions.hour(colName);
                    case "identity":
                        return Expressions.ref(colName);
                    case "truncate":
                        IntLiteral width = (IntLiteral) functionCallExpr.getChild(1);
                        return Expressions.truncate(colName, (int) width.getValue());
                    case "bucket":
                        IntLiteral numBuckets = (IntLiteral) functionCallExpr.getChild(1);
                        return Expressions.bucket(colName, (int) numBuckets.getValue());
                    case "void":
                        // not supported yet.
                    default:
                        throw new SemanticException(
                                "Unsupported partition transform %s for column %s", fn, colName);
                }
            } else {
                throw new SemanticException("Unsupported partition transform %s for arguments", fn);
            }
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    public static String normalizePartitionExpr(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return "`" + slotRef.getColumnName() + "`";
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            String fn = functionCallExpr.getFunctionName().toLowerCase(Locale.ROOT);
            Expr child = functionCallExpr.getChild(0);
            if (!(child instanceof SlotRef slotRef)) {
                throw new SemanticException("Unsupported partition transform %s for arguments",
                        functionCallExpr.getFunctionName());
            }

            String quotedColumn = "`" + slotRef.getColumnName() + "`";
            switch (fn) {
                case "year":
                case "month":
                case "day":
                case "hour":
                    return String.format("%s(%s)", fn, quotedColumn);
                case "identity":
                    return quotedColumn;
                case "truncate":
                case "bucket":
                    IntLiteral number = (IntLiteral) functionCallExpr.getChild(1);
                    return String.format("%s(%s, %s)", fn, quotedColumn, number.getValue());
                case "void":
                    // not supported yet.
                default:
                    throw new SemanticException("Unsupported partition transform %s for column %s",
                            functionCallExpr.getFunctionName(), slotRef.getColumnName());
            }
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    public static String getPartitionExprSourceColumn(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return slotRef.getColumnName();
        } else if (expr instanceof FunctionCallExpr functionCallExpr) {
            Expr child = functionCallExpr.getChild(0);
            if (child instanceof SlotRef slotRef) {
                return slotRef.getColumnName();
            }
            throw new SemanticException("Unsupported partition transform %s for arguments",
                    functionCallExpr.getFunctionName());
        } else {
            throw new SemanticException("Does not support partition clause: " + expr);
        }
    }

    // Get the date interval from iceberg partition transform
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromIceberg(IcebergTable table,
                                                                                Column partitionColumn) {
        PartitionField partitionField = table.getPartitionFiled(partitionColumn.getName());
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColumn.getName(), table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
        }
        String transform = partitionField.transform().toString();
        IcebergPartitionTransform icebergPartitionTransform = IcebergPartitionTransform.fromString(transform);
        switch (icebergPartitionTransform) {
            case YEAR:
                return PartitionUtil.DateTimeInterval.YEAR;
            case MONTH:
                return PartitionUtil.DateTimeInterval.MONTH;
            case DAY:
                return PartitionUtil.DateTimeInterval.DAY;
            case HOUR:
                return PartitionUtil.DateTimeInterval.HOUR;
            default:
                return PartitionUtil.DateTimeInterval.NONE;
        }
    }

    public static boolean isSupportedConvertPartitionTransform(IcebergPartitionTransform transform) {
        return transform == IcebergPartitionTransform.IDENTITY ||
                transform == YEAR ||
                transform == IcebergPartitionTransform.MONTH ||
                transform == IcebergPartitionTransform.DAY ||
                transform == IcebergPartitionTransform.HOUR ||
                transform == IcebergPartitionTransform.BUCKET ||
                transform == IcebergPartitionTransform.TRUNCATE;
    }

    public static LocalDateTime addDateTimeInterval(LocalDateTime dateTime, IcebergPartitionTransform transform) {
        switch (transform) {
            case YEAR:
                return dateTime.plusYears(1);
            case MONTH:
                return dateTime.plusMonths(1);
            case DAY:
                return dateTime.plusDays(1);
            case HOUR:
                return dateTime.plusHours(1);
            default:
                throw new StarRocksConnectorException("Unsupported partition transform to add: %s", transform);
        }
    }

    /**
        convert partition value to predicate
        eg.
        partitionColumn: ts(date)
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01' and ts < '2024-01-01'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01' and ts < '2023-02-01'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01' and ts < '2023-01-02'

        partitionColumn: ts(datetime)   transform: year
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01 00:00:00' and ts < '2024-01-01 00:00:00'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01 00:00:00' and ts < '2023-02-01 00:00:00'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01 00:00:00' and ts < '2023-01-02 00:00:00'
        partitionValue: 2023-01-01-12  transform: hour
        return ts >= '2023-01-01 12:00:00' and ts < '2023-01-01 13:00:00'
    */
    public static Range<String> toPartitionRange(IcebergTable table, String partitionColumn,
                                                 String partitionValue, PartitionField partitionField,
                                                 boolean isFromIcebergTime) {
        Preconditions.checkArgument(partitionField != null,
                "Partition field is null for column: %s", partitionColumn);
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.IDENTITY) {
            return Range.singleton(partitionValue);
        } else {
            // transform is year, month, day, hour
            Type partitiopnColumnType = table.getColumn(partitionColumn).getType();
            Preconditions.checkState(partitiopnColumnType.isDateType(),
                    "Partition column %s type must be date or datetime", partitionColumn);
            if (isFromIcebergTime) {
                partitionValue = normalizeTimePartitionName(partitionValue, partitionField,
                        table.getNativeTable().schema(), partitiopnColumnType);
            }
            LocalDateTime startDateTime = null;
            DateTimeFormatter dateTimeFormatter = null;
            if (partitiopnColumnType.isDate()) {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                startDateTime = LocalDate.parse(partitionValue, dateTimeFormatter).atStartOfDay();
            } else {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                startDateTime = LocalDateTime.parse(partitionValue, dateTimeFormatter);
            }
            LocalDateTime endDateTime = addDateTimeInterval(startDateTime, transform);
            String endDateTimeStr = endDateTime.format(dateTimeFormatter);
            return Range.closedOpen(partitionValue, endDateTimeStr);
        }
    }

    /**
     * Convert iceberg partition transform to sql predicate
     * eg.
     * Iceberg table partition column: day(dt)
     * partition value      : 2023-01-02
     * generated predicate  : dt >= '2023-01-01 00:08:00' and dt < '2023-01-02:00:08:00'
     */
    public static String convertPartitionTransformToPredicate(IcebergTable table, PartitionField partitionField,
                                                              String partitionColumn, String partitionValue) {
        if (partitionField == null || Strings.isNullOrEmpty(partitionColumn) || Strings.isNullOrEmpty(partitionValue)) {
            throw new StarRocksConnectorException("Partition field/column/value is null");
        }
        IcebergPartitionTransform transform =
                IcebergPartitionTransform.fromString(partitionField.transform().toString());
        String partitionCol = StatisticUtils.quoting(partitionColumn);

        // Handle bucket and truncate explicitly
        if (transform == IcebergPartitionTransform.BUCKET) {
            // transform string format: bucket[<num>]
            int numBuckets = extractTransformParam(partitionField.transform().toString());
            int bucketId;
            try {
                bucketId = Integer.parseInt(partitionValue);
            } catch (NumberFormatException e) {
                throw new StarRocksConnectorException("Invalid bucket partition value: %s", partitionValue);
            }
            String fn = FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX + "bucket";
            return String.format("%s(%s, %d) = %d", fn, partitionCol, numBuckets, bucketId);
        } else if (transform == IcebergPartitionTransform.TRUNCATE) {
            // transform string format: truncate[<width>]
            int width = extractTransformParam(partitionField.transform().toString());
            Type partitionType = table.getColumn(partitionColumn).getType();
            if (partitionType.isBinaryType()) {
                try {
                    partitionValue = new String(Base64.getDecoder().decode(partitionValue));
                } catch (Exception e) {
                    throw new StarRocksConnectorException("Invalid base64 partition value: %s", partitionValue, e);
                }
            }
            String fn = FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX + "truncate";
            return String.format("%s(%s, %d) = '%s'", fn, partitionCol, width, partitionValue);
        }

        Range<String> range = toPartitionRange(table, partitionColumn, partitionValue, partitionField, true);
        if (range.lowerEndpoint().equals(range.upperEndpoint())) {
            return String.format("%s = '%s'", partitionCol, range.lowerEndpoint());
        } else {
            String lowerEndpoint = range.lowerEndpoint();
            String upperEndpoint = range.upperEndpoint();
            return String.format("%s >= '%s' and %s < '%s'", partitionCol, lowerEndpoint, partitionCol, upperEndpoint);
        }
    }

    private static int extractTransformParam(String transform) {
        int l = transform.indexOf('[');
        int r = transform.indexOf(']');
        if (l >= 0 && r > l) {
            try {
                return Integer.parseInt(transform.substring(l + 1, r));
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        throw new StarRocksConnectorException("Unsupported or missing transform parameter: %s", transform);
    }

    public static Expr getIcebergTablePartitionPredicateExpr(IcebergTable table,
                                                             String partitionColName,
                                                             SlotRef slotRef,
                                                             Expr expr) {
        return getIcebergTablePartitionPredicateExpr(table, partitionColName, slotRef, ImmutableList.of(expr));
    }

    /**
     * Generate Iceberg's partition predicate according its partition transform.
     * eg:
     * Iceberg table partition column: day(dt)
     * partition value      : 2023-01-02
     * generated predicate  : dt >= '2023-01-01 00:08:00' and dt < '2023-01-02:00:08:00'
     * NOTE: use range predicate rather than `date_trunc` function for better partition prune in Iceberg SDK.
     */
    public static Expr getIcebergTablePartitionPredicateExpr(IcebergTable table,
                                                             String partitionColName,
                                                             SlotRef slotRef,
                                                             List<Expr> exprs) {
        PartitionField partitionField = table.getPartitionFiled(partitionColName);
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColName, table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
        }
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.IDENTITY) {
            return MvUtils.convertToInPredicate(slotRef, exprs);
        } else {
            List<Expr> result = Lists.newArrayList();
            for (Expr expr : exprs) {
                if (!(expr instanceof LiteralExpr)) {
                    throw new StarRocksConnectorException("Partition value must be literal");
                }
                String partitionVal = ((LiteralExpr) expr).getStringValue();
                Range<String> range = toPartitionRange(table, partitionColName, partitionVal, partitionField,
                        false);
                Preconditions.checkArgument(!range.lowerEndpoint().equals(range.upperEndpoint()),
                        "Partition value must be range");
                try {
                    LiteralExpr lowerExpr = LiteralExprFactory.create(range.lowerEndpoint(), slotRef.getType());
                    LiteralExpr upperExpr = LiteralExprFactory.create(range.upperEndpoint(), slotRef.getType());
                    Expr lower = new BinaryPredicate(BinaryType.GE, slotRef, lowerExpr);
                    Expr upper = new BinaryPredicate(BinaryType.LT, slotRef, upperExpr);
                    result.add(ExprUtils.compoundAnd(ImmutableList.of(lower, upper)));
                } catch (AnalysisException e) {
                    throw new StarRocksConnectorException("Create literal expr failed", e);
                }
            }
            return ExprUtils.compoundOr(result);
        }
    }
    /**
     * Decide whether partition-change-tracking (PCT) refresh can work on an Iceberg table whose partition spec
     * has evolved.
     * <p>
     * PCT maps a base partition onto an MV partition by position: the MV's ref partition columns are located in
     * the current spec, and the values at the same positions are read from every partition name. Partition
     * names are rendered per file spec, in partition-field-id order, and only contain that spec's fields. So
     * the mapping stays correct when:
     * <ol>
     * <li>every historical spec, ignoring its void fields (fields dropped from a format v1 table, which keep
     * their slot with a void transform), is a positional prefix of the current spec (same field id, source
     * column and transform at each position), so in practice the current spec must extend every earlier spec
     * by appending fields: for example (a, b) -&gt; (a) -&gt; (a, c) is rejected because (a, b) is not a prefix
     * of (a, c),</li>
     * <li>the current spec's field ids ascend with position, so id order equals position order,</li>
     * <li>the current spec has no void field, so the table's partition columns are exactly its fields,</li>
     * <li>every MV ref partition column sits inside the prefix shared by all specs, so no partition name lacks
     * a value for it.</li>
     * </ol>
     * Appending a partition field that the MV does not partition on is the typical tolerated case.
     * <p>
     * An empty {@code refPartitionColumnNames} still enforces conditions 1-3. A non-ref base table does not need
     * them, because its partition names are only compared for change detection, but they guard a ref base table
     * whose partition columns could not be resolved and is therefore checked as a non-ref one.
     *
     * @param table                   the native Iceberg table
     * @param refPartitionColumnNames the base table columns the MV partitions by; empty for a non-ref base table
     * @return empty when PCT refresh is safe, otherwise the reason it is not
     */
    public static Optional<String> checkPartitionEvolutionCompatible(Table table,
                                                                     List<String> refPartitionColumnNames) {
        PartitionSpec current = table.spec();
        List<PartitionField> currentFields = current.fields();
        for (int i = 0; i < currentFields.size(); i++) {
            PartitionField field = currentFields.get(i);
            if (field.transform().isVoid()) {
                return Optional.of(String.format("current partition spec %d still contains dropped field %s",
                        current.specId(), field.name()));
            }
            if (i > 0 && field.fieldId() <= currentFields.get(i - 1).fieldId()) {
                return Optional.of(String.format("fields of current partition spec %d are not in the order they " +
                        "were added (field %s)", current.specId(), field.name()));
            }
        }

        int sharedPrefix = currentFields.size();
        for (PartitionSpec spec : table.specs().values()) {
            if (spec.specId() == current.specId()) {
                continue;
            }
            List<PartitionField> fields = spec.fields().stream()
                    .filter(field -> !field.transform().isVoid())
                    .collect(Collectors.toList());
            if (fields.size() > currentFields.size()) {
                return Optional.of(String.format("partition spec %d has more fields than current partition spec %d",
                        spec.specId(), current.specId()));
            }
            for (int i = 0; i < fields.size(); i++) {
                PartitionField old = fields.get(i);
                PartitionField cur = currentFields.get(i);
                if (old.fieldId() != cur.fieldId() || old.sourceId() != cur.sourceId()
                        || !old.transform().toString().equals(cur.transform().toString())) {
                    return Optional.of(String.format("field %s of partition spec %d differs from field %s of " +
                                    "current partition spec %d (field id, source column or transform)",
                            old.name(), spec.specId(), cur.name(), current.specId()));
                }
            }
            sharedPrefix = Math.min(sharedPrefix, fields.size());
        }

        Schema schema = table.schema();
        for (String columnName : refPartitionColumnNames) {
            int position = -1;
            for (int i = 0; i < currentFields.size(); i++) {
                String sourceName = schema.findColumnName(currentFields.get(i).sourceId());
                if (sourceName != null && sourceName.equalsIgnoreCase(columnName)) {
                    position = i;
                    break;
                }
            }
            if (position < 0) {
                return Optional.of(String.format("partition column %s is not a partition field of current " +
                        "partition spec %d", columnName, current.specId()));
            }
            if (position >= sharedPrefix) {
                return Optional.of(String.format("partition column %s is not in every historical partition spec " +
                        "(only the first %d field(s) are shared)", columnName, sharedPrefix));
            }
        }
        return Optional.empty();
    }
}
