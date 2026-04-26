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


package com.starrocks.connector;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PartitionDiff;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.FileUtils.escapePathName;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class PartitionUtil {
    // used for compute date/datetime literal
    public enum DateTimeInterval {
        NONE,
        YEAR,
        MONTH,
        DAY,
        HOUR
    }

    private static final Logger LOG = LogManager.getLogger(PartitionUtil.class);
    public static final String ICEBERG_DEFAULT_PARTITION = "ICEBERG_DEFAULT_PARTITION";

    public static final String MYSQL_PARTITION_MAXVALUE = "MAXVALUE";

    @Deprecated
    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
        return createPartitionKey(values, columns, Table.TableType.HIVE);
    }

    /**
     * Use createPartitionKey instead, because `createPartitionKeyWithType` not takes care timezone.
     */
    @Deprecated
    public static PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types,
                                                          Table.TableType tableType) throws AnalysisException {
        return ConnectorPartitionTraits.build(tableType).createPartitionKeyWithType(values, types);
    }

    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns,
                                                  Table.TableType tableType) throws AnalysisException {
        Preconditions.checkState(values.size() == columns.size(),
                "columns size is %s, but values size is %s", columns.size(), values.size());

        return createPartitionKeyWithType(values, columns.stream().map(Column::getType).collect(Collectors.toList()), tableType);
    }

    public static PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns,
                                                  Table table) throws AnalysisException {
        Preconditions.checkState(partitionValues.size() == partitionColumns.size(),
                "partition columns size is %s, but partition values size is %s",
                partitionColumns.size(), partitionValues.size());

        return ConnectorPartitionTraits.build(table).createPartitionKey(partitionValues, partitionColumns);
    }


    // If partitionName is `par_col=0/par_date=2020-01-01`, return ["0", "2020-01-01"]
    public static List<String> toPartitionValues(String partitionName) {
        // mimics Warehouse.makeValsFromName
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        Iterable<String> pieces = Splitter.on("/").split(partitionName);
        for (String piece : pieces) {
            int idx = piece.indexOf("=");
            if (idx == -1) {
                break;
            }
            resultBuilder.add(unescapePathName(piece.substring(idx + 1)));
        }
        return resultBuilder.build();
    }

    public static String toHivePartitionName(List<String> partitionColumnNames,
                                             PartitionKey partitionKey) {
        List<String> partitionValues = fromPartitionKey(partitionKey);
        return toHivePartitionName(partitionColumnNames, partitionValues);
    }

    public static String toHivePartitionName(List<String> partitionColumnNames,
                                             List<String> partitionValues) {
        return FileUtils.makePartName(partitionColumnNames, partitionValues);
    }

    public static String toHivePartitionName(List<String> partitionColNames, Map<String, String> partitionColNameToValue) {
        int i = 0;
        StringBuilder name = new StringBuilder();
        for (String partitionColName : partitionColNames) {
            if (i++ > 0) {
                name.append(Path.SEPARATOR);
            }
            String partitionValue = partitionColNameToValue.get(partitionColName);
            if (partitionValue == null) {
                throw new StarRocksConnectorException("Can't find column {} in {}", partitionColName, partitionColNameToValue);
            }
            name.append(escapePathName(partitionColName.toLowerCase(Locale.ROOT)));
            name.append('=');
            name.append(escapePathName(partitionValue.toLowerCase(Locale.ROOT)));

        }
        return name.toString();
    }

    public static List<String> fromPartitionKey(PartitionKey key) {
        // get string value from partitionKey
        List<LiteralExpr> literalValues = key.getKeys();
        List<String> values = new ArrayList<>(literalValues.size());
        for (LiteralExpr value : literalValues) {
            if (value instanceof NullLiteral) {
                values.add(key.getNullPartitionValue());
            } else if (value instanceof BoolLiteral) {
                BoolLiteral boolValue = ((BoolLiteral) value);
                values.add(String.valueOf(boolValue.getValue()));
            } else if (key instanceof HivePartitionKey && value instanceof DateLiteral) {
                // Special handle Hive timestamp partition key
                values.add(getHiveFormatStringValue((DateLiteral) value));
            } else {
                values.add(value.getStringValue());
            }
        }
        return values;
    }

    private static String getHiveFormatStringValue(DateLiteral dateLiteral) {
        if (dateLiteral.getType().getPrimitiveType() == PrimitiveType.DATE) {
            return String.format("%04d-%02d-%02d", dateLiteral.getYear(), dateLiteral.getMonth(), dateLiteral.getDay());
        } else {
            if (dateLiteral.getMicrosecond() == 0) {
                String datetime = String.format("%04d-%02d-%02d %02d:%02d:%02d", dateLiteral.getYear(),
                        dateLiteral.getMonth(),
                        dateLiteral.getDay(), dateLiteral.getHour(), dateLiteral.getMinute(),
                        dateLiteral.getSecond());
                if (dateLiteral.getPrecision() > 0) {
                    // 2007-01-01 10:35:00 => 2007-01-01 10:35:00.000000(precision=6)
                    datetime = datetime + "." +
                            String.join("", Collections.nCopies(dateLiteral.getPrecision(), "0"));
                }
                return datetime;
            } else {
                // 2007-01-01 10:35:00.123000 => 2007-01-01 10:35:00.123
                return String.format("%04d-%02d-%02d %02d:%02d:%02d.%6d", dateLiteral.getYear(), dateLiteral.getMonth(),
                        dateLiteral.getDay(), dateLiteral.getHour(), dateLiteral.getMinute(), dateLiteral.getSecond(),
                        dateLiteral.getMicrosecond()).replaceFirst("0{1,6}$", "");
            }
        }
    }

    public static List<String> getPartitionNames(Table table) {
        return ConnectorPartitionTraits.build(table).getPartitionNames();
    }

    // use partitionValues to filter partitionNames
    // eg. partitionNames: [p1=1/p2=2, p1=1/p2=3, p1=2/p2=2, p1=2/p2=3]
    //     partitionValues: [empty, 2]
    // return [p1=1/p2=2, p1=2/p2=2]
    public static List<String> getFilteredPartitionKeys(List<String> partitionNames, List<Optional<String>> partitionValues) {
        List<String> filteredPartitionName = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            List<String> values = toPartitionValues(partitionName);
            int index = 0;
            for (; index < values.size(); ++index) {
                if (partitionValues.get(index).isPresent()) {
                    if (!values.get(index).equals(partitionValues.get(index).get())) {
                        break;
                    }
                }
            }
            if (index == values.size()) {
                filteredPartitionName.add(partitionName);
            }
        }
        return filteredPartitionName;
    }


    public static List<PartitionKey> getPartitionKeys(Table table) {
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        if (table.isUnPartitioned()) {
            partitionKeys.add(new PartitionKey());
        } else {
            List<String> partitionNames = getPartitionNames(table);
            List<Column> partitionColumns = getPartitionColumns(table);
            try {
                for (String partitionName : partitionNames) {
                    partitionKeys.add(
                            createPartitionKey(toPartitionValues(partitionName), partitionColumns, table));
                }
            } catch (Exception e) {
                LOG.error("Failed to get partition keys", e);
                throw new StarRocksConnectorException("Failed to get partition keys", e);
            }
        }
        return partitionKeys;
    }

    public static List<Column> getPartitionColumns(Table table) {
        return ConnectorPartitionTraits.build(table).getPartitionColumns();
    }

    // partition name such like p1=1/p2=__HIVE_DEFAULT_PARTITION__, this function will convert it to
    // p1=1/p2=NULL
    public static String normalizePartitionName(String partitionName, List<String> partitionColumnNames, String nullValue) {
        List<String> partitionValues = Lists.newArrayList(toPartitionValues(partitionName));
        for (int i = 0; i < partitionValues.size(); ++i) {
            if (partitionValues.get(i).equals(nullValue)) {
                partitionValues.set(i, "NULL");
            }
        }
        return toHivePartitionName(partitionColumnNames, partitionValues);
    }

    // check the partitionColumn exist in the partitionColumns
    public static int checkAndGetPartitionColumnIndex(List<Column> partitionColumns, Column partitionColumn)
            throws AnalysisException {
        int partitionColumnIndex = -1;
        for (int index = 0; index < partitionColumns.size(); ++index) {
            if (partitionColumns.get(index).equals(partitionColumn)) {
                partitionColumnIndex = index;
                break;
            }
        }
        if (partitionColumnIndex == -1) {
            throw new AnalysisException("Materialized view partition column in partition exp " +
                    "must be base table partition column");
        }
        return partitionColumnIndex;
    }

    public static Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table table) {
        return ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo();
    }

    public static Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table table, List<String> partitionNames) {
        return ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo(partitionNames);
    }

    // Iceberg Table has partition transforms like this:
    // partition column is (ts timestamp), table could partition by year(ts), month(ts), day(ts), hour(ts)
    // this function could get the dateInterval from partition transform
    public static DateTimeInterval getDateTimeInterval(Table table, Column partitionColumn) {
        if (partitionColumn.getType() != DateType.DATE && partitionColumn.getType() != DateType.DATETIME) {
            return DateTimeInterval.NONE;
        }
        if (table.isIcebergTable()) {
            return IcebergPartitionUtils.getDateTimeIntervalFromIceberg((IcebergTable) table, partitionColumn);
        } else {
            // add 1 day as default interval
            return DateTimeInterval.DAY;
        }
    }

    /**
     * If base table column type is string but partition type is date, we need to convert the string to date
     * @param partitionExpr   PARTITION BY expr
     * @param partitionColumn PARTITION BY referenced column
     * @return
     */
    public static boolean isConvertToDate(Expr partitionExpr, Column partitionColumn) {
        if (!(partitionExpr instanceof FunctionCallExpr)) {
            return false;
        }
        return isConvertToDate(partitionExpr.getType(), partitionColumn.getType());
    }

    /**
     * Check whether convert filter to date type.
     * @param partitionType     : Partition defined type.
     * @param filterType        : Filter type from query.
     * @return: true if convert is needed, false if not.
     */
    public static boolean isConvertToDate(Type partitionType, Type filterType) {
        if (partitionType == null || filterType == null) {
            return false;
        }

        PrimitiveType filterPrimitiveType = filterType.getPrimitiveType();
        PrimitiveType partitionPrimitiveType = partitionType.getPrimitiveType();
        return partitionPrimitiveType.isDateType() && !filterPrimitiveType.isDateType();
    }

    /**
     * Check whether convert partition column filter to date type.
     * @param partitionColumn           : Partition column which is defined in adding partitions.
     * @param partitionColumnFilter     : Partition column filter from query.
     * @return : true if partition column is defined as date type but filter is not date type.
     */
    public static boolean isConvertToDate(Column partitionColumn, PartitionColumnFilter partitionColumnFilter) {
        if (partitionColumnFilter == null || partitionColumn == null) {
            return false;
        }
        Type filterType = partitionColumnFilter.getFilterType();
        if (filterType == null) {
            return false;
        }
        return isConvertToDate(partitionColumn.getType(), filterType);
    }

    /**
     * Convert a string literal expr to a date literal.
     * @param stringLiteral: input string literal to convert.
     * @return             : date literal if string literal can be converted, otherwise throw SemanticException.
     */
    public static LiteralExpr convertToDateLiteral(LiteralExpr stringLiteral) throws SemanticException {
        if (stringLiteral == null) {
            return null;
        }
        if (stringLiteral.isConstantNull()) {
            return NullLiteral.create(DateType.DATE);
        }
        try {
            String dateLiteral = stringLiteral.getStringValue();
            LocalDateTime dateValue = DateUtils.parseStrictDateTime(dateLiteral);
            return new DateLiteral(dateValue, DateType.DATE);
        } catch (Exception e) {
            throw new SemanticException("create string to date literal failed:" +  stringLiteral.getStringValue(), e);
        }
    }

    /**
     * Convert a string type partition key to date type partition key.
     * @param partitionKey : input string partition key to convert.
     * @return             : partition key with date type if input can be converted.
     */
    static PartitionKey convertToDate(PartitionKey partitionKey) throws SemanticException {
        PartitionKey newPartitionKey = new PartitionKey();
        try {
            LiteralExpr dateLiteral = convertToDateLiteral(partitionKey.getKeys().get(0));
            newPartitionKey.pushColumn(dateLiteral, PrimitiveType.DATE);
            return newPartitionKey;
        } catch (SemanticException e) {
            SemanticException semanticException =
                    new SemanticException("convert string %s to date partition key failed:",
                            partitionKey.getKeys().get(0).getStringValue(), e);
            semanticException.addSuppressed(e);
            throw semanticException;
        }
    }

    /**
     * Get MV's partition column indexes in ref base table's partition columns.
     * NOTE:MV's partition columns may not be the same with ref base table's partition columns which may be less than ref base
     * table's partition columns or not in the same order.
     */
    public static List<Integer> getRefBaseTablePartitionColumIndexes(MaterializedView mv,
                                                                     Table refBaseTable) {
        Map<Table, List<Column>> mvRefBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (!mvRefBaseTablePartitionColumns.containsKey(refBaseTable)) {
            return null;
        }
        List<Column> mvRefBaseTablePartitionCols = mvRefBaseTablePartitionColumns.get(refBaseTable);
        if (mvRefBaseTablePartitionCols.size() > refBaseTable.getPartitionColumns().size()) {
            return null;
        }
        List<Column> refBaseTablePartitionColumns = refBaseTable.getPartitionColumns();
        return mvRefBaseTablePartitionCols.stream()
                .map(col -> refBaseTablePartitionColumns.indexOf(col))
                .collect(Collectors.toList());
    }

    /**
     * Return the partition key of the selected partition columns. colIndexes is the index of selected partition columns.
     */
    public static PartitionKey getSelectedPartitionKey(PartitionKey partitionKey,
                                                       List<Integer> colIndexes) {
        if (partitionKey.getKeys().size() <= 1 || colIndexes == null) {
            return partitionKey;
        }
        List<LiteralExpr> newPartitionKeys =
                colIndexes.stream().map(partitionKey.getKeys()::get).collect(Collectors.toList());
        List<PrimitiveType> newPartitionTypes =
                colIndexes.stream().map(partitionKey.getTypes()::get).collect(Collectors.toList());
        return new PartitionKey(newPartitionKeys, newPartitionTypes);
    }

    // return partition name in forms of `col1=value1/col2=value2`
    // if the partition field is explicitly named, use this name without change
    // if the partition field is not identity transform, column name is appended by its transform name (e.g. col1_hour)
    // if all partition fields are no longer active (dropped by partition evolution), return "ICEBERG_DEFAULT_PARTITION"
    public static String convertIcebergPartitionToPartitionName(org.apache.iceberg.Table table, PartitionSpec spec,
                                                                StructProjection partition) {
        StringBuilder sb = new StringBuilder();
        // partition have all active partition types, so we need to get full partition type from table
        Types.StructType partitionType = Partitioning.partitionType(table);

        for (int i = 0; i < partitionType.fields().size(); i++) {
            Types.NestedField field = partitionType.fields().get(i);
            for (PartitionField partitionField : spec.fields()) {
                if (partitionField.fieldId() == field.fieldId()) {
                    // skip inactive partition field
                    if (partitionField.transform().isVoid()) {
                        continue;
                    }
                    org.apache.iceberg.types.Type type = spec.partitionType().fieldType(partitionField.name());
                    sb.append(partitionField.name());
                    sb.append("=");
                    String value = partitionField.transform().toHumanString(type,
                            getPartitionValue(partition, i, type.typeId().javaClass()));
                    sb.append(value);
                    sb.append("/");
                }
            }
        }
        if (!sb.isEmpty()) {
            return sb.substring(0, sb.length() - 1);
        }
        return ICEBERG_DEFAULT_PARTITION;
    }

    public static List<String> getIcebergPartitionValues(PartitionSpec spec,
                                                         StructLike partition,
                                                         boolean existPartitionTransformedEvolution) {
        PartitionData partitionData = (PartitionData) partition;
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < spec.fields().size(); i++) {
            PartitionField partitionField = spec.fields().get(i);
            if ((!partitionField.transform().isIdentity() && existPartitionTransformedEvolution) ||
                    (partitionField.transform().isVoid() && partitionData.get(i) == null)) {
                continue;
            }

            Class<?> clazz = spec.javaClasses()[i];
            String value = null;
            if (getPartitionValue(partitionData, i, clazz) != null) {
                value = partitionField.transform().toHumanString(getPartitionValue(partitionData, i, clazz));
            }

            // currently starrocks date literal only support local datetime
            org.apache.iceberg.types.Type icebergType = spec.schema().findType(partitionField.sourceId());
            if (value != null && partitionField.transform().isIdentity() &&
                    icebergType.equals(Types.TimestampType.withZone())) {
                value = ChronoUnit.MICROS.addTo(Instant.ofEpochSecond(0).atZone(TimeUtils.getTimeZone().toZoneId()),
                        getPartitionValue(partitionData, i, clazz)).toLocalDateTime().toString();
            }

            partitionValues.add(value);
        }

        return partitionValues;
    }

    public static <T> T getPartitionValue(StructLike partition, int position, Class<?> javaClass) {
        return partition.get(position, (Class<T>) javaClass);
    }

    public static LiteralExpr addOffsetForLiteral(LiteralExpr expr, int offset, DateTimeInterval dateTimeInterval)
            throws AnalysisException {
        if (expr instanceof DateLiteral) {
            // If expr is date literal, add offset should consider the date interval
            DateLiteral lowerDate = (DateLiteral) expr;
            if (dateTimeInterval == DateTimeInterval.YEAR) {
                return new DateLiteral(lowerDate.toLocalDateTime().plusYears(offset), expr.getType());
            } else if (dateTimeInterval == DateTimeInterval.MONTH) {
                return new DateLiteral(lowerDate.toLocalDateTime().plusMonths(offset), expr.getType());
            } else if (dateTimeInterval == DateTimeInterval.HOUR) {
                return new DateLiteral(lowerDate.toLocalDateTime().plusHours(offset), expr.getType());
            } else {
                return new DateLiteral(lowerDate.toLocalDateTime().plusDays(offset), expr.getType());
            }
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return new IntLiteral(intLiteral.getLongValue() + offset);
        } else if (expr instanceof MaxLiteral) {
            return MaxLiteral.MAX_VALUE;
        } else {
            return null;
        }
    }

    public static String getSuffixName(String dirPath, String filePath) {
        if (!FeConstants.runningUnitTest) {
            Preconditions.checkArgument(filePath.startsWith(dirPath),
                    "dirPath " + dirPath + " should be prefix of filePath " + filePath);
        }

        //we had checked the startsWith, so just get substring
        String name = filePath.substring(dirPath.length());
        if (name.startsWith("/")) {
            name = name.substring(1);
        }
        return name;
    }

    public static <T> void executeInNewThread(String threadName, Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        Thread thread = new Thread(task);
        thread.setName(threadName);
        thread.setDaemon(true);
        thread.start();
    }

    public static <T> void executeInNewThread(String threadName, Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName(threadName);
        thread.setDaemon(true);
        thread.start();
    }

    public static PartitionDiff getPartitionDiff(Expr partitionExpr,
                                                 PCellSortedSet basePartitionMap,
                                                 PCellSortedSet mvPartitionMap,
                                                 RangePartitionDiffer differ) {
        if (partitionExpr instanceof SlotRef) {
            return SyncPartitionUtils.getRangePartitionDiffOfSlotRef(basePartitionMap, mvPartitionMap, differ);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            if (functionCallExpr.getFunctionName().equalsIgnoreCase(FunctionSet.DATE_TRUNC) ||
                    functionCallExpr.getFunctionName().equalsIgnoreCase(FunctionSet.STR2DATE)) {
                return SyncPartitionUtils.getRangePartitionDiffOfExpr(basePartitionMap,
                        mvPartitionMap, functionCallExpr, differ);
            } else {
                throw new SemanticException("Materialized view partition function " +
                        functionCallExpr.getFunctionName() +
                        " is not supported yet.", functionCallExpr.getPos());
            }
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition expr:" + partitionExpr);
        }
    }

    public static String getPartitionName(String basePath, String partitionPath) {
        String basePathWithSlash = getPathWithSlash(basePath);
        String partitionPathWithSlash = getPathWithSlash(partitionPath);

        if (basePathWithSlash.equals(partitionPathWithSlash)) {
            return "";
        }

        Preconditions.checkState(partitionPath.startsWith(basePathWithSlash),
                "Can't infer partition name. base path: %s, partition path: %s", basePath, partitionPath);

        partitionPath = partitionPath.endsWith("/") ? partitionPath.substring(0, partitionPath.length() - 1) : partitionPath;
        return partitionPath.substring(basePathWithSlash.length());
    }

    public static String getPathWithSlash(String path) {
        return path.endsWith("/") ? path : path + "/";
    }

    /**
     * Get filtered partition keys based on partition predicate using Hive partition pruning
     * Reuses key logic from OptExternalPartitionPruner
     *
     * @param context         The ConnectContext
     * @param sourceHiveTable The source Hive table
     * @return List of filtered partition keys, or null if no filtering is needed
     */
    public static List<PartitionKey> getFilteredPartitionKeys(ConnectContext context,
                                                              com.starrocks.catalog.Table sourceHiveTable,
                                                              Expr partitionFilter) {
        try {
            List<Column> partitionColumns = sourceHiveTable.getPartitionColumns();

            if (partitionColumns.isEmpty()) {
                LOG.info("Source table is not partitioned, partition filter will be ignored");
                return null;
            }

            // Create column reference mappings for reusing OptExternalPartitionPruner logic
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            Map<Column, ColumnRefOperator> columnToColRefMap = new HashMap<>();
            Map<ColumnRefOperator, Column> colRefToColumnMap = new HashMap<>();

            // Create a simple mock operator that provides the necessary interface
            LogicalHiveScanOperator hiveScanOperator = makeSourceTableHiveScanOperator(sourceHiveTable, columnToColRefMap,
                    colRefToColumnMap, columnRefFactory, partitionFilter, context);

            OptExternalPartitionPruner.prunePartitions(OptimizerFactory.initContext(context, columnRefFactory),
                    hiveScanOperator);

            ScanOperatorPredicates scanOperatorPredicates = hiveScanOperator.getScanOperatorPredicates();
            if (!scanOperatorPredicates.getNonPartitionConjuncts().isEmpty() ||
                    !scanOperatorPredicates.getNoEvalPartitionConjuncts().isEmpty()) {
                LOG.warn("Partition filter contains non-partition predicates or can not eval predicates, " +
                                "non-partition predicates: {}, no-eval partition predicates: {}. ",
                        Joiner.on(", ").join(scanOperatorPredicates.getNonPartitionConjuncts()),
                        Joiner.on(", ").join(scanOperatorPredicates.getNoEvalPartitionConjuncts()));
                throw new StarRocksConnectorException("Partition filter contains non-partition predicates or can not eval " +
                        "predicates, only simple partition predicates are supported for partition pruning. " +
                        "Non-partition predicates: %s, no-eval partition predicates: %s",
                        Joiner.on(", ").join(scanOperatorPredicates.getNonPartitionConjuncts()),
                        Joiner.on(", ").join(scanOperatorPredicates.getNoEvalPartitionConjuncts()));
            }

            List<PartitionKey> partitionKeys = Lists.newArrayList();
            scanOperatorPredicates.getSelectedPartitionIds().stream()
                    .map(id -> scanOperatorPredicates.getIdToPartitionKey().get(id))
                    .filter(Objects::nonNull)
                    .forEach(partitionKeys::add);
            LOG.info("Partition pruning selected {} partitions, select partitions : {}", partitionKeys.size(),
                    Joiner.on(", ").join(partitionKeys));

            return partitionKeys;
        } catch (Exception e) {
            LOG.warn("Failed to perform partition pruning, Error: {}", e.getMessage());
            throw new StarRocksConnectorException("Failed to perform partition pruning: %s", e.getMessage(), e);
        }
    }

    private static LogicalHiveScanOperator makeSourceTableHiveScanOperator(com.starrocks.catalog.Table sourceTable,
                                                                           Map<Column, ColumnRefOperator> columnToColRefMap,
                                                                           Map<ColumnRefOperator, Column> colRefToColumnMap,
                                                                           ColumnRefFactory columnRefFactory,
                                                                           Expr whereExpr, ConnectContext context) {
        List<ColumnRefOperator> columnRefOperators = new ArrayList<>();
        for (Column column : sourceTable.getBaseSchema()) {
            ColumnRefOperator columnRef = columnRefFactory.create(column.getName(),
                    column.getType(), column.isAllowNull());
            colRefToColumnMap.put(columnRef, column);
            columnToColRefMap.put(column, columnRef);
            columnRefOperators.add(columnRef);
        }

        ScalarOperator scalarOperator = null;
        if (whereExpr != null) {
            // Create scope with table columns for expression analysis
            TableName sourceTableName = new TableName(sourceTable.getCatalogName(),
                    sourceTable.getCatalogDBName(), sourceTable.getCatalogTableName());
            Scope scope = new Scope(RelationId.anonymous(), new RelationFields(columnRefOperators.stream()
                    .map(col -> new Field(col.getName(), col.getType(), sourceTableName, null))
                    .collect(Collectors.toList())));
            // Analyze the WHERE expression
            ExpressionAnalyzer.analyzeExpression(whereExpr, new AnalyzeState(), scope, context);

            // Create expression mapping for conversion
            ExpressionMapping expressionMapping = new ExpressionMapping(scope, columnRefOperators);

            // Convert Expr to ScalarOperator
            scalarOperator = SqlToScalarOperatorTranslator.translate(whereExpr, expressionMapping, columnRefFactory);
        }
        return new LogicalHiveScanOperator(sourceTable, colRefToColumnMap, columnToColRefMap,
                Operator.DEFAULT_LIMIT, scalarOperator);
    }
}
