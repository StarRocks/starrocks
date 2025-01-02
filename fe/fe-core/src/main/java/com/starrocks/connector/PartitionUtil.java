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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergApiConverter.PARTITION_NULL_VALUE;
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

    /**
     * Return table's partition name to partition key range's mapping:
     * - for native base table, just return its range partition map;
     * - for external base table, convert external partition to normalized partition.
     *
     * @param table           : the ref base table of materialized view
     * @param partitionColumn : the ref base table's partition column which mv's partition derives
     */
    public static Map<String, Range<PartitionKey>> getPartitionKeyRange(Table table, Column partitionColumn, Expr partitionExpr)
            throws UserException {
        return ConnectorPartitionTraits.build(table).getPartitionKeyRange(partitionColumn, partitionExpr);
    }

    /**
     * NOTE: Ensure result list cell's order is the same with partitionColumns.
     */
    public static Map<String, PListCell> getPartitionList(Table table, List<Column> partitionColumns)
            throws UserException {
        return ConnectorPartitionTraits.build(table).getPartitionList(partitionColumns);
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

    /**
     * NOTE:
     * this method may generate the same partition name:
     * partitionName1 : par_col=0/par_date=2020-01-01 => p0_20200101
     * partitionName2 : par_col=1/par_date=2020-01-01 => p1_20200101
     */
    public static String generateMVPartitionName(PartitionKey partitionKey) {
        StringBuilder sb = new StringBuilder("p");
        List<String> partitionNames = partitionKey.getKeys()
                .stream()
                .map(literalExpr -> generateLiteralPartitionName(literalExpr))
                .collect(Collectors.toList());
        sb.append(Joiner.on("_").join(partitionNames));
        return sb.toString();
    }

    private static String generateLiteralPartitionName(LiteralExpr literalExpr) {
        return literalExpr.getStringValue().replaceAll("[^a-zA-Z0-9_]*", "");
    }

    public static Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table table) {
        return ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo();
    }

    public static Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table table, List<String> partitionNames) {
        return ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo(partitionNames);
    }

    // Get partition name generated for mv from hive/hudi/iceberg partition name,
    // external table partition name like this :
    // col_date=2023-01-01,
    // it needs to generate a legal partition name for mv like 'p20230101' when mv
    // based on partitioned external table.
    public static Set<String> getMVPartitionName(Table table, Column partitionColumn,
                                                 List<String> partitionNames,
                                                 boolean isListPartition,
                                                 Expr partitionExpr) throws AnalysisException {

        if (isListPartition) {
            Map<String, PListCell> partitionNameWithList = getMVPartitionNameWithList(table, ImmutableList.of(partitionColumn),
                    partitionNames);
            return Sets.newHashSet(partitionNameWithList.keySet());
        } else {
            Map<String, Range<PartitionKey>> partitionNameWithRange = getMVPartitionNameWithRange(table, partitionColumn,
                    partitionNames, partitionExpr);
            return Sets.newHashSet(partitionNameWithRange.keySet());
        }
    }

    public static Map<String, Range<PartitionKey>> getMVPartitionNameWithRange(Table table, Column partitionColumn,
                                                                               List<String> partitionNames,
                                                                               Expr partitionExpr) throws AnalysisException {
        if (table.isHiveTable() || table.isHudiTable() || table.isIcebergTable() || table.isPaimonTable()) {
            return getRangePartitionMapOfExternalTable(table, partitionColumn, partitionNames, partitionExpr);
        } else if (table.isJDBCTable()) {
            return PartitionUtil.getRangePartitionMapOfJDBCTable(table, partitionColumn, partitionNames, partitionExpr);
        } else {
            throw new DmlException("Can not get partition range from table with type : %s", table.getType());
        }
    }

    /**
     *  NOTE:
     *  External tables may contain multi partition columns, so the same mv partition name may contain multi external
     *  table partitions. eg:
     *   partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     *   partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    public static Map<String, Set<String>> getMVPartitionNameMapOfExternalTable(Table table,
                                                                                List<Column> refPartitionColumns,
                                                                                List<String> partitionNames)
            throws AnalysisException {
        List<Column> partitionColumns = getPartitionColumns(table);
        // Get the index of partitionColumn when table has multi partition columns.
        List<Integer> partitionColumnIdxes = Lists.newArrayList();
        for (Column refPartitionColumn : refPartitionColumns) {
            partitionColumnIdxes.add(checkAndGetPartitionColumnIndex(partitionColumns, refPartitionColumn));
        }

        Map<String, Set<String>> mvPartitionKeySetMap = Maps.newHashMap();
        if (table.isJDBCTable()) {
            for (String partitionName : partitionNames) {
                PartitionKey partitionKey = createPartitionKey(
                        partitionColumnIdxes.stream()
                                .map(p -> getPartitionNameForJDBCTable(partitionColumns.get(p), partitionName))
                                .collect(Collectors.toList()),
                        partitionColumnIdxes.stream().map(partitionColumns::get).collect(Collectors.toList()),
                        table);
                String mvPartitionName = generateMVPartitionName(partitionKey);
                mvPartitionKeySetMap.computeIfAbsent(mvPartitionName, x -> Sets.newHashSet())
                        .add(partitionName);
            }
        } else {
            for (String partitionName : partitionNames) {
                List<String> partitionNameValues = toPartitionValues(partitionName);
                PartitionKey partitionKey = createPartitionKey(
                        partitionColumnIdxes.stream().map(partitionNameValues::get).collect(Collectors.toList()),
                        partitionColumnIdxes.stream().map(partitionColumns::get).collect(Collectors.toList()),
                        table);
                String mvPartitionName = generateMVPartitionName(partitionKey);
                mvPartitionKeySetMap.computeIfAbsent(mvPartitionName, x -> Sets.newHashSet())
                        .add(partitionName);
            }
        }
        return mvPartitionKeySetMap;
    }

    // Iceberg Table has partition transforms like this:
    // partition column is (ts timestamp), table could partition by year(ts), month(ts), day(ts), hour(ts)
    // this function could get the dateInterval from partition transform
    public static DateTimeInterval getDateTimeInterval(Table table, Column partitionColumn) {
        if (partitionColumn.getType() != Type.DATE && partitionColumn.getType() != Type.DATETIME) {
            return DateTimeInterval.NONE;
        }
        if (table.isIcebergTable()) {
            return IcebergPartitionUtils.getDateTimeIntervalFromIceberg((IcebergTable) table, partitionColumn);
        } else {
            // add 1 day as default interval
            return DateTimeInterval.DAY;
        }
    }

    private static String getPartitionNameForJDBCTable(Column partitionColumn, String partitionName) {
        if (partitionName.equalsIgnoreCase(MYSQL_PARTITION_MAXVALUE)) {
            // For the special handling of maxvalue, take the maximum value for int type and 9999-12-31 for date,
            // For varchar and char, due to the need to convert them to time partitions using str2date, they are also taken as 9999-12-31
            if (partitionColumn.getPrimitiveType().isIntegerType()) {
                partitionName = IntLiteral.createMaxValue(partitionColumn.getType()).getStringValue();
            } else {
                partitionName = DateLiteral.createMaxValue(Type.DATE).getStringValue();
            }
        }
        return partitionName;
    }

    /**
     * Map partition values to partition ranges, eg:
     * [NULL,1992-01-01,1992-01-02,1992-01-03]
     * ||
     * \/
     * [0000-01-01,1992-01-01),[1992-01-01, 1992-01-02),[1992-01-02, 1992-01-03),[1993-01-03, 1993-01-04)
     * <p>
     * NOTE:
     * External tables may contain multi partition columns, so the same mv partition name may contain multi external
     * table partitions. eg:
     * partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     * partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    public static Map<String, Range<PartitionKey>> getRangePartitionMapOfExternalTable(Table table,
                                                                                       Column partitionColumn,
                                                                                       List<String> partitionNames,
                                                                                       Expr partitionExpr)
            throws AnalysisException {
        List<Column> partitionColumns = getPartitionColumns(table);

        // Get the index of partitionColumn when table has multi partition columns.
        int partitionColumnIndex = checkAndGetPartitionColumnIndex(partitionColumns, partitionColumn);
        List<PartitionKey> partitionKeys = new ArrayList<>();
        Map<String, PartitionKey> mvPartitionKeyMap = Maps.newHashMap();

        for (String partitionName : partitionNames) {
            List<String> partitionNameValues = toPartitionValues(partitionName);
            putMvPartitionKeyIntoMap(table, partitionColumns.get(partitionColumnIndex), partitionKeys,
                    mvPartitionKeyMap, partitionNameValues.get(partitionColumnIndex));
        }

        LinkedHashMap<String, PartitionKey> sortedPartitionLinkMap = mvPartitionKeyMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        boolean isConvertToDate = isConvertToDate(partitionExpr, partitionColumn);
        Map<String, Range<PartitionKey>> mvPartitionRangeMap = new LinkedHashMap<>();

        PrimitiveType partitionColPrimType = partitionColumn.getPrimitiveType();
        DateTimeInterval partitionDateTimeInterval = getDateTimeInterval(table, partitionColumn);

        // NOTE: For external table, convert partition values to partition ranges just like list partition.
        // Each partition's lower bound is the partition value itself, and the upper bound is the next partition value.
        // This is more robust, and it's better to handle the case where the partition value is not continuous.
        for (Map.Entry<String, PartitionKey> entry : sortedPartitionLinkMap.entrySet()) {
            String partitionKeyName = entry.getKey();
            PartitionKey lowerBound = entry.getValue();
            if (lowerBound.getKeys().get(0).isNullable()) {
                // If partition key is NULL literal, rewrite it to min value.
                lowerBound = PartitionKey.createInfinityPartitionKeyWithType(
                        ImmutableList.of(partitionColumn.getPrimitiveType()), false);
            }
            Preconditions.checkState(!mvPartitionRangeMap.containsKey(partitionKeyName));
            PartitionKey upperBound = nextPartitionKey(lowerBound, partitionDateTimeInterval, partitionColPrimType,
                    isConvertToDate);
            mvPartitionRangeMap.put(partitionKeyName, Range.closedOpen(lowerBound, upperBound));
        }
        return mvPartitionRangeMap;
    }

    public static PartitionKey nextPartitionKey(PartitionKey lastPartitionKey,
                                                DateTimeInterval dateTimeInterval,
                                                PrimitiveType partitionColPrimType,
                                                boolean isStringConvertToDate) throws AnalysisException {
        LiteralExpr literalExpr;
        if (isStringConvertToDate) {
            PartitionKey lastDate = convertToDate(lastPartitionKey);
            String lastDateFormat = lastPartitionKey.getKeys().get(0).getStringValue();
            DateTimeFormatter formatter = DateUtils.probeFormat(lastDateFormat);
            DateLiteral nextDate = (DateLiteral) addOffsetForLiteral(lastDate.getKeys().get(0), 1,
                    dateTimeInterval);
            literalExpr = new StringLiteral(nextDate.toLocalDateTime().format(formatter));
        } else {
            literalExpr = addOffsetForLiteral(lastPartitionKey.getKeys().get(0), 1, dateTimeInterval);
        }
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(literalExpr, partitionColPrimType);
        return partitionKey;
    }

    /**
     * Map partition values to partition ranges, eg:
     * [1992-01-01,1992-01-02,1992-01-03]
     * ||
     * \/
     * (0000-01-01, 1992-01-01],(1992-01-01, 1992-01-02], (1992-01-02, 1992-01-03]
     * <p>
     * NOTE:
     * External tables may contain multi partition columns, so the same mv partition name may contain multi external
     * table partitions. eg:
     * partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     * partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    public static Map<String, Range<PartitionKey>> getRangePartitionMapOfJDBCTable(Table table,
                                                                                   Column partitionColumn,
                                                                                   List<String> partitionNames,
                                                                                   Expr partitionExpr)
            throws AnalysisException {

        // Determine if it is the str2date function
        boolean isConvertToDate = isConvertToDate(partitionExpr, partitionColumn);
        List<PartitionKey> partitionKeys = new ArrayList<>();
        Map<String, PartitionKey> mvPartitionKeyMap = Maps.newHashMap();
        for (String partitionName : partitionNames) {
            partitionName = getPartitionNameForJDBCTable(partitionColumn, partitionName);
            putMvPartitionKeyIntoMap(table, partitionColumn, partitionKeys, mvPartitionKeyMap, partitionName);
        }

        LinkedHashMap<String, PartitionKey> sortedPartitionLinkMap = mvPartitionKeyMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        PartitionKey lastPartitionKey = null;
        String partitionName = null;

        Map<String, Range<PartitionKey>> mvPartitionRangeMap = new LinkedHashMap<>();
        for (Map.Entry<String, PartitionKey> entry : sortedPartitionLinkMap.entrySet()) {
            // Adapt to the range partitioning method of JDBC Table, the partitionName adopts the name of upperBound
            partitionName = entry.getKey();
            if (lastPartitionKey == null) {
                lastPartitionKey = entry.getValue();
                if (!lastPartitionKey.getKeys().get(0).isMinValue()) {
                    // If partition key is not min value, rewrite it to min value.
                    lastPartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                            ImmutableList.of(isConvertToDate ?
                                    PrimitiveType.DATE : partitionColumn.getPrimitiveType()), false);
                } else {
                    if (lastPartitionKey.getKeys().get(0).isNullable()) {
                        // If partition key is NULL literal, rewrite it to min value.
                        lastPartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                                ImmutableList.of(isConvertToDate ?
                                        PrimitiveType.DATE : partitionColumn.getPrimitiveType()), false);
                    } else {
                        lastPartitionKey = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
                    }
                    continue;
                }
            }
            PartitionKey upperBound = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
            putRangeToMvPartitionRangeMapForJDBCTable(mvPartitionRangeMap, partitionName, lastPartitionKey, upperBound);
            lastPartitionKey = upperBound;
        }

        return mvPartitionRangeMap;
    }

    private static void putRangeToMvPartitionRangeMapForJDBCTable(Map<String, Range<PartitionKey>> mvPartitionRangeMap,
                                                                  String partitionName,
                                                                  PartitionKey lastPartitionKey,
                                                                  PartitionKey upperBound) {
        Preconditions.checkState(!mvPartitionRangeMap.containsKey(partitionName));
        mvPartitionRangeMap.put(partitionName, Range.openClosed(lastPartitionKey, upperBound));
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
            return NullLiteral.create(Type.DATE);
        }
        try {
            String dateLiteral = stringLiteral.getStringValue();
            LocalDateTime dateValue = DateUtils.parseStrictDateTime(dateLiteral);
            return new DateLiteral(dateValue, Type.DATE);
        } catch (Exception e) {
            throw new SemanticException("create string to date literal failed:" +  stringLiteral.getStringValue(), e);
        }
    }

    /**
     * Convert a string type partition key to date type partition key.
     * @param partitionKey : input string partition key to convert.
     * @return             : partition key with date type if input can be converted.
     */
    private static PartitionKey convertToDate(PartitionKey partitionKey) throws SemanticException {
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

    private static void putMvPartitionKeyIntoMap(Table table, Column partitionColumn, List<PartitionKey> partitionKeys,
                                                 Map<String, PartitionKey> mvPartitionKeyMap, String partitionValue)
            throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                ImmutableList.of(partitionValue),
                ImmutableList.of(partitionColumn),
                table);
        partitionKeys.add(partitionKey);
        String mvPartitionName = generateMVPartitionName(partitionKey);
        // TODO: check `mvPartitionName` existed.
        mvPartitionKeyMap.put(mvPartitionName, partitionKey);
    }

    /**
     * NOTE: Ensure output plist cell's order is the same with partitionColumns.
     */
    public static Map<String, PListCell> getMVPartitionNameWithList(Table table,
                                                                    List<Column> refPartitionColumns,
                                                                    List<String> partitionNames)
            throws AnalysisException {
        Map<String, PListCell> partitionListMap = new LinkedHashMap<>();
        List<Column> partitionColumns = getPartitionColumns(table);

        // Get the index of partitionColumn when table has multi partition columns.
        List<Integer> partitionColumnIdxes = Lists.newArrayList();
        for (Column refPartitionColumn : refPartitionColumns) {
            partitionColumnIdxes.add(checkAndGetPartitionColumnIndex(partitionColumns, refPartitionColumn));
        }

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (String partitionName : partitionNames) {
            List<String> partitionNameValues = toPartitionValues(partitionName);
            PartitionKey partitionKey = createPartitionKey(
                    partitionColumnIdxes.stream().map(partitionNameValues::get).collect(Collectors.toList()),
                    partitionColumnIdxes.stream().map(partitionColumns::get).collect(Collectors.toList()),
                    table);
            partitionKeys.add(partitionKey);
            String mvPartitionName = generateMVPartitionName(partitionKey);
            List<List<String>> partitionKeyList = generateMVPartitionList(partitionKey);
            partitionListMap.put(mvPartitionName, new PListCell(partitionKeyList));
        }
        return partitionListMap;
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

    private static List<List<String>> generateMVPartitionList(PartitionKey partitionKey) {
        List<List<String>> partitionKeyList = Lists.newArrayList();
        List<String> partitionItem = Lists.newArrayList();
        for (LiteralExpr key : partitionKey.getKeys()) {
            partitionItem.add(key.getStringValue());
        }
        partitionKeyList.add(partitionItem);
        return partitionKeyList;
    }

    // return partition name in forms of `col1=value1/col2=value2`
    // if the partition field is explicitly named, use this name without change
    // if the partition field is not identity transform, column name is appended by its transform name (e.g. col1_hour)
    // if all partition fields are no longer active (dropped by partition evolution), return "ICEBERG_DEFAULT_PARTITION"
    public static String convertIcebergPartitionToPartitionName(PartitionSpec partitionSpec, StructLike partition) {
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < partitionSpec.fields().size(); ++index) {
            PartitionField partitionField = partitionSpec.fields().get(index);
            // skip inactive partition field
            if (partitionField.transform().isVoid()) {
                continue;
            }
            org.apache.iceberg.types.Type type = partitionSpec.partitionType().fieldType(partitionField.name());
            sb.append(partitionField.name());
            sb.append("=");
            String value = partitionField.transform().toHumanString(type, getPartitionValue(partition, index,
                    partitionSpec.javaClasses()[index]));
            sb.append(value);
            sb.append("/");
        }

        if (sb.length() > 0) {
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
            String value = partitionField.transform().toHumanString(getPartitionValue(partitionData, i, clazz));

            // currently starrocks date literal only support local datetime
            org.apache.iceberg.types.Type icebergType = spec.schema().findType(partitionField.sourceId());
            if (!value.equals(PARTITION_NULL_VALUE) && partitionField.transform().isIdentity() &&
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

    public static RangePartitionDiff getPartitionDiff(Expr partitionExpr,
                                                      Map<String, Range<PartitionKey>> basePartitionMap,
                                                      Map<String, Range<PartitionKey>> mvPartitionMap,
                                                      RangePartitionDiffer differ) {
        if (partitionExpr instanceof SlotRef) {
            return SyncPartitionUtils.getRangePartitionDiffOfSlotRef(basePartitionMap, mvPartitionMap, differ);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC) ||
                    functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
                return SyncPartitionUtils.getRangePartitionDiffOfExpr(basePartitionMap,
                        mvPartitionMap, functionCallExpr, differ);
            } else {
                throw new SemanticException("Materialized view partition function " +
                        functionCallExpr.getFnName().getFunction() +
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
}
