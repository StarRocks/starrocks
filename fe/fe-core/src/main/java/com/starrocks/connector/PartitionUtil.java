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

import com.google.common.base.Preconditions;
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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakePartitionKey;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiPartitionKey;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.JDBCPartitionKey;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonPartitionKey;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.DmlException;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
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

import static org.apache.hadoop.hive.common.FileUtils.escapePathName;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class PartitionUtil {
    private static final Logger LOG = LogManager.getLogger(PartitionUtil.class);

    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
        return createPartitionKey(values, columns, Table.TableType.HIVE);
    }

    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns,
                                                  Table.TableType tableType) throws AnalysisException {
        Preconditions.checkState(values.size() == columns.size(),
                "columns size is %s, but values size is %s", columns.size(), values.size());

        PartitionKey partitionKey = null;
        switch (tableType) {
            case HIVE:
                partitionKey = new HivePartitionKey();
                break;
            case HUDI:
                partitionKey = new HudiPartitionKey();
                break;
            case ICEBERG:
                partitionKey = new IcebergPartitionKey();
                break;
            case DELTALAKE:
                partitionKey = new DeltaLakePartitionKey();
                break;
            case JDBC:
                partitionKey = new JDBCPartitionKey();
                break;
            case PAIMON:
                partitionKey = new PaimonPartitionKey();
                break;
            default:
                Preconditions.checkState(false, "Do not support create partition key for " +
                        "table type %s", tableType);
        }

        // change string value to LiteralExpr,
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
            Type type = columns.get(i).getType();
            LiteralExpr exprValue;
            // rawValue could be null for delta table
            if (rawValue == null) {
                rawValue = "null";
            }
            if (((NullablePartitionKey) partitionKey).nullPartitionValueList().contains(rawValue)) {
                partitionKey.setNullPartitionValue(rawValue);
                exprValue = NullLiteral.create(type);
            } else {
                exprValue = LiteralExpr.create(rawValue, type);
            }
            partitionKey.pushColumn(exprValue, type.getPrimitiveType());
        }
        return partitionKey;
    }

    // If partitionName is `par_col=0/par_date=2020-01-01`, return ["0", "2020-01-01"]
    public static List<String> toPartitionValues(String partitionName) {
        // mimics Warehouse.makeValsFromName
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            resultBuilder.add(unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
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
        if (table.isUnPartitioned()) {
            return Lists.newArrayList(table.getName());
        }
        List<String> partitionNames = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
        } else if (table.isIcebergTable()) {
            IcebergTable icebergTable = (IcebergTable) table;
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalogName(), icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName());
        } else if (table.isJDBCTable()) {
            JDBCTable jdbcTable = (JDBCTable) table;
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    jdbcTable.getCatalogName(), jdbcTable.getDbName(), jdbcTable.getJdbcTable());
        } else if (table.isPaimonTable()) {
            PaimonTable paimonTable = (PaimonTable) table;
            if (paimonTable.isUnPartitioned()) {
                // return table name if table is unpartitioned
                return Lists.newArrayList(paimonTable.getTableName());
            }
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    paimonTable.getCatalogName(), paimonTable.getDbName(), paimonTable.getTableName());
        } else {
            Preconditions.checkState(false, "Do not support get partition names and columns for" +
                    "table type %s", table.getType());
        }
        return partitionNames;
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

    public static List<Column> getPartitionColumns(Table table) {
        List<Column> partitionColumns = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            partitionColumns = hmsTable.getPartitionColumns();
        } else if (table.isIcebergTable()) {
            IcebergTable icebergTable = (IcebergTable) table;
            partitionColumns = icebergTable.getPartitionColumns();
        } else if (table.isJDBCTable()) {
            JDBCTable jdbcTable = (JDBCTable) table;
            partitionColumns = jdbcTable.getPartitionColumns();
        } else if (table.isPaimonTable()) {
            PaimonTable paimonTable = (PaimonTable) table;
            partitionColumns = paimonTable.getPartitionColumns();
        } else {
            Preconditions.checkState(false, "Do not support get partition names and columns for" +
                    "table type %s", table.getType());
        }
        return partitionColumns;
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
     * @param table : the ref base table of materialized view
     * @param partitionColumn : the ref base table's partition column which mv's partition derives
     */
    public static Map<String, Range<PartitionKey>> getPartitionKeyRange(Table table, Column partitionColumn, Expr partitionExpr)
            throws UserException {
        if (table.isNativeTableOrMaterializedView()) {
            return ((OlapTable) table).getRangePartitionMap();
        } else if (table.isHiveTable() || table.isHudiTable() || table.isIcebergTable() || table.isJDBCTable() ||
                table.isPaimonTable()) {
            return PartitionUtil.getRangePartitionMapOfExternalTable(
                    table, partitionColumn, getPartitionNames(table), partitionExpr);
        } else {
            throw new DmlException("Can not get partition range from table with type : %s", table.getType());
        }
    }

    public static Map<String, List<List<String>>> getPartitionList(Table table, Column partitionColumn)
            throws UserException {
        if (table.isNativeTableOrMaterializedView()) {
            return ((OlapTable) table).getListPartitionMap();
        } else if (table.isHiveTable() || table.isHudiTable() || table.isIcebergTable() || table.isPaimonTable()) {
            return PartitionUtil.getMVPartitionNameWithList(table, partitionColumn, getPartitionNames(table));
        } else {
            throw new DmlException("Can not get partition list from table with type : %s", table.getType());
        }
    }

    // check the partitionColumn exist in the partitionColumns
    private static int checkAndGetPartitionColumnIndex(List<Column> partitionColumns, Column partitionColumn)
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
     *   partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     *   partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    public static String generateMVPartitionName(PartitionKey partitionKey) {
        String partitionName = "p" + partitionKey.getKeys().get(0).getStringValue();
        // generate legal partition name
        return partitionName.replaceAll("[^a-zA-Z0-9_]*", "");
    }

    public static Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(Table table) {
        Map<String, PartitionInfo> partitionNameWithPartition = Maps.newHashMap();
        List<String> partitionNames = getPartitionNames(table);

        List<PartitionInfo> partitions;
        if (table.isHiveTable()) {
            HiveTable hiveTable = (HiveTable) table;
            partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(hiveTable.getCatalogName(), table, partitionNames);
        } else if (table.isJDBCTable()) {
            JDBCTable jdbcTable = (JDBCTable) table;
            partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(jdbcTable.getCatalogName(), table, partitionNames);
        } else {
            LOG.warn("Only support get partition for hive table and jdbc table type");
            return null;
        }
        for (int index = 0; index < partitionNames.size(); ++index) {
            partitionNameWithPartition.put(partitionNames.get(index), partitions.get(index));
        }
        return partitionNameWithPartition;
    }

    // Get partition name generated for mv from hive/hudi/iceberg partition name,
    // external table partition name like this :
    // col_date=2023-01-01,
    // it needs to generate a legal partition name for mv like 'p20230101' when mv
    // based on partitioned external table.
    public static Set<String> getMVPartitionName(Table table, Column partitionColumn,
                                                 List<String> partitionNames,
                                                 boolean isListPartition,
                                                 Expr partitionExpr)
            throws AnalysisException {
        if (isListPartition) {
            return Sets.newHashSet(getMVPartitionNameWithList(table, partitionColumn, partitionNames).keySet());
        } else {
            return Sets.newHashSet(getRangePartitionMapOfExternalTable(
                    table, partitionColumn, partitionNames, partitionExpr).keySet());
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
                                                                                Column partitionColumn,
                                                                                List<String> partitionNames)
            throws AnalysisException {
        List<Column> partitionColumns = getPartitionColumns(table);
        // Get the index of partitionColumn when table has multi partition columns.
        int partitionColumnIndex = checkAndGetPartitionColumnIndex(partitionColumns, partitionColumn);
        Map<String, Set<String>> mvPartitionKeySetMap = Maps.newHashMap();
        if (table.isJDBCTable()) {
            for (String partitionName : partitionNames) {
                PartitionKey partitionKey = createPartitionKey(
                        ImmutableList.of(partitionName),
                        ImmutableList.of(partitionColumn),
                        table.getType());
                String mvPartitionName = generateMVPartitionName(partitionKey);
                mvPartitionKeySetMap.computeIfAbsent(mvPartitionName, x -> Sets.newHashSet())
                        .add(partitionName);
            }
        } else {
            for (String partitionName : partitionNames) {
                List<String> partitionNameValues = toPartitionValues(partitionName);
                PartitionKey partitionKey = createPartitionKey(
                        ImmutableList.of(partitionNameValues.get(partitionColumnIndex)),
                        ImmutableList.of(partitionColumns.get(partitionColumnIndex)),
                        table.getType());
                String mvPartitionName = generateMVPartitionName(partitionKey);
                mvPartitionKeySetMap.computeIfAbsent(mvPartitionName, x -> Sets.newHashSet())
                        .add(partitionName);
            }
        }
        return mvPartitionKeySetMap;
    }

    /**
     *  Map partition values to partition ranges, eg:
     *  [NULL,1992-01-01,1992-01-02,1992-01-03]
     *                  ||
     *                  \/
     *  [0000-01-01, 1992-01-01),[1992-01-01, 1992-01-02),[1992-01-02, 1992-01-03),[1993-01-03, MAX_VALUE)
     *
     *  NOTE:
     *  External tables may contain multi partition columns, so the same mv partition name may contain multi external
     *  table partitions. eg:
     *   partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     *   partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
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
        if (table.isJDBCTable()) {
            for (String partitionName : partitionNames) {
                putMvPartitionKeyIntoMap(table, partitionColumn, partitionKeys, mvPartitionKeyMap, partitionName);
            }
        } else {
            for (String partitionName : partitionNames) {
                List<String> partitionNameValues = toPartitionValues(partitionName);
                putMvPartitionKeyIntoMap(table, partitionColumns.get(partitionColumnIndex), partitionKeys,
                        mvPartitionKeyMap, partitionNameValues.get(partitionColumnIndex));
            }
        }

        LinkedHashMap<String, PartitionKey> sortedPartitionLinkMap = mvPartitionKeyMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        int index = 0;
        PartitionKey lastPartitionKey = null;
        String lastPartitionName = null;

        boolean isConvertToDate = isConvertToDate(partitionExpr, partitionColumn);
        Map<String, Range<PartitionKey>> mvPartitionRangeMap = new LinkedHashMap<>();
        for (Map.Entry<String, PartitionKey> entry : sortedPartitionLinkMap.entrySet()) {
            if (index == 0) {
                lastPartitionName = entry.getKey();
                lastPartitionKey = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
                if (lastPartitionKey.getKeys().get(0).isNullable()) {
                    // If partition key is NULL literal, rewrite it to min value.
                    lastPartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                            ImmutableList.of(isConvertToDate ? PrimitiveType.DATE : partitionColumn.getPrimitiveType()), false);
                }
                ++index;
                continue;
            }
            Preconditions.checkState(!mvPartitionRangeMap.containsKey(lastPartitionName));
            PartitionKey upperBound = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
            mvPartitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, upperBound));
            lastPartitionName = entry.getKey();
            lastPartitionKey = upperBound;
        }
        if (lastPartitionName != null) {
            PartitionKey endKey = new PartitionKey();
            endKey.pushColumn(addOffsetForLiteral(lastPartitionKey.getKeys().get(0), 1),
                    isConvertToDate ? PrimitiveType.DATE : partitionColumn.getPrimitiveType());

            Preconditions.checkState(!mvPartitionRangeMap.containsKey(lastPartitionName));
            mvPartitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, endKey));
        }
        return mvPartitionRangeMap;
    }

    /**
     * If base table column type is string but partition type is date, we need to convert the string to date
     *
     * @param partitionExpr   PARTITION BY expr
     * @param partitionColumn PARTITION BY referenced column
     * @return
     */
    public static boolean isConvertToDate(Expr partitionExpr, Column partitionColumn) {
        if (!(partitionExpr instanceof FunctionCallExpr)) {
            return false;
        }
        PrimitiveType columnType = partitionColumn.getPrimitiveType();
        PrimitiveType partitionType = partitionExpr.getType().getPrimitiveType();
        return partitionType.isDateType() && !columnType.isDateType();
    }

    private static PartitionKey  convertToDate(PartitionKey partitionKey) {
        PartitionKey newPartitionKey = new PartitionKey();
        String dateLiteral = partitionKey.getKeys().get(0).getStringValue();
        LocalDateTime dateValue = DateUtils.parseStrictDateTime(dateLiteral);
        try {
            newPartitionKey.pushColumn(new DateLiteral(dateValue, Type.DATE), PrimitiveType.DATE);
            return newPartitionKey;
        } catch (AnalysisException e) {
            throw new SemanticException("create date string:{} failed:", partitionKey.getKeys().get(0).getStringValue(), e);
        }
    }

    private static void putMvPartitionKeyIntoMap(Table table, Column partitionColumn, List<PartitionKey> partitionKeys,
                                                 Map<String, PartitionKey> mvPartitionKeyMap, String partitionName)
            throws AnalysisException {
        PartitionKey partitionKey = createPartitionKey(
                ImmutableList.of(partitionName),
                ImmutableList.of(partitionColumn),
                table.getType());
        partitionKeys.add(partitionKey);
        String mvPartitionName = generateMVPartitionName(partitionKey);
        // TODO: check `mvPartitionName` existed.
        mvPartitionKeyMap.put(mvPartitionName, partitionKey);
    }

    public static Map<String, List<List<String>>> getMVPartitionNameWithList(Table table,
                                                                             Column partitionColumn,
                                                                             List<String> partitionNames)
            throws AnalysisException {
        Map<String, List<List<String>>> partitionListMap = new LinkedHashMap<>();
        List<Column> partitionColumns = getPartitionColumns(table);

        // Get the index of partitionColumn when table has multi partition columns.
        int partitionColumnIndex = checkAndGetPartitionColumnIndex(partitionColumns, partitionColumn);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (String partitionName : partitionNames) {
            List<String> partitionNameValues = toPartitionValues(partitionName);
            PartitionKey partitionKey = createPartitionKey(
                    ImmutableList.of(partitionNameValues.get(partitionColumnIndex)),
                    ImmutableList.of(partitionColumns.get(partitionColumnIndex)),
                    table.getType());
            partitionKeys.add(partitionKey);
            String mvPartitionName = generateMVPartitionName(partitionKey);
            List<List<String>> partitionKeyList = generateMVPartitionList(partitionKey);
            partitionListMap.put(mvPartitionName, partitionKeyList);
        }
        return partitionListMap;
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

    public static String convertIcebergPartitionToPartitionName(PartitionSpec partitionSpec, StructLike partition) {
        int filePartitionFields = partition.size();
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < filePartitionFields; ++index) {
            PartitionField partitionField = partitionSpec.fields().get(index);
            sb.append(partitionField.name());
            sb.append("=");
            String value = partitionField.transform().toHumanString(getPartitionValue(partition, index,
                    partitionSpec.javaClasses()[index]));
            sb.append(value);
            sb.append("/");
        }
        return sb.substring(0, sb.length() - 1);
    }

    public static List<String> getIcebergPartitionValues(PartitionSpec spec, StructLike partition) {
        PartitionData partitionData = (PartitionData) partition;
        List<String> partitionValues = new ArrayList<>();
        boolean existPartitionEvolution = spec.fields().stream().anyMatch(field -> field.transform().isVoid());
        for (int i = 0; i < spec.fields().size(); i++) {
            PartitionField partitionField = spec.fields().get(i);
            if ((!partitionField.transform().isIdentity() && existPartitionEvolution) || partitionData.get(i) == null) {
                continue;
            }

            Class<?> clazz = spec.javaClasses()[i];
            String value = partitionField.transform().toHumanString(getPartitionValue(partitionData, i, clazz));
            partitionValues.add(value);
        }

        return partitionValues;
    }

    public static <T> T getPartitionValue(StructLike partition, int position, Class<?> javaClass) {
        return partition.get(position, (Class<T>) javaClass);
    }

    public static LiteralExpr addOffsetForLiteral(LiteralExpr expr, int offset) throws AnalysisException {
        if (expr instanceof DateLiteral) {
            DateLiteral lowerDate = (DateLiteral) expr;
            return new DateLiteral(lowerDate.toLocalDateTime().plusDays(offset), Type.DATE);
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

    public static RangePartitionDiff getPartitionDiff(Expr partitionExpr, Column partitionColumn,
                                                      Map<String, Range<PartitionKey>> basePartitionMap,
                                                      Map<String, Range<PartitionKey>> mvPartitionMap) {
        if (partitionExpr instanceof SlotRef) {
            return SyncPartitionUtils.getRangePartitionDiffOfSlotRef(basePartitionMap, mvPartitionMap);
        } else if (partitionExpr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr;
            if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC) ||
                    functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
                return SyncPartitionUtils.getRangePartitionDiffOfExpr(basePartitionMap,
                        mvPartitionMap, functionCallExpr, null);
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
