// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakePartitionKey;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiPartitionKey;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.DmlException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
                String.format("columns size is %d, but values size is %d", columns.size(), values.size()));

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
            if (((NullablePartitionKey) partitionKey).nullPartitionValue().equals(rawValue)) {
                exprValue = NullLiteral.create(type);
            } else {
                exprValue = LiteralExpr.create(rawValue, type);
            }
            partitionKey.pushColumn(exprValue, type.getPrimitiveType());
        }
        return partitionKey;
    }

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
                values.add(((NullablePartitionKey) key).nullPartitionValue());
            } else if (value instanceof BoolLiteral) {
                BoolLiteral boolValue = ((BoolLiteral) value);
                values.add(String.valueOf(boolValue.getValue()));
            } else {
                values.add(value.getStringValue());
            }
        }
        return values;
    }

    public static List<String> getPartitionNames(Table table) {
        List<String> partitionNames = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            if (hmsTable.isUnPartitioned()) {
                // return table name if table is unpartitioned
                return Lists.newArrayList(hmsTable.getTableName());
            }
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
        } else if (table.isIcebergTable()) {
            IcebergTable icebergTable = (IcebergTable) table;
            if (icebergTable.isUnPartitioned()) {
                // return table name if table is unpartitioned
                return Lists.newArrayList(icebergTable.getTable());
            }
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalog(), icebergTable.getDb(), icebergTable.getTable());
        } else {
            Preconditions.checkState(false, "Do not support get partition names and columns for" +
                    "table type %s", table.getType());
        }
        return partitionNames;
    }

    public static List<Column> getPartitionColumns(Table table) {
        List<Column> partitionColumns = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            partitionColumns = hmsTable.getPartitionColumns();
        } else if (table.isIcebergTable()) {
            IcebergTable icebergTable = (IcebergTable) table;
            partitionColumns = icebergTable.getPartitionColumns();
        } else {
            Preconditions.checkState(false, "Do not support get partition names and columns for" +
                    "table type %s", table.getType());
        }
        return partitionColumns;
    }

    public static Map<String, Range<PartitionKey>> getPartitionRange(Table table, Column partitionColumn)
            throws UserException {
        if (table.isLocalTable()) {
            return ((OlapTable) table).getRangePartitionMap();
        } else if (table.isHiveTable() || table.isHudiTable() || table.isIcebergTable()) {
            return PartitionUtil.getMVPartitionNameWithRange(table, partitionColumn, getPartitionNames(table));
        } else {
            throw new DmlException("Can not get partition range from table with type : %s", table.getType());
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

    private static String generateMVPartitionName(PartitionKey partitionKey) {
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
        } else {
            LOG.warn("Only support get partition for hive table type");
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
    // it need to generate a legal partition name for mv like 'p20230101' when mv
    // based on partitioned external table.
    public static Set<String> getMVPartitionName(Table table, Column partitionColumn, List<String> partitionNames)
            throws AnalysisException {
        return getMVPartitionNameWithRange(table, partitionColumn, partitionNames).keySet();
    }

    // Map partition values to partition ranges, eg
    // [NULL,1992-01-01,1992-01-02,1992-01-03]
    //               ||
    //               \/
    // [0000-01-01, 1992-01-01),[1992-01-01, 1992-01-02),[1992-01-02, 1992-01-03),[1993-01-03, MAX_VALUE)
    public static Map<String, Range<PartitionKey>> getMVPartitionNameWithRange(Table table,
                                                                               Column partitionColumn,
                                                                               List<String> partitionNames)
            throws AnalysisException {
        Map<String, Range<PartitionKey>> partitionRangeMap = new LinkedHashMap<>();
        List<Column> partitionColumns = getPartitionColumns(table);

        // Get the index of partitionColumn when table has multi partition columns.
        int partitionColumnIndex = checkAndGetPartitionColumnIndex(partitionColumns, partitionColumn);

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (String partitionName : partitionNames) {
            PartitionKey partitionKey = createPartitionKey(
                    ImmutableList.of(toPartitionValues(partitionName).get(partitionColumnIndex)),
                    ImmutableList.of(partitionColumns.get(partitionColumnIndex)),
                    table.getType());
            partitionKeys.add(partitionKey);
        }

        Map<String, PartitionKey> partitionMap = Maps.newHashMap();
        for (PartitionKey partitionKey : partitionKeys) {
            String partitionName = generateMVPartitionName(partitionKey);
            partitionMap.put(partitionName, partitionKey);
        }
        LinkedHashMap<String, PartitionKey> sortedPartitionLinkMap = partitionMap.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        int index = 0;
        PartitionKey lastPartitionKey = null;
        String lastPartitionName = null;
        for (Map.Entry<String, PartitionKey> entry : sortedPartitionLinkMap.entrySet()) {
            if (index == 0) {
                lastPartitionName = entry.getKey();
                lastPartitionKey = entry.getValue();
                if (lastPartitionKey.getKeys().get(0).isNullable()) {
                    // If partition key is NULL literal, rewrite it to min value.
                    lastPartitionKey = PartitionKey.createInfinityPartitionKey(ImmutableList.of(partitionColumn), false);
                }
                ++index;
                continue;
            }
            partitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, entry.getValue()));
            lastPartitionName = entry.getKey();
            lastPartitionKey = entry.getValue();
        }
        if (lastPartitionName != null) {
            PartitionKey endKey = new PartitionKey();
            endKey.pushColumn(addOffsetForLiteral(lastPartitionKey.getKeys().get(0), 1),
                    partitionColumn.getPrimitiveType());

            partitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, endKey));
        }
        return partitionRangeMap;
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
}
