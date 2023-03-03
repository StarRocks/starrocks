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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakePartitionKey;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
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
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.DmlException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.FileUtils.escapePathName;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class PartitionUtil {
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

    public static Pair<List<String>, List<Column>> getPartitionNamesAndColumns(Table table) {
        List<String> partitionNames = null;
        List<Column> partitionColumns = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
            partitionColumns = hmsTable.getPartitionColumns();
            return new Pair<>(partitionNames, partitionColumns);
        } else if (table.isIcebergTable()) {
            IcebergTable icebergTable = (IcebergTable) table;
            partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalogName(), icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName());
            partitionColumns = icebergTable.getPartitionColumns();
        } else {
            Preconditions.checkState(false, "Do not support get partition names and columns for" +
                    "table type %s", table.getType());
        }
        return Pair.create(partitionNames, partitionColumns);
    }

    public static Map<String, Range<PartitionKey>> getPartitionRange(Table table, Column partitionColumn)
            throws UserException {
        if (table.isNativeTable()) {
            return ((OlapTable) table).getRangePartitionMap();
        } else if (table.isHiveTable() || table.isHudiTable() || table.isIcebergTable()) {
            return PartitionUtil.getPartitionRangeFromPartitionList(table, partitionColumn);
        } else {
            throw new DmlException("Can not get partition range from table with type : %s", table.getType());
        }
    }

    // Map partition values to partition ranges, eg
    // [NULL,1992-01-01,1992-01-02,1992-01-03]
    //               ||
    //               \/
    // [0000-01-01, 1992-01-01),[1992-01-01, 1992-01-02),[1992-01-02, 1992-01-03),[1993-01-03, MAX_VALUE)
    public static Map<String, Range<PartitionKey>> getPartitionRangeFromPartitionList(Table table,
                                                                                      Column partitionColumn)
            throws AnalysisException {
        Map<String, Range<PartitionKey>> partitionRangeMap = new LinkedHashMap<>();
        Pair<List<String>, List<Column>> partitionPair = getPartitionNamesAndColumns(table);
        List<String> partitionNames = partitionPair.first;
        List<Column> partitionColumns = partitionPair.second;

        // Get the index of partitionColumn when table has multi partition columns.
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
            String partitionName = "p" + partitionKey.getKeys().get(0).getStringValue();
            // generate legal partition name
            partitionName = partitionName.replaceAll("[^a-zA-Z0-9_]*", "");
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
            partitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey,
                    PartitionKey.createPartitionKey(ImmutableList.of(PartitionValue.MAX_VALUE),
                            ImmutableList.of(partitionColumn))));
        }
        return partitionRangeMap;
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

    public static <T> T getPartitionValue(StructLike partition, int position, Class<?> javaClass) {
        return partition.get(position, (Class<T>) javaClass);
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
