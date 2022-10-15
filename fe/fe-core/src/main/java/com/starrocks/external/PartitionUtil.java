// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HudiPartitionKey;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class PartitionUtil {
    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
        return createPartitionKey(values, columns, false);
    }

    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns,
                                                  boolean isHudiTable) throws AnalysisException {
        Preconditions.checkState(values.size() == columns.size(),
                String.format("columns size is %d, but values size is %d", columns.size(), values.size()));

        PartitionKey partitionKey = isHudiTable ? new HudiPartitionKey() : new HivePartitionKey();

        // change string value to LiteralExpr,
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
            Type type = columns.get(i).getType();
            LiteralExpr exprValue;
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

    // Map partition values to partition ranges, eg
    // [NULL,1992-01-01,1992-01-02,1992-01-03]
    //               ||
    //               \/
    // [0000-01-01, 1992-01-01),[1992-01-01, 1992-01-02),[1992-01-02, 1992-01-03),[1993-01-03, 9999-12-31)
    public static Map<String, Range<PartitionKey>> getPartitionRange(Table table, Column partitionColumn)
            throws AnalysisException {
        Map<String, Range<PartitionKey>> partitionRangeMap = new LinkedHashMap<>();
        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
        List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());

        List<Column> partitionColumns = hmsTable.getPartitionColumns();

        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (String partitionName : partitionNames) {
            PartitionKey partitionKey = createPartitionKey(toPartitionValues(partitionName),
                    partitionColumns, table instanceof HudiTable);
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
                    PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(
                                    LiteralExpr.createMaxValue(partitionColumn.getType()).getStringValue())),
                            ImmutableList.of(partitionColumn))));
        }
        return partitionRangeMap;
    }

    public static String getSuffixName(String dirPath, String filePath) {
        if (!FeConstants.runningUnitTest) {
            Preconditions.checkArgument(filePath.startsWith(dirPath),
                    "dirPath " + dirPath + " should be prefix of filePath " + filePath);
        }

        String name = filePath.replaceFirst(dirPath, "");
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
