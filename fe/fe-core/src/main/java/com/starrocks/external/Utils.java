// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.external.hive.HiveMetaClient;
import org.apache.hadoop.hive.common.StatsSetupConst;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class Utils {
    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
        return createPartitionKey(values, columns, false);
    }

    public static PartitionKey createPartitionKey(List<String> values, List<Column> columns,
                                                  boolean isHudiTable) throws AnalysisException {
        Preconditions.checkState(values.size() == columns.size(),
                String.format("columns size is %d, but values size is %d", columns.size(), values.size()));

        PartitionKey partitionKey = new PartitionKey();
        // change string value to LiteralExpr,
        // and transfer __HIVE_DEFAULT_PARTITION__ to NullLiteral
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
            Type type = columns.get(i).getType();
            LiteralExpr exprValue;
            if (HiveMetaClient.PARTITION_NULL_VALUE.equals(rawValue)) {
                exprValue = NullLiteral.create(type);
            } else if (isHudiTable && HiveMetaClient.HUDI_PARTITION_NULL_VALUE.equals(rawValue)) {
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

    public static List<String> getPartitionValues(PartitionKey partitionKey) {
        return getPartitionValues(partitionKey, false);
    }

    public static List<String> getPartitionValues(PartitionKey partitionKey, boolean isHudiTable) {
        // get string value from partitionKey
        // using __HIVE_DEFAULT_PARTITION__ replace null value
        List<LiteralExpr> literalValues = partitionKey.getKeys();
        List<String> values = new ArrayList<>(literalValues.size());
        for (LiteralExpr value : literalValues) {
            if (value instanceof NullLiteral) {
                if (isHudiTable) {
                    values.add(HiveMetaClient.HUDI_PARTITION_NULL_VALUE);
                } else {
                    values.add(HiveMetaClient.PARTITION_NULL_VALUE);
                }
            } else if (value instanceof BoolLiteral) {
                BoolLiteral boolValue = ((BoolLiteral) value);
                values.add(String.valueOf(boolValue.getValue()));
            } else {
                values.add(value.getStringValue());
            }
        }
        return values;
    }

    public static List<String> getPartitionValues(String filePath, List<String> columnNames) {
        String[] subPaths = filePath.split("/");
        List<String> values = Lists.newArrayListWithCapacity(columnNames.size());
        for (String columnName : columnNames) {
            boolean found = false;
            for (String subPath : subPaths) {
                String pattern = columnName + "=";
                if (subPath.startsWith(pattern)) {
                    values.add(subPath.replace(pattern, ""));
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new StarRocksConnectorException("can not find value for column: " + columnName + " from path: " + filePath);
            }
        }
        return values;
    }

    /**
     * Returns the value of the ROW_COUNT constant, or -1 if not found.
     */
    public static long getRowCount(Map<String, String> parameters) {
        return getLongParam(StatsSetupConst.ROW_COUNT, parameters);
    }

    public static long getTotalSize(Map<String, String> parameters) {
        return getLongParam(StatsSetupConst.TOTAL_SIZE, parameters);
    }

    private static long getLongParam(String key, Map<String, String> parameters) {
        if (parameters == null) {
            return -1;
        }

        String value = parameters.get(key);
        if (value == null) {
            return -1;
        }

        try {
            return Long.valueOf(value);
        } catch (NumberFormatException exc) {
            // ignore
        }
        return -1;
    }
}
