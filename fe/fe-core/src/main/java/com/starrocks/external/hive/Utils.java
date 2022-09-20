// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Utils {
    public static final String DECIMAL_PATTERN = "^decimal\\((\\d+),(\\d+)\\)";
    public static final String ARRAY_PATTERN = "^array<([0-9a-z<>(),]+)>";
    public static final String MAP_PATTERN = "^map<([0-9a-z<>(),:]+)>";
    public static final String CHAR_PATTERN = "^char\\(([0-9]+)\\)";
    public static final String VARCHAR_PATTERN = "^varchar\\(([0-9,-1]+)\\)";
    protected static final List<String> HIVE_UNSUPPORTED_TYPES = Arrays.asList("STRUCT", "BINARY", "MAP", "UNIONTYPE");

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

    public static String getSuffixName(String dirPath, String filePath) {
        Preconditions.checkArgument(filePath.startsWith(dirPath),
                "dirPath " + dirPath + " should be prefix of filePath " + filePath);

        String name = filePath.replaceFirst(dirPath, "");
        if (name.startsWith("/")) {
            name = name.substring(1);
        }
        return name;
    }

    public static List<String> getPartitionValues(String filePath, List<String> columnNames) throws DdlException {
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
                throw new DdlException("can not find value for column: " + columnName + " from path: " + filePath);
            }
        }
        return values;
    }

    public static String getTypeKeyword(String type) {
        String keyword = type;
        int parenthesesIndex;
        if ((parenthesesIndex = keyword.indexOf('<')) >= 0) {
            keyword = keyword.substring(0, parenthesesIndex).trim();
        } else if ((parenthesesIndex = keyword.indexOf('(')) >= 0) {
            keyword = keyword.substring(0, parenthesesIndex).trim();
        }
        return keyword;
    }

    // Decimal string like "Decimal(3,2)"
    public static int[] getPrecisionAndScale(String typeStr) throws DdlException {
        Matcher matcher = Pattern.compile(DECIMAL_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return new int[] {Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))};
        }
        throw new DdlException("Failed to get precision and scale at " + typeStr);
    }

    // Array string like "Array<Array<int>>"
    public static Type convertToArrayType(String typeStr) throws DdlException {
        if (!HIVE_UNSUPPORTED_TYPES.stream().filter(typeStr.toUpperCase()::contains).collect(Collectors.toList())
                .isEmpty()) {
            return Type.UNKNOWN_TYPE;
        }
        Matcher matcher = Pattern.compile(ARRAY_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        Type itemType;
        if (matcher.find()) {
            if (convertToArrayType(matcher.group(1)).equals(Type.UNKNOWN_TYPE)) {
                itemType = Type.UNKNOWN_TYPE;
            } else {
                itemType = new ArrayType(convertToArrayType(matcher.group(1)));
            }
        } else {
            itemType = HiveMetaStoreTableUtils.convertHiveTableColumnType(typeStr);
        }
        return itemType;
    }

    public static String[] getKeyValueStr(String typeStr) throws DdlException {
        Matcher matcher = Pattern.compile(MAP_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            String kvStr = matcher.group(1);
            int size = kvStr.length();
            int stack = 0;
            int index = 0;
            for (int i = 0; i < size; i++) {
                char c = kvStr.charAt(i);
                if (c == '<' || c == '(') {
                    stack++;
                } else if (c == '>' || c == ')') {
                    stack--;
                } else if (c == ',' && stack == 0) {
                    index = i;
                    break;
                }
            }
            if (index == 0 || index == size - 1) {
                throw new DdlException("Error Map Type" + typeStr);
            }
            return new String[] {kvStr.substring(0, index).trim(), kvStr.substring(index + 1, size).trim()};
        } else {
            throw new DdlException("Failed to get MapType at " + typeStr);
        }
    }

    // Map string like map<keytype, valuetype>
    public static Type convertToMapType(String typeStr) throws DdlException {
        String[] kv = getKeyValueStr(typeStr);
        return new MapType(HiveMetaStoreTableUtils.convertHiveTableColumnType(kv[0]),
                HiveMetaStoreTableUtils.convertHiveTableColumnType(kv[1]));
    }

    // Char string like char(100)
    public static int getCharLength(String typeStr) throws DdlException {
        Matcher matcher = Pattern.compile(CHAR_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new DdlException("Failed to get char length at " + typeStr);
    }

    // Varchar string like varchar(100)
    public static int getVarcharLength(String typeStr) throws DdlException {
        Matcher matcher = Pattern.compile(VARCHAR_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new DdlException("Failed to get varchar length at " + typeStr);
    }

}
