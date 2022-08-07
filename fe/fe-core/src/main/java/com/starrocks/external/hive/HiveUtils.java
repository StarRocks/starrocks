package com.starrocks.external.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.NullableKey;
import com.starrocks.catalog.PartitionKey;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class HiveUtils {

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

    public static String toPartitionNames(List<String> partitionNames, List<String> partitionValues) {
        Preconditions.checkState(partitionNames.size() == partitionValues.size(), "xxxxx");
        StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < partitionNames.size(); i++) {
            String value = partitionNames.get(i) + "=" + partitionValues.get(i);

            if (i != partitionNames.size() - 1) {
                value = value + "/";
            }
            resultBuilder.append(value);
        }
        return resultBuilder.toString();
    }

    public static List<String> toPartitionValues(PartitionKey partitionKey) {
        // get string value from partitionKey
        // using __HIVE_DEFAULT_PARTITION__ replace null value
        List<LiteralExpr> literalValues = partitionKey.getKeys();
        List<String> values = new ArrayList<>(literalValues.size());
        for (LiteralExpr value : literalValues) {
            if (value instanceof NullLiteral) {
                values.add(((NullableKey) partitionKey).nullPartitionValue());
            } else if (value instanceof BoolLiteral) {
                BoolLiteral boolValue = ((BoolLiteral) value);
                values.add(String.valueOf(boolValue.getValue()));
            } else {
                values.add(value.getStringValue());
            }
        }
        return values;
    }
}
