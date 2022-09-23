// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.external.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

// TODO(stephen): rename NewHivePartitionName to HivePartitionName and remove original HivePartitionName
//  After refactoring others related codes
public class NewHivePartitionName {
    private final String databaseName;
    private final String tableName;
    private final List<String> partitionValues;

    // partition name eg: "year=2020/month=10/day=10"
    private final Optional<String> partitionNames;

    public NewHivePartitionName(String dbName, String tableName, List<String> partitionValues) {
        this(dbName, tableName, partitionValues, Optional.empty());
    }

    public NewHivePartitionName(String databaseName,
                                String tableName,
                                List<String> partitionValues,
                                Optional<String> partitionNames) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionValues = partitionValues;
        this.partitionNames = partitionNames;
    }

    public static NewHivePartitionName of(String dbName, String tblName, List<String> partitionValues) {
        return new NewHivePartitionName(dbName, tblName, partitionValues);
    }

    public static NewHivePartitionName of(String dbName, String tblName, String partitionNames) {
        return new NewHivePartitionName(dbName, tblName, Utils.toPartitionValues(partitionNames), Optional.of(partitionNames));
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

    public String getTableName() {
        return tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public Optional<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean approximateMatchTable(String db, String tblName) {
        return this.databaseName.equals(db) && this.tableName.equals(tblName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NewHivePartitionName other = (NewHivePartitionName) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(partitionValues, other.partitionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, partitionValues);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NewHivePartitionName{");
        sb.append("databaseName='").append(databaseName).append('\'');
        sb.append(", tableName='").append(tableName).append('\'');
        sb.append(", partitionValues=").append(partitionValues);
        sb.append(", partitionNames=").append(partitionNames);
        sb.append('}');
        return sb.toString();
    }
}