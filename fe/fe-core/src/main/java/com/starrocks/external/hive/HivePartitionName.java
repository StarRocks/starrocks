// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.NullableKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table.TableType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class HivePartitionName {
    private final String databaseName;
    private final String tableName;
    private final List<String> partitionValues;

    // eg : "year=2020/month=10/day=10"
    private final Optional<String> partitionNames;

    public HivePartitionName(String dbName, String tableName, List<String> partitionValues) {
        this(dbName, tableName, partitionValues, Optional.empty());
    }


    public HivePartitionName(String databaseName, String tableName, List<String> partitionValues, Optional<String> partitionNames) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionValues = partitionValues;
        this.partitionNames = partitionNames;
    }

    public static HivePartitionName of(String dbName, String tblName, List<String> partitionValues) {
        return new HivePartitionName(dbName, tblName, partitionValues);
    }

    public static HivePartitionName of(String dbName, String tblName, String partitionNames) {
        return new HivePartitionName(dbName, tblName, HiveUtils.toPartitionValues(partitionNames), Optional.of(partitionNames));
    }


    public static List<String> fromPartitionKey(PartitionKey key) {
        // get string value from partitionKey
        List<LiteralExpr> literalValues = key.getKeys();
        List<String> values = new ArrayList<>(literalValues.size());
        for (LiteralExpr value : literalValues) {
            if (value instanceof NullLiteral) {
                values.add(((NullableKey) key).nullPartitionValue());
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

        HivePartitionName other = (HivePartitionName) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName) &&
                Objects.equals(partitionValues, other.partitionValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, partitionValues);
    }
}
