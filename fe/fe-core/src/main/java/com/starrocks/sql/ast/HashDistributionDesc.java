// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class HashDistributionDesc extends DistributionDesc {
    private int numBucket;
    private final List<String> distributionColumnNames;

    public HashDistributionDesc() {
        type = DistributionInfoType.HASH;
        distributionColumnNames = Lists.newArrayList();
    }

    public HashDistributionDesc(int numBucket, List<String> distributionColumnNames) {
        type = DistributionInfoType.HASH;
        this.numBucket = numBucket;
        this.distributionColumnNames = distributionColumnNames;
    }

    public List<String> getDistributionColumnNames() {
        return distributionColumnNames;
    }

    @Override
    public int getBuckets() {
        return numBucket;
    }

    @Override
    public void analyze(Set<String> cols) {
        if (numBucket < 0) {
            throw new SemanticException("Number of hash distribution is zero.");
        }

        if (distributionColumnNames == null || distributionColumnNames.size() == 0) {
            throw new SemanticException("Number of hash column is zero.");
        }
        for (String columnName : distributionColumnNames) {
            if (!cols.contains(columnName)) {
                throw new SemanticException("Distribution column(" + columnName + ") doesn't exist.");
            }
        }
    }

    @Override
    public DistributionInfo toDistributionInfo(List<Column> columns) throws DdlException {
        List<Column> distributionColumns = Lists.newArrayList();

        // check and get distribution column
        for (String colName : distributionColumnNames) {
            boolean find = false;
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                        throw new DdlException("Distribution column[" + colName + "] is not key column");
                    }

                    if (!column.getType().canDistributedBy()) {
                        throw new DdlException(column.getType() + " column can not be distribution column");
                    }

                    distributionColumns.add(column);
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Distribution column[" + colName + "] does not found");
            }
        }

        return new HashDistributionInfo(numBucket, distributionColumns);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeInt(numBucket);
        int count = distributionColumnNames.size();
        out.writeInt(count);
        for (String colName : distributionColumnNames) {
            Text.writeString(out, colName);
        }
    }

    public void readFields(DataInput in) throws IOException {
        numBucket = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            distributionColumnNames.add(Text.readString(in));
        }
    }

    @Override
    public String toString() {
        return "DISTRIBUTED BY HASH" + Joiner.on(",").join(distributionColumnNames) + " BUCKETS " + numBucket;
    }
}
