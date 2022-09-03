// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/*
 * To represent following stmt:
 *      PARTITION p1
 *      TEMPORARY PARTITION p1
 *      PARTITION (p1, p2)
 *      TEMPORARY PARTITION (p1, p2)
 *      PARTITIONS (p1, p2)
 *      TEMPORARY PARTITIONS (p1, p2)
 */
public class PartitionNames implements ParseNode, Writable {

    @SerializedName(value = "partitionNames")
    private final List<String> partitionNames;
    // true if these partitions are temp partitions
    @SerializedName(value = "isTemp")
    private final boolean isTemp;

    public PartitionNames(boolean isTemp, List<String> partitionNames) {
        this.partitionNames = partitionNames;
        this.isTemp = isTemp;
    }

    public PartitionNames(PartitionNames other) {
        this.partitionNames = Lists.newArrayList(other.partitionNames);
        this.isTemp = other.isTemp;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public boolean isTemp() {
        return isTemp;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (partitionNames.isEmpty()) {
            throw new AnalysisException("No partition specifed in partition lists");
        }
        // check if partition name is not empty string
        if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
            throw new AnalysisException("there are empty partition name");
        }
    }

    @Override
    public String toString() {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (isTemp) {
            sb.append("TEMPORARY ");
        }
        sb.append("PARTITIONS (");
        sb.append(Joiner.on(", ").join(partitionNames));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static PartitionNames read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionNames.class);
    }
}
