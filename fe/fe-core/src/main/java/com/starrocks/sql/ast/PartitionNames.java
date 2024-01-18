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


package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * partition_names is ["p1=1/p2=2", "p1=5/p2=6"] in the hive or spark. The concept isn't the same as the partition_names
     * in current StarRocks. So we use partitionColNames to denote the names of partition columns,
     * and partitionColValues to denote their corresponding values.
     *
     * Static partition insert hive/iceberg/hudi table
     * for example:
     *         create external target_external_table (c1 int, c2 int, p1 int, p2 int) partition by (p1, p2);
     *         insert into target_external_table partition(p1=1, p2=2) select a1, a2 from source_table;
     *
     * The partitionColNames is ["p1", "p2"]
     * The partitionColValues is [expr(1), expr(2)]
     */
    private final List<String> partitionColNames;
    private final List<Expr> partitionColValues;

    private final NodePosition pos;

    public PartitionNames(boolean isTemp, List<String> partitionNames) {
        this(isTemp, partitionNames, NodePosition.ZERO);
    }

    public PartitionNames(boolean isTemp, List<String> partitionNames, NodePosition pos) {
        this(isTemp, partitionNames, new ArrayList<>(), new ArrayList<>(), pos);
    }

    public PartitionNames(boolean isTemp, List<String> partitionNames, List<String> partitionColNames,
                          List<Expr> partitionColValues, NodePosition pos) {
        this.pos = pos;
        this.partitionNames = partitionNames;
        this.partitionColNames = partitionColNames;
        this.partitionColValues = partitionColValues;
        this.isTemp = isTemp;
    }

    public PartitionNames(PartitionNames other) {
        this.pos = other.pos;
        this.partitionNames = Lists.newArrayList(other.partitionNames);
        this.partitionColNames = Lists.newArrayList(other.partitionColNames);
        this.partitionColValues = Lists.newArrayList(other.partitionColValues);
        this.isTemp = other.isTemp;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public List<Expr> getPartitionColValues() {
        return partitionColValues;
    }

    public boolean isTemp() {
        return isTemp;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public boolean isStaticKeyPartitionInsert() {
        return !partitionColValues.isEmpty();
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
