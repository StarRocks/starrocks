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

import com.google.common.collect.Maps;
import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// clause which is used to replace temporary partition
// eg:
// ALTER TABLE tbl REPLACE PARTITION (p1, p2, p3) WITH TEMPORARY PARTITION(tp1, tp2);
public class ReplacePartitionClause extends AlterTableClause {
    private PartitionNames partitionNames;
    private PartitionNames tempPartitionNames;
    private Map<String, String> properties = Maps.newHashMap();

    // "isStrictMode" is got from property "strict_range", and default is true.
    // If true, when replacing partition, the range of partitions must same as the range of temp partitions.
    private boolean isStrictRange;

    // "useTempPartitionName" is got from property "use_temp_partition_name", and default is false.
    // If false, after replacing, the replaced partition's name will remain unchanged.
    // Otherwise, the replaced partition's name will be the temp partitions name.
    // This parameter is valid only when the number of partitions is the same as the number of temp partitions.
    // For example:
    // 1. REPLACE PARTITION (p1, p2, p3) WITH TEMPORARY PARTITION(tp1, tp2) PROPERTIES("use_temp_partition_name" = "false");
    //      "use_temp_partition_name" will take no effect after replacing, and the partition names will be "tp1" and "tp2".
    //
    // 2. REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION(tp1, tp2) PROPERTIES("use_temp_partition_name" = "false");
    //      alter replacing, the partition names will be "p1" and "p2".
    //      but if "use_temp_partition_name" is true, the partition names will be "tp1" and "tp2".
    private boolean useTempPartitionName;

    public ReplacePartitionClause(PartitionNames partitionNames, PartitionNames tempPartitionNames,
                                  Map<String, String> properties) {
        this(partitionNames, tempPartitionNames, properties, NodePosition.ZERO);
    }

    public ReplacePartitionClause(PartitionNames partitionNames, PartitionNames tempPartitionNames,
                                  Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.REPLACE_PARTITION, pos);
        this.partitionNames = partitionNames;
        this.tempPartitionNames = tempPartitionNames;
        this.needTableStable = false;
        this.properties = properties;
    }

    public List<String> getPartitionNames() {
        return partitionNames.getPartitionNames();
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames.getPartitionNames();
    }

    public PartitionNames getPartition() {
        return partitionNames;
    }

    public PartitionNames getTempPartition() {
        return tempPartitionNames;
    }

    public boolean isStrictRange() {
        return isStrictRange;
    }

    public void setStrictRange(boolean strictRange) {
        isStrictRange = strictRange;
    }

    public boolean useTempPartitionName() {
        return useTempPartitionName;
    }

    public void setUseTempPartitionName(boolean useTempPartitionName) {
        this.useTempPartitionName = useTempPartitionName;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitReplacePartitionClause(this, context);
    }
}
