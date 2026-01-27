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

import com.starrocks.sql.parser.NodePosition;

/**
 * AddPhysicalPartitionClause is used to add a new physical partition (sub-partition)
 * to an existing logical partition. This is primarily used for random distribution tables
 * with automatic bucketing enabled.
 * 
 * Physical partitions inherit all properties from the parent logical partition.
 * 
 * Syntax:
 *   ALTER TABLE table_name ADD PHYSICAL PARTITION [partition_name] [BUCKETS bucket_num]
 * 
 * Example:
 *   ALTER TABLE my_table ADD PHYSICAL PARTITION p1 BUCKETS 16;
 */
public class AddPhysicalPartitionClause extends AlterTableClause {

    // The name of the logical partition to add physical partition to
    // If null, defaults to the single partition for non-partitioned tables
    private final String partitionName;

    // Number of buckets for the new physical partition
    // If 0, the system will infer the bucket number automatically
    private final int bucketNum;

    public AddPhysicalPartitionClause(String partitionName, int bucketNum) {
        this(partitionName, bucketNum, NodePosition.ZERO);
    }

    public AddPhysicalPartitionClause(String partitionName, int bucketNum, NodePosition pos) {
        super(pos);
        this.partitionName = partitionName;
        this.bucketNum = bucketNum;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAddPhysicalPartitionClause(this, context);
    }
}
