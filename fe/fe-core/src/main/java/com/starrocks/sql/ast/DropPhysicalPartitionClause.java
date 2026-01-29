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
 * DropPhysicalPartitionClause is used to drop an existing physical partition (sub-partition)
 * from a logical partition. This is primarily used for random distribution tables
 * with automatic bucketing enabled.
 * 
 * Syntax:
 *   ALTER TABLE table_name DROP PHYSICAL PARTITION physical_partition_id [FORCE]
 * 
 * Example:
 *   ALTER TABLE my_table DROP PHYSICAL PARTITION 12345;
 *   ALTER TABLE my_table DROP PHYSICAL PARTITION 12345 FORCE;
 */
public class DropPhysicalPartitionClause extends AlterTableClause {

    // The ID of the physical partition to drop
    private final long physicalPartitionId;

    // If true, force drop even if partition has data
    private final boolean forceDrop;

    public DropPhysicalPartitionClause(long physicalPartitionId, boolean forceDrop) {
        this(physicalPartitionId, forceDrop, NodePosition.ZERO);
    }

    public DropPhysicalPartitionClause(long physicalPartitionId, boolean forceDrop, NodePosition pos) {
        super(pos);
        this.physicalPartitionId = physicalPartitionId;
        this.forceDrop = forceDrop;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitDropPhysicalPartitionClause(this, context);
    }
}
