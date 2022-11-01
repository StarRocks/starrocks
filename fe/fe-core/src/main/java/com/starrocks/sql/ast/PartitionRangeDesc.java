// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

public class PartitionRangeDesc implements ParseNode {
    private final String partitionStart;
    private final String partitionEnd;

    public PartitionRangeDesc(String partitionStart, String partitionEnd) {
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
    }

    public String getPartitionStart() {
        return partitionStart;
    }

    public String getPartitionEnd() {
        return partitionEnd;
    }
}
