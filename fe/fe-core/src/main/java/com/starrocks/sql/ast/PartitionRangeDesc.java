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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class PartitionRangeDesc implements ParseNode {
    private final String partitionStart;
    private final String partitionEnd;

    private final NodePosition pos;

    public PartitionRangeDesc(String partitionStart, String partitionEnd) {
        this(partitionStart, partitionEnd, NodePosition.ZERO);
    }

    public PartitionRangeDesc(String partitionStart, String partitionEnd, NodePosition pos) {
        this.pos = pos;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
    }

    public String getPartitionStart() {
        return partitionStart;
    }

    public String getPartitionEnd() {
        return partitionEnd;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
