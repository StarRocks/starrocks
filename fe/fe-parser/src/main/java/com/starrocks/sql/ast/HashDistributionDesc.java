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
import com.google.common.collect.Lists;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class HashDistributionDesc extends DistributionDesc {
    private final int numBucket;
    private final List<String> distributionColumnNames;

    public HashDistributionDesc() {
        this(0, Lists.newArrayList(), NodePosition.ZERO);
    }

    public HashDistributionDesc(int numBucket, List<String> distributionColumnNames) {
        this(numBucket, distributionColumnNames, NodePosition.ZERO);
    }

    public HashDistributionDesc(int numBucket, List<String> distributionColumnNames, NodePosition pos) {
        super(pos);
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
    public String toString() {
        if (numBucket > 0) {
            return "DISTRIBUTED BY HASH(" + Joiner.on(", ").join(distributionColumnNames) + ") BUCKETS " + numBucket;
        } else {
            return "DISTRIBUTED BY HASH(" + Joiner.on(", ").join(distributionColumnNames) + ")";
        }
    }
}
