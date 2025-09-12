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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/RandomDistributionDesc.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class RandomDistributionDesc extends DistributionDesc {
    int numBucket;

    public RandomDistributionDesc() {
        this(0, NodePosition.ZERO);
    }

    public RandomDistributionDesc(int numBucket) {
        this(numBucket, NodePosition.ZERO);
    }

    public RandomDistributionDesc(int numBucket, NodePosition pos) {
        super(pos);
        this.numBucket = numBucket;
    }


    @Override
    public int getBuckets() {
        return numBucket;
    }

    @Override
    public String toString() {
        if (numBucket > 0) {
            return "DISTRIBUTED BY RANDOM BUCKETS " + numBucket;
        } else {
            return "DISTRIBUTED BY RANDOM";
        }
    }
}
