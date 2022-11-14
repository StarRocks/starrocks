// This file is made available under Elastic License 2.0.
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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@Deprecated
public class RandomDistributionDesc extends DistributionDesc {
    int numBucket;

    public RandomDistributionDesc() {
        type = DistributionInfoType.RANDOM;
    }

    public RandomDistributionDesc(int numBucket) {
        type = DistributionInfoType.RANDOM;
        this.numBucket = numBucket;
    }

    @Override
    public void analyze(Set<String> colSet) {
        throw new SemanticException("Random distribution is deprecated now, use Hash distribution instead");
    }

    @Override
    public int getBuckets() {
        return numBucket;
    }

    @Override
    public DistributionInfo toDistributionInfo(List<Column> columns) {
        return new RandomDistributionInfo(numBucket);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(numBucket);
    }

    public void readFields(DataInput in) throws IOException {
        numBucket = in.readInt();
    }
}
