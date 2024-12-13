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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/RandomDistributionInfo.java

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

package com.starrocks.catalog;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;

<<<<<<< HEAD
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
=======
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

/**
 * Random partition.
 */
public class RandomDistributionInfo extends DistributionInfo {

    @SerializedName("b")
    private int bucketNum;

    public RandomDistributionInfo() {
        super();
    }

    public RandomDistributionInfo(int bucketNum) {
        super(DistributionInfoType.RANDOM);
        this.bucketNum = bucketNum;
    }

    @Override
<<<<<<< HEAD
    public DistributionDesc toDistributionDesc() {
        DistributionDesc distributionDesc = new RandomDistributionDesc(bucketNum);
        return distributionDesc;
=======
    public DistributionDesc toDistributionDesc(Map<ColumnId, Column> schema) {
        return new RandomDistributionDesc(bucketNum);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public boolean supportColocate() {
        return false;
    }

    @Override
    public int getBucketNum() {
        return bucketNum;
    }

    @Override
    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    @Override
    public RandomDistributionInfo copy() {
        return new RandomDistributionInfo(bucketNum);
    }

    @Override
<<<<<<< HEAD
    public String toSql() {
=======
    public String toSql(Map<ColumnId, Column> schema) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        StringBuilder builder = new StringBuilder();
        builder.append("DISTRIBUTED BY RANDOM");
        if (bucketNum > 0) {
            builder.append(" BUCKETS ").append(bucketNum);
        }
        return builder.toString();
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(bucketNum);
    }

<<<<<<< HEAD
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        bucketNum = in.readInt();
    }

    public static DistributionInfo read(DataInput in) throws IOException {
        DistributionInfo distributionInfo = new RandomDistributionInfo();
        distributionInfo.readFields(in);
        return distributionInfo;
=======
    @Override
    public List<ColumnId> getDistributionColumns() {
        return Collections.emptyList();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, bucketNum);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof RandomDistributionInfo)) {
            return false;
        }

        RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) other;

        return type == randomDistributionInfo.type
                && bucketNum == randomDistributionInfo.bucketNum;
    }

    public HashDistributionInfo toHashDistributionInfo(List<Column> baseSchema) {
        List<Column> keyColumns = Lists.newArrayList();
        for (Column column : baseSchema) {
            if (column.isKey()) {
                keyColumns.add(column);
            }
        }
        HashDistributionInfo hashDistributionInfo = new HashDistributionInfo(bucketNum, keyColumns);
        return hashDistributionInfo;
    }
}
