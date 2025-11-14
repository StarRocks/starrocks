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

package com.starrocks.catalog;

import com.google.common.base.Objects;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.RangeDistributionDesc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Range distribution.
 */
public class RangeDistributionInfo extends DistributionInfo {

    public RangeDistributionInfo() {
        super(DistributionInfoType.RANGE);
    }

    /*
     * Will be changed to true later once colocate is supported.
     */
    @Override
    public boolean supportColocate() {
        return false;
    }

    /*
     * Range distribution doesn't need bucket num, just return 1 to create 1 tablet.
     */
    @Override
    public int getBucketNum() {
        return 1;
    }

    /*
     * Range distribution doesn't need bucket num.
     */
    @Override
    public void setBucketNum(int bucketNum) {
        // Do nothing
    }

    @Override
    public List<ColumnId> getDistributionColumns() {
        return Collections.emptyList();
    }

    @Override
    public DistributionDesc toDistributionDesc(Map<ColumnId, Column> schema) {
        return new RangeDistributionDesc();
    }

    /*
     * Range distribution will be the default distribution, no sql needed,
     * just return empty string.
     */
    @Override
    public String toSql(Map<ColumnId, Column> schema) {
        return "";
    }

    @Override
    public RangeDistributionInfo copy() {
        return new RangeDistributionInfo();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof RangeDistributionInfo)) {
            return false;
        }

        RangeDistributionInfo rangeDistributionInfo = (RangeDistributionInfo) other;

        return type == rangeDistributionInfo.type;
    }
}
