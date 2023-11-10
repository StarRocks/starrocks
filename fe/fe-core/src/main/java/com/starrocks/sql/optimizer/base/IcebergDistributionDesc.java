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

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Objects;
import com.starrocks.catalog.IcebergTable;

import java.util.List;

public class IcebergDistributionDesc extends HashDistributionDesc {

    private final List<IcebergTable.BucketProperty> bucketProperties;
    public IcebergDistributionDesc(List<Integer> columns, HashDistributionDesc.SourceType sourceType,
                                   List<IcebergTable.BucketProperty> bucketProperties) {
        super(columns, sourceType);
        this.bucketProperties = bucketProperties;
    }

    public List<IcebergTable.BucketProperty> getBucketProperties() {
        return bucketProperties;
    }

    @Override
    public boolean isSatisfy(HashDistributionDesc item) {
        if (item == this) {
            return true;
        }

        if (getDistributionCols().size() > item.getDistributionCols().size()) {
            return false;
        }

        if (item.getSourceType() == SourceType.SHUFFLE_AGG || item.getSourceType() == SourceType.SHUFFLE_JOIN) {
            return distributionColsContainsAll(item.getDistributionCols());
        } else {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergDistributionDesc)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IcebergDistributionDesc that = (IcebergDistributionDesc) o;
        return Objects.equal(bucketProperties, that.bucketProperties);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), bucketProperties);
    }
}