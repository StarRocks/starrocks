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

import com.google.common.base.Preconditions;
import com.starrocks.connector.BucketProperty;

import java.util.List;

public class HashDistributionDescBP extends HashDistributionDesc {
    private final List<BucketProperty> bucketProperties;

    public HashDistributionDescBP(List<Integer> columns, SourceType sourceType, List<BucketProperty> bucketProperties) {
        super(columns, sourceType);
        this.bucketProperties = bucketProperties;
        Preconditions.checkArgument(columns.size() == bucketProperties.size(),
                "distribution cols don't match");
    }

    private HashDistributionDescBP(HashDistributionDesc desc, List<BucketProperty> bucketProperties) {
        super(desc.getDistributionCols(), desc.getSourceType());
        this.bucketProperties = bucketProperties;
    }

    public List<BucketProperty> getBucketProperties() {
        return bucketProperties;
    }

    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public boolean canColocate(HashDistributionDesc o) {
        if (!this.isLocal() || !o.isLocal()) {
            return false;
        }
        if (o.isNative()) {
            return false;
        }
        List<BucketProperty> oBP = ((HashDistributionDescBP) o).getBucketProperties();
        if (bucketProperties.size() != oBP.size()) {
            return false;
        }
        for (int i = 0; i < bucketProperties.size(); i++) {
            BucketProperty leftProperty = bucketProperties.get(i);
            BucketProperty rightProperty = oBP.get(i);
            if (!leftProperty.satisfy(rightProperty)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public HashDistributionDescBP getNullRelaxDesc() {
        return new HashDistributionDescBP(super.getNullRelaxDesc(), bucketProperties);
    }

    @Override
    public HashDistributionDescBP getNullStrictDesc() {
        return new HashDistributionDescBP(super.getNullStrictDesc(), bucketProperties);
    }
}
