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

package com.starrocks.alter.dynamictablet;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/*
 * SplittingTablet saves the old tablet id and the new tablets during a tablet splitting
 */
public class SplittingTablet implements DynamicTablet {

    @SerializedName(value = "oldTabletId")
    protected final long oldTabletId;

    @SerializedName(value = "newTabletIds")
    protected final List<Long> newTabletIds;

    public SplittingTablet(long oldTabletId, List<Long> newTabletIds) {
        // New tablet size is usaully 2, but we allow a power of 2
        Preconditions.checkState(DynamicTabletUtils.isPowerOfTwo(newTabletIds.size()),
                "New tablet size must be a power of 2, actual: " + newTabletIds.size());

        this.oldTabletId = oldTabletId;
        this.newTabletIds = newTabletIds;
    }

    public long getOldTabletId() {
        return oldTabletId;
    }

    public List<Long> getNewTabletIds() {
        return newTabletIds;
    }

    @Override
    public SplittingTablet getSplittingTablet() {
        return this;
    }

    @Override
    public MergingTablet getMergingTablet() {
        return null;
    }

    @Override
    public IdenticalTablet getIdenticalTablet() {
        return null;
    }

    @Override
    public long getFirstOldTabletId() {
        return oldTabletId;
    }

    @Override
    public long getParallelTablets() {
        return newTabletIds.size();
    }
}
