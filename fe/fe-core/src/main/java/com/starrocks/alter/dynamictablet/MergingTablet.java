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
 * MergingTablet saves the old tablet ids and the new tablet during tablets merging
 */
public class MergingTablet implements DynamicTablet {

    @SerializedName(value = "oldTabletIds")
    protected final List<Long> oldTabletIds;

    @SerializedName(value = "newTabletId")
    protected final long newTabletId;

    public MergingTablet(List<Long> oldTabletIds, long newTabletId) {
        // Old tablet size is usaully 2, but we allow a power of 2
        Preconditions.checkState(DynamicTabletUtils.isPowerOfTwo(oldTabletIds.size()),
                "Old tablet size must be a power of 2, actual: " + oldTabletIds.size());

        this.oldTabletIds = oldTabletIds;
        this.newTabletId = newTabletId;
    }

    public List<Long> getOldTabletIds() {
        return oldTabletIds;
    }

    public long getNewTabletId() {
        return newTabletId;
    }

    @Override
    public SplittingTablet getSplittingTablet() {
        return null;
    }

    @Override
    public MergingTablet getMergingTablet() {
        return this;
    }

    @Override
    public IdenticalTablet getIdenticalTablet() {
        return null;
    }

    @Override
    public long getFirstOldTabletId() {
        return oldTabletIds.get(0);
    }

    @Override
    public long getParallelTablets() {
        return oldTabletIds.size();
    }
}
