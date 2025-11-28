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

package com.starrocks.alter.reshard;

import com.google.gson.annotations.SerializedName;
import com.starrocks.proto.MergingTabletInfoPB;
import com.starrocks.proto.ReshardingTabletInfoPB;

import java.util.List;

/*
 * MergingTablet saves the old tablet ids and the new tablet during tablets merging
 */
public class MergingTablet implements ReshardingTablet {

    @SerializedName(value = "oldTabletIds")
    protected final List<Long> oldTabletIds;

    @SerializedName(value = "newTabletId")
    protected final long newTabletId;

    public MergingTablet(List<Long> oldTabletIds, long newTabletId) {
        this.oldTabletIds = oldTabletIds;
        this.newTabletId = newTabletId;
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
    public List<Long> getOldTabletIds() {
        return oldTabletIds;
    }

    public long getNewTabletId() {
        return newTabletId;
    }

    public List<Long> getNewTabletIds() {
        return List.of(newTabletId);
    }

    @Override
    public long getParallelTablets() {
        return oldTabletIds.size();
    }

    @Override
    public ReshardingTabletInfoPB toProto() {
        ReshardingTabletInfoPB reshardingTabletInfoPB = new ReshardingTabletInfoPB();
        reshardingTabletInfoPB.mergingTabletInfo = new MergingTabletInfoPB();
        reshardingTabletInfoPB.mergingTabletInfo.oldTabletIds = oldTabletIds;
        reshardingTabletInfoPB.mergingTabletInfo.newTabletId = newTabletId;
        return reshardingTabletInfoPB;
    }
}
