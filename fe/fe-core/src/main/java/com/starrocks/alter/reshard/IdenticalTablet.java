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
import com.starrocks.proto.IdenticalTabletInfoPB;
import com.starrocks.proto.ReshardingTabletInfoPB;

import java.util.List;

/*
 * IdenticalTablet saves the old tablet id and the new tablet for a tablet that is not split or merged
 */
public class IdenticalTablet implements ReshardingTablet {

    @SerializedName(value = "oldTabletId")
    protected final long oldTabletId;

    @SerializedName(value = "newTabletId")
    protected final long newTabletId;

    public IdenticalTablet(long oldTabletId, long newTabletId) {
        this.oldTabletId = oldTabletId;
        this.newTabletId = newTabletId;
    }

    @Override
    public SplittingTablet getSplittingTablet() {
        return null;
    }

    @Override
    public MergingTablet getMergingTablet() {
        return null;
    }

    @Override
    public IdenticalTablet getIdenticalTablet() {
        return this;
    }

    @Override
    public long getFirstOldTabletId() {
        return oldTabletId;
    }

    public long getOldTabletId() {
        return oldTabletId;
    }

    @Override
    public List<Long> getOldTabletIds() {
        return List.of(oldTabletId);
    }

    public long getNewTabletId() {
        return newTabletId;
    }

    @Override
    public List<Long> getNewTabletIds() {
        return List.of(newTabletId);
    }

    @Override
    public long getParallelTablets() {
        return 0;
    }

    @Override
    public ReshardingTabletInfoPB toProto() {
        ReshardingTabletInfoPB reshardingTabletInfoPB = new ReshardingTabletInfoPB();
        reshardingTabletInfoPB.identicalTabletInfo = new IdenticalTabletInfoPB();
        reshardingTabletInfoPB.identicalTabletInfo.oldTabletId = oldTabletId;
        reshardingTabletInfoPB.identicalTabletInfo.newTabletId = newTabletId;
        return reshardingTabletInfoPB;
    }
}
