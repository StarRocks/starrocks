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

import com.starrocks.proto.ReshardingTabletInfoPB;

import java.util.ArrayList;
import java.util.List;

/*
 * PublishTabletsInfo is for publish version
 */
public class PublishTabletsInfo {
    private final List<Long> tabletIds = new ArrayList<>();
    private final List<ReshardingTabletInfoPB> reshardingTablets = new ArrayList<>();

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public void addTabletId(long tabletId) {
        tabletIds.add(tabletId);
    }

    public List<ReshardingTabletInfoPB> getReshardingTablets() {
        return reshardingTablets;
    }

    public void addReshardingTablet(ReshardingTablet reshardingTablet) {
        reshardingTablets.add(reshardingTablet.toProto());
    }

    public List<Long> getOldTabletIds() {
        List<Long> oldTabletIds = new ArrayList<>(tabletIds);
        for (ReshardingTabletInfoPB reshardingTabletInfoPB : reshardingTablets) {
            if (reshardingTabletInfoPB.splittingTabletInfo != null) {
                oldTabletIds.add(reshardingTabletInfoPB.splittingTabletInfo.getOldTabletId());
            } else if (reshardingTabletInfoPB.mergingTabletInfo != null) {
                oldTabletIds.addAll(reshardingTabletInfoPB.mergingTabletInfo.getOldTabletIds());
            } else if (reshardingTabletInfoPB.identicalTabletInfo != null) {
                oldTabletIds.add(reshardingTabletInfoPB.identicalTabletInfo.getOldTabletId());
            }
        }
        return oldTabletIds;
    }
}
