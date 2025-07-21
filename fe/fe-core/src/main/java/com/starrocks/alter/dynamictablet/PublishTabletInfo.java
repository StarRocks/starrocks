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

import com.starrocks.proto.MergingTabletInfoPB;
import com.starrocks.proto.SplittingTabletInfoPB;

import java.util.ArrayList;
import java.util.List;

/*
 * PublishTabletInfo is for publish version
 */
public class PublishTabletInfo {
    private final List<Long> tabletIds = new ArrayList<>();
    private final List<SplittingTabletInfoPB> splittingTabletInfos = new ArrayList<>();
    private final List<MergingTabletInfoPB> mergingTabletInfos = new ArrayList<>();

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public void addTabletId(long tabletId) {
        tabletIds.add(tabletId);
    }

    public List<SplittingTabletInfoPB> getSplittingTabletInfos() {
        return splittingTabletInfos;
    }

    public void addSplittingTabletInfo(SplittingTabletInfoPB splittingTabletInfoPB) {
        splittingTabletInfos.add(splittingTabletInfoPB);
    }

    public List<MergingTabletInfoPB> getMergingTabletInfos() {
        return mergingTabletInfos;
    }

    public void addMergingTabletInfo(MergingTabletInfoPB mergingTabletInfoPB) {
        mergingTabletInfos.add(mergingTabletInfoPB);
    }

    public List<Long> getOldTabletIds() {
        List<Long> oldTabletIds = new ArrayList<>(tabletIds);
        for (SplittingTabletInfoPB splittingTabletInfoPB : splittingTabletInfos) {
            oldTabletIds.add(splittingTabletInfoPB.getOldTabletId());
        }
        for (MergingTabletInfoPB mergingTabletInfoPB : mergingTabletInfos) {
            oldTabletIds.addAll(mergingTabletInfoPB.getOldTabletIds());
        }
        return oldTabletIds;
    }
}
