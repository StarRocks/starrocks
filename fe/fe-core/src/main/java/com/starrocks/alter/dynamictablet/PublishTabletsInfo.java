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

import com.starrocks.proto.DynamicTabletsInfoPB;
import com.starrocks.proto.IdenticalTabletInfoPB;
import com.starrocks.proto.MergingTabletInfoPB;
import com.starrocks.proto.SplittingTabletInfoPB;

import java.util.ArrayList;
import java.util.List;

/*
 * PublishTabletsInfo is for publish version
 */
public class PublishTabletsInfo {
    private final List<Long> tabletIds = new ArrayList<>();
    private final DynamicTabletsInfoPB dynamicTablets = new DynamicTabletsInfoPB();

    public PublishTabletsInfo() {
        dynamicTablets.splittingTabletInfos = new ArrayList<>();
        dynamicTablets.mergingTabletInfos = new ArrayList<>();
        dynamicTablets.identicalTabletInfos = new ArrayList<>();
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public void addTabletId(long tabletId) {
        tabletIds.add(tabletId);
    }

    public boolean isDynamicTabletsEmpty() {
        return dynamicTablets.splittingTabletInfos.isEmpty()
                && dynamicTablets.mergingTabletInfos.isEmpty()
                && dynamicTablets.identicalTabletInfos.isEmpty();
    }

    public DynamicTabletsInfoPB getDynamicTablets() {
        if (isDynamicTabletsEmpty()) {
            return null;
        }
        return dynamicTablets;
    }

    public void addDynamicTablet(DynamicTablet dynamicTablet) {
        SplittingTablet splittingTablet = dynamicTablet.getSplittingTablet();
        if (splittingTablet != null) {
            addSplittingTablet(splittingTablet);
        }

        MergingTablet mergingTablet = dynamicTablet.getMergingTablet();
        if (mergingTablet != null) {
            addMergingTablet(mergingTablet);
        }

        IdenticalTablet identicalTablet = dynamicTablet.getIdenticalTablet();
        if (identicalTablet != null) {
            addIdenticalTablet(identicalTablet);
        }
    }

    public List<Long> getOldTabletIds() {
        List<Long> oldTabletIds = new ArrayList<>(tabletIds);
        for (SplittingTabletInfoPB splittingTabletInfoPB : dynamicTablets.splittingTabletInfos) {
            oldTabletIds.add(splittingTabletInfoPB.getOldTabletId());
        }
        for (MergingTabletInfoPB mergingTabletInfoPB : dynamicTablets.mergingTabletInfos) {
            oldTabletIds.addAll(mergingTabletInfoPB.getOldTabletIds());
        }
        for (IdenticalTabletInfoPB identicalTabletInfoPB : dynamicTablets.identicalTabletInfos) {
            oldTabletIds.add(identicalTabletInfoPB.getOldTabletId());
        }
        return oldTabletIds;
    }

    private void addSplittingTablet(SplittingTablet splittingTablet) {
        SplittingTabletInfoPB splittingTabletInfoPB = new SplittingTabletInfoPB();
        splittingTabletInfoPB.oldTabletId = splittingTablet.getOldTabletId();
        splittingTabletInfoPB.newTabletIds = splittingTablet.getNewTabletIds();
        dynamicTablets.splittingTabletInfos.add(splittingTabletInfoPB);
    }

    private void addMergingTablet(MergingTablet mergingTablet) {
        MergingTabletInfoPB mergingTabletInfoPB = new MergingTabletInfoPB();
        mergingTabletInfoPB.oldTabletIds = mergingTablet.getOldTabletIds();
        mergingTabletInfoPB.newTabletId = mergingTablet.getNewTabletId();
        dynamicTablets.mergingTabletInfos.add(mergingTabletInfoPB);
    }

    private void addIdenticalTablet(IdenticalTablet identicalTablet) {
        IdenticalTabletInfoPB identicalTabletInfoPB = new IdenticalTabletInfoPB();
        identicalTabletInfoPB.oldTabletId = identicalTablet.getOldTabletId();
        identicalTabletInfoPB.newTabletId = identicalTablet.getNewTabletId();
        dynamicTablets.identicalTabletInfos.add(identicalTabletInfoPB);
    }
}
