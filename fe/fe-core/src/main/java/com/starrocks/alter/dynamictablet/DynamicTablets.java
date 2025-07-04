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

import com.starrocks.catalog.Tablet;
import com.starrocks.common.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * DynamicTablets is the base class of DynamicTablets and MergingTablets.
 * DynamicTablets saves the old and new tablets during tablet splitting or merging for a materialized index
 */
public interface DynamicTablets {

    void addSplittingTablet(long oldTabletId, List<Tablet> newTablets);

    Map<Long, List<Tablet>> getSplittingTablets();

    void addMergingTablet(List<Long> oldTabletIds, Tablet newTablet);

    List<Pair<List<Long>, Tablet>> getMergingTablets();

    Set<Long> getOldTabletIds();

    List<Tablet> getNewTablets();

    boolean isEmpty();

    void clear();

    List<Long> calcNewVirtualBuckets(List<Long> oldVirtualBuckets);
}
