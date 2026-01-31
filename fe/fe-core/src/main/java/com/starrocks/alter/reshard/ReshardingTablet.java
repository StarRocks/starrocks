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

import java.util.List;

/*
 * ReshardingTablet saves the old and new tablets info during tablet splitting or merging
 * ReshardingTablet is the base class of SplittingTablet, MergingTablet and IdenticalTablet.
 */
public interface ReshardingTablet {

    SplittingTablet getSplittingTablet();

    MergingTablet getMergingTablet();

    IdenticalTablet getIdenticalTablet();

    long getFirstOldTabletId();

    List<Long> getOldTabletIds();

    List<Long> getNewTabletIds();

    long getParallelTablets();

    ReshardingTabletInfoPB toProto();
}
