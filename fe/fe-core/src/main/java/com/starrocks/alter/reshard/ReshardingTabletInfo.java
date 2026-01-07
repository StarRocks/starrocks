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

/*
 * ReshardingTabletInfo saves the info during tablet splitting or merging for a tablet
 */
public class ReshardingTabletInfo {

    protected final ReshardingTablet reshardingTablet;

    // The physical partition version of tablet splitting or merging.
    // Versions greater than it need cross publish until tablet reshard finished.
    // This field is used to check which versions need cross publish.
    protected final long visibleVersion;

    public ReshardingTabletInfo(ReshardingTablet reshardingTablet, long visibleVersion) {
        this.reshardingTablet = reshardingTablet;
        this.visibleVersion = visibleVersion;
    }

    public ReshardingTablet getReshardingTablet() {
        return reshardingTablet;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }
}
