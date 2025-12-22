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

import com.starrocks.common.Config;

public class TabletReshardUtils {

    public static int calcSplitCount(long dataSize, long targetSize) {
        if (dataSize < 2 * targetSize) {
            return 1;
        }

        // Round of (dataSize / targetSize)
        long splitCount = (dataSize + targetSize / 2) / targetSize;
        if (splitCount < Config.tablet_reshard_max_split_count) {
            return (int) splitCount;
        }
        return Config.tablet_reshard_max_split_count;
    }
}
