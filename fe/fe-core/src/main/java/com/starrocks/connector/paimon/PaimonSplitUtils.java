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

package com.starrocks.connector.paimon;

import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.util.Optional;

/** Utilities for handling Paimon data splits and global-index split wrappers uniformly. */
public final class PaimonSplitUtils {
    private PaimonSplitUtils() {
    }

    public static boolean isGlobalIndexSplit(Split split) {
        return split instanceof IndexedSplit;
    }

    public static Optional<DataSplit> getDataSplit(Split split) {
        if (split instanceof DataSplit) {
            return Optional.of((DataSplit) split);
        }
        if (split instanceof IndexedSplit) {
            return Optional.of(((IndexedSplit) split).dataSplit());
        }
        return Optional.empty();
    }
}
