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

package com.starrocks.lake.bookmark;

import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;

import java.util.Optional;

/**
 * Maps a live physical partition to its bookmark-scoped replacement, or
 * {@link Optional#empty()} to drop it from the shadow. Empty means the
 * physical is not part of the bookmark — either absent from the bookmark, or
 * its base materialized index has drifted (replaced or resharded).
 */
@FunctionalInterface
public interface BookmarkPartitionRewriter {
    Optional<PhysicalPartition> rewrite(Partition partition, PhysicalPartition physical);
}
