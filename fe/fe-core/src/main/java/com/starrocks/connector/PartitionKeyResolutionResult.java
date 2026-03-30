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

package com.starrocks.connector;

import com.starrocks.catalog.PartitionKey;

import java.util.Collections;
import java.util.List;

/**
 * Result of resolving a single external base partition to MV partition key(s).
 * <p>
 * In the current phase, each base partition maps to exactly one PartitionKey.
 * The interface supports 1..N from day one so that future phases (historical spec,
 * partition expr fallback, synthetic transform) can return multiple keys without
 * changing the pipeline contract.
 */
public class PartitionKeyResolutionResult {
    private final List<PartitionKey> mvPartitionKeys;
    private final PartitionKeyResolutionPath path;

    private PartitionKeyResolutionResult(List<PartitionKey> mvPartitionKeys, PartitionKeyResolutionPath path) {
        this.mvPartitionKeys = mvPartitionKeys;
        this.path = path;
    }

    public static PartitionKeyResolutionResult of(PartitionKey mvPartitionKey, PartitionKeyResolutionPath path) {
        return new PartitionKeyResolutionResult(Collections.singletonList(mvPartitionKey), path);
    }

    public static PartitionKeyResolutionResult of(List<PartitionKey> mvPartitionKeys,
                                                  PartitionKeyResolutionPath path) {
        return new PartitionKeyResolutionResult(mvPartitionKeys, path);
    }

    public List<PartitionKey> getKeys() {
        return mvPartitionKeys;
    }

    public PartitionKey getSingleKey() {
        return mvPartitionKeys.get(0);
    }

    public PartitionKeyResolutionPath getPath() {
        return path;
    }
}
