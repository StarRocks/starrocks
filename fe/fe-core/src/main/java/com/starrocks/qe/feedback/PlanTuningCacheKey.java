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

package com.starrocks.qe.feedback;

import com.google.common.collect.Lists;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;

import java.util.List;
import java.util.Objects;

public class PlanTuningCacheKey {
    private final String sql;

    private final SkeletonNode root;

    private final List<SkeletonNode> skeletonNodes = Lists.newArrayList();

    private List<Integer> cachedHashCodes = Lists.newArrayList();


    public PlanTuningCacheKey(String sql, SkeletonNode root) {
        this.sql = sql;
        this.root = root;
        extractSkeletonNodes(root);
    }

    public String getSql() {
        return sql;
    }

    @Override
    public int hashCode() {
        if (cachedHashCodes.isEmpty()) {
            cachedHashCodes.add(sql.hashCode());
            for (SkeletonNode node : skeletonNodes) {
                cachedHashCodes.add(node.hashCode());
            }
        }

        return cachedHashCodes.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanTuningCacheKey that = (PlanTuningCacheKey) o;
        return Objects.equals(sql, that.sql) && Objects.equals(skeletonNodes, that.skeletonNodes);
    }

    private void extractSkeletonNodes(SkeletonNode node) {
        skeletonNodes.add(node);
        for (SkeletonNode child : node.getChildren()) {
            extractSkeletonNodes(child);
        }
    }
}
