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
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;

import java.util.List;
import java.util.Objects;

public class PlanTuningCacheKey {
    private final String sql;

    private final SkeletonNode root;

    private final List<SkeletonNode> skeletonNodes = Lists.newArrayList();

    // The key lives in hash containers, so its hash code is computed once and never changes. It does not depend on
    // the scan predicate ranges, which are widened in place when tuning guides are merged.
    private final int hashCode;

    public PlanTuningCacheKey(String sql, SkeletonNode root) {
        this.sql = sql;
        this.root = root;
        extractSkeletonNodes(root);
        this.hashCode = skeletonNodes.hashCode();
    }

    public List<SkeletonNode> getSkeletonNodes() {
        return skeletonNodes;
    }

    public String getSql() {
        return sql;
    }

    /**
     * Whether this key equals {@code other} and the predicate ranges of all its scan nodes are enclosed by the ones
     * of {@code other}, i.e. the tuning guides collected for {@code other} also apply to this key.
     */
    public boolean isCoveredBy(PlanTuningCacheKey other) {
        if (!equals(other)) {
            return false;
        }

        for (int i = 0; i < skeletonNodes.size(); i++) {
            if (skeletonNodes.get(i) instanceof ScanNode scanNode &&
                    !scanNode.isCoveredBy((ScanNode) other.skeletonNodes.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
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
        return Objects.equals(skeletonNodes, that.skeletonNodes);
    }

    private void extractSkeletonNodes(SkeletonNode node) {
        skeletonNodes.add(node);
        for (SkeletonNode child : node.getChildren()) {
            extractSkeletonNodes(child);
        }
    }
}
