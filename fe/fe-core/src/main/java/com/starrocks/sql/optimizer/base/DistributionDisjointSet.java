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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.function.Function;

// it's a disjoint-set data structure to record derived distribution cols from equalPredicate in on clause
public class DistributionDisjointSet {

    private Map<DistributionCol, DistributionCol> parent;

    public DistributionDisjointSet() {
        parent = Maps.newHashMap();
    }

    public Map<DistributionCol, DistributionCol> getParentMap() {
        return parent;
    }

    public void updateParentMap(Map<DistributionCol, DistributionCol> parentMap) {
        this.parent = parentMap;
    }

    public DistributionCol find(DistributionCol col) {
        parent.computeIfAbsent(col, Function.identity());

        DistributionCol root = col;
        while (parent.get(root) != root) {
            root = parent.get(root);
        }

        while (col != root) {
            DistributionCol next = parent.get(col);
            parent.put(col, root);
            col = next;
        }

        return root;
    }

    public void union(DistributionCol col1, DistributionCol col2) {
        DistributionCol root1 = find(col1);
        DistributionCol root2 = find(col2);

        if (!root1.equals(root2)) {
            parent.put(root2, root1);
        }
    }

    public DistributionDisjointSet copy() {
        DistributionDisjointSet copy = new DistributionDisjointSet();
        copy.parent = Maps.newHashMap(parent);
        return copy;
    }

    public boolean isConnected(DistributionCol col1, DistributionCol col2) {
        return find(col1) == find(col2);
    }
}
