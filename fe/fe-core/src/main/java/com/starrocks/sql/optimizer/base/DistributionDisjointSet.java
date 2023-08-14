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

// it's a disjoint-set data structure to record derived distribution cols from equalPredicate in on clause.
// Given the following example：
//      select * from A JOIN B ON A.a = B.b
//      JOIN C ON B.b = C.c
//      JOIN D ON C.c = D.d
//      JOIN E ON D.d = E.e
// After A JOIN B, we can use A.a and B.b to describe the distribution info.
// After A JOIN B Join C, we can use A.a and B.b to describe the distribution info.
// The `distributionCols` in `HashDistributionDesc` can only record one possibility, while Equivalence-related
// information is not preserved. When performing the next JOIN operation JOIN D ON C.c = D.d, the output property
// of the left child may A.a, and the required property is C.c. We need determine if A.a is equivalent to C.c.
// The operation of checking if the output of the child satisfies the join condition is like a "find" operation.
// After the join, the operation of establishing equivalence between the corresponding columns of the left and right
// child is like a "union" operation.
// Naturally, using a disjoint-set data structure (also known as a union-find data structure) to describe this procedure
// is a suitable approach.
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

        // path compress
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

    public boolean isEquivalent(DistributionCol col1, DistributionCol col2) {
        return find(col1).equals(find(col2));
    }
}
