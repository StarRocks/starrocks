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

import com.starrocks.common.util.UnionFind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

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
    private static final Logger LOG = LogManager.getLogger(DistributionDisjointSet.class);
    private final UnionFind<DistributionCol> parent;

    public DistributionDisjointSet() {
        this.parent = new UnionFind<>();
    }

    public DistributionDisjointSet(UnionFind<DistributionCol> parent) {
        this.parent = parent;
    }

    public UnionFind<DistributionCol> getParent() {
        return parent;
    }

    public void add(DistributionCol col) {
        parent.add(col);
    }

    public void union(DistributionCol col1, DistributionCol col2) {
        parent.union(col1, col2);
    }

    public DistributionDisjointSet copy() {
        return new DistributionDisjointSet(parent.copy());
    }

    public boolean isEquivalent(DistributionCol col1, DistributionCol col2) {
        add(col1);
        add(col2);

        Optional<Integer> opt1 = parent.getGroupId(col1);
        Optional<Integer> opt2 = parent.getGroupId(col2);
        return opt1.get() == opt2.get();
    }
}
