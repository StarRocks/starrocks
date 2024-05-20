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

package com.starrocks.sql.automv.pn;

import com.starrocks.common.util.UnionFind;

import java.util.Objects;

public class StrictOp {
    private static final UnionFind<Integer> EMPTY_UNION_FIND = new UnionFind<Integer>().sealed();
    private final Op op;
    private final UnionFind<Integer> equivColRefs;

    public StrictOp(Op op) {
        this(op, EMPTY_UNION_FIND);
    }

    public StrictOp(Op op, UnionFind<Integer> equivColRefs) {
        this.op = op;
        this.equivColRefs = equivColRefs;
    }

    public Op getOp() {
        return op;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StrictOp that = (StrictOp) o;

        return this.equivColRefs == that.equivColRefs && Op.equivalent(this.op, that.op, equivColRefs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, op.getSymTab());
    }
}