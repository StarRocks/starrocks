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

package com.starrocks.sql.optimizer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;
import java.util.Objects;

// Cardinality-preserving edge, it is constructed from CPBiRels, identical CPEdge
// will be merged into one. a unilateral CPEdge and its inverse unilateral CPEdge
// will be merged into bilateral CPEdge. CPEdges are used to construct
// cardinality-preserving tree consisting of CPNodes.
public class CPEdge {
    private final OptExpression lhs;
    private final OptExpression rhs;
    private final boolean unilateral;
    final BiMap<ColumnRefOperator, ColumnRefOperator> eqColumnRefs;

    public CPEdge(OptExpression lhs, OptExpression rhs, boolean unilateral,
                  Map<ColumnRefOperator, ColumnRefOperator> eqColumnRefs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.unilateral = unilateral;
        this.eqColumnRefs = HashBiMap.create(eqColumnRefs);
    }

    CPEdge inverse() {
        return new CPEdge(rhs, lhs, unilateral, eqColumnRefs.inverse());
    }

    CPEdge toUniLateral() {
        if (unilateral) {
            return this;
        } else {
            return new CPEdge(lhs, rhs, true, eqColumnRefs);
        }
    }

    CPEdge toBiLateral() {
        if (!unilateral) {
            return this;
        } else {
            return new CPEdge(lhs, rhs, false, eqColumnRefs);
        }
    }

    public OptExpression getLhs() {
        return lhs;
    }

    public OptExpression getRhs() {
        return rhs;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getEqColumnRefs() {
        return eqColumnRefs;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getInverseEqColumnRefs() {
        return eqColumnRefs.inverse();
    }

    public boolean isUnilateral() {
        return unilateral;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CPEdge that = (CPEdge) o;
        return unilateral == that.unilateral && Objects.equals(lhs, that.lhs) &&
                Objects.equals(rhs, that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs, rhs, unilateral);
    }
}
