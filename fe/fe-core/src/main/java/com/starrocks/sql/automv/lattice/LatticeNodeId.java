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

package com.starrocks.sql.automv.lattice;

import com.google.common.base.Preconditions;
import com.starrocks.sql.automv.column.GenericColumn;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LatticeNodeId {
    private final ColumnRefSet value;
    private transient String str = null;
    private transient List<Integer> columnOrdinals = null;

    private LatticeNodeId(ColumnRefSet value) {
        this.value = Objects.requireNonNull(value);
    }

    public static LatticeNodeId calcRootId(int width) {
        Preconditions.checkArgument(width > 0);
        ColumnRefSet value = ColumnRefSet.createByIds(IntStream.range(0, width).boxed().collect(Collectors.toList()));
        return new LatticeNodeId(value);
    }

    public static LatticeNodeId calcId(Collection<GenericColumn> dimensions, Map<String, Integer> columnToOrdinalMap) {
        ColumnRefSet value = ColumnRefSet.of();
        for (GenericColumn dim : dimensions) {
            value.union(columnToOrdinalMap.get(dim.getNorm().toString()));
        }
        return new LatticeNodeId(value);
    }

    public LatticeNodeId merge(LatticeNodeId that) {
        ColumnRefSet value = ColumnRefSet.of();
        value.union(this.value);
        value.union(that.value);
        return new LatticeNodeId(value);
    }

    public LatticeNodeId diff(LatticeNodeId that) {
        ColumnRefSet value = ColumnRefSet.of();
        value.union(this.value);
        value.except(that.value);
        return new LatticeNodeId(value);
    }

    public boolean isCoveredBy(LatticeNodeId that) {
        return that.isCovering(this);
    }

    public boolean isCoveredStrictlyBy(LatticeNodeId that) {
        return that.isCoveringStrictly(this);
    }

    public boolean isCovering(LatticeNodeId that) {
        Order ord = this.compare(that);
        return ord.equals(Order.MORE_THAN) || ord.equals(Order.EQUAL);
    }

    public boolean isCoveringStrictly(LatticeNodeId that) {
        Order ord = this.compare(that);
        return ord.equals(Order.MORE_THAN);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LatticeNodeId that = (LatticeNodeId) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    private Order compare(LatticeNodeId that) {
        Preconditions.checkArgument(that != null);
        if (this.equals(that)) {
            return Order.EQUAL;
        } else {
            if (that.value.containsAll(this.value)) {
                return Order.LESS_THAN;
            } else if (this.value.containsAll(that.value)) {
                return Order.MORE_THAN;
            } else {
                return Order.NONE;
            }
        }
    }

    @Override
    public String toString() {
        if (str == null) {
            str = value.toString();
        }
        return str;
    }

    public List<Integer> getColumnOrdinals() {
        if (columnOrdinals == null) {
            columnOrdinals = value.getStream().collect(Collectors.toList());
        }
        return columnOrdinals;
    }

    public int size() {
        return value.size();
    }

    public boolean isOverlappingPartially(LatticeNodeId that) {
        ColumnRefSet mergedValue = ColumnRefSet.of();
        mergedValue.union(this.value);
        mergedValue.union(that.value);
        return mergedValue.size() > this.value.size() &&
                mergedValue.size() > that.value.size() &&
                this.value.size() + that.value.size() > mergedValue.size();
    }

    enum Order {
        LESS_THAN,
        EQUAL,
        MORE_THAN,
        NONE
    }
}
