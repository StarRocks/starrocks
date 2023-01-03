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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnEntry;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * RowDescriptor is used to describe the output row info returned by an operator.
 * Because of the Projection field in Operator, an operator with a not null projection may take
 * an input row and yield a different output row.
 * To unify the input/output info of an operator, we use the RowDescriptor to describe the input/output
 * row info received/returned by an operator.
 */
public class RowDescriptor {

    private final Map<Integer, ColumnEntry> columnEntryMap;

    public static RowDescriptor createEmptyDescriptor() {
        return new RowDescriptor();
    }

    private RowDescriptor() {
        this.columnEntryMap = Maps.newHashMap();
    }

    public RowDescriptor(Collection<ColumnEntry> columnEntries) {
        Map<Integer, ColumnEntry> map = Maps.newHashMap();
        for (ColumnEntry columnEntry : columnEntries) {
            map.put(columnEntry.getColId(), columnEntry);
        }
        this.columnEntryMap = map;
    }

    public RowDescriptor(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Map<Integer, ColumnEntry> map = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            map.put(entry.getKey().getId(), new ColumnEntry(entry));
        }
        this.columnEntryMap = map;
    }

    public List<ColumnEntry> getColumnEntries() {
        return Lists.newArrayList(columnEntryMap.values());
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnEntryMap.values().stream()
                .collect(Collectors.toMap(ColumnEntry::getColumnRef, ColumnEntry::getScalarOp));
    }

    public ColumnRefSet getOutputColumnRefSet() {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (Integer colId : columnEntryMap.keySet()) {
            columnRefSet.union(colId);
        }

        return columnRefSet;
    }

    public ColumnRefSet getUsedColumnRefSet() {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (ColumnEntry entry : getColumnEntries()) {
            columnRefSet.union(entry.getScalarOp().getUsedColumns());
        }
        return columnRefSet;
    }

    public int getColumnCount() {
        return columnEntryMap.size();
    }

    public ColumnEntry rewriteColWithRowInfo(ColumnEntry columnEntry) {
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(getColumnRefMap());
        return new ColumnEntry(columnEntry.getColumnRef(), rewriter.rewrite(columnEntry.getScalarOp()));
    }

    public RowDescriptor addColsToRow(List<ColumnEntry> entryList, boolean existProjection) {
        List<ColumnEntry> newCols = Lists.newArrayList();
        if (existProjection) {
            newCols.addAll(getColumnEntries());
            for (ColumnEntry entry : entryList) {
                ColumnEntry newEntry = rewriteColWithRowInfo(entry);
                newCols.add(newEntry);
            }
        } else {
            for (ColumnEntry entry : getColumnEntries()) {
                newCols.add(new ColumnEntry(entry.getColumnRef(), entry.getColumnRef()));
            }
            newCols.addAll(entryList);
        }
        return new RowDescriptor(newCols);
    }

    @Override
    public int hashCode() {
        return getOutputColumnRefSet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowDescriptor)) {
            return false;
        }

        RowDescriptor that = (RowDescriptor) obj;

        return Objects.equals(getOutputColumnRefSet(), that.getOutputColumnRefSet());
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (ColumnEntry entry : columnEntryMap.values()) {
            joiner.add(entry.toString());
        }
        return joiner.toString();
    }
}
