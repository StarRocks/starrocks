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
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
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
 * RowOutputInfo is used to describe the info of output columns returned by an operator.
 * It consists of a set of columnOutputInfo. Because of the Projection field in Operator,
 * an operator with a not null projection may take the original output of this operator
 * and project it to a new output.
 * <p>
 * To unify the output info of an operator, we use the RowOutputInfo to describe the output
 * row of this operator.
 * When an operator with a not null projection, the RowOutputInfo records the set of
 * columnOutputInfo of the projection.
 * When an operator without a not null projection, the RowOutInfo records the set of
 * columnOutputInfo of itself.
 */
public class RowOutputInfo {

    private final Map<Integer, ColumnOutputInfo> columnOutputInfoMap;

    public static RowOutputInfo createEmptyDescriptor() {
        return new RowOutputInfo();
    }

    private RowOutputInfo() {
        this.columnOutputInfoMap = Maps.newHashMap();
    }

    public RowOutputInfo(Collection<ColumnOutputInfo> columnEntries) {
        Map<Integer, ColumnOutputInfo> map = Maps.newHashMap();
        for (ColumnOutputInfo columnOutputInfo : columnEntries) {
            map.put(columnOutputInfo.getColId(), columnOutputInfo);
        }
        this.columnOutputInfoMap = map;
    }

    public RowOutputInfo(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Map<Integer, ColumnOutputInfo> map = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            map.put(entry.getKey().getId(), new ColumnOutputInfo(entry));
        }
        this.columnOutputInfoMap = map;
    }

    public List<ColumnOutputInfo> getColumnEntries() {
        return Lists.newArrayList(columnOutputInfoMap.values());
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnOutputInfoMap.values().stream()
                .collect(Collectors.toMap(ColumnOutputInfo::getColumnRef, ColumnOutputInfo::getScalarOp));
    }

    public ColumnRefSet getOutputColumnRefSet() {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (Integer colId : columnOutputInfoMap.keySet()) {
            columnRefSet.union(colId);
        }

        return columnRefSet;
    }

    public ColumnRefSet getUsedColumnRefSet() {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (ColumnOutputInfo entry : getColumnEntries()) {
            columnRefSet.union(entry.getScalarOp().getUsedColumns());
        }
        return columnRefSet;
    }

    public int getColumnCount() {
        return columnOutputInfoMap.size();
    }

    public ColumnOutputInfo rewriteColWithRowInfo(ColumnOutputInfo columnOutputInfo) {
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(getColumnRefMap());
        return new ColumnOutputInfo(columnOutputInfo.getColumnRef(), rewriter.rewrite(columnOutputInfo.getScalarOp()));
    }

    public RowOutputInfo addColsToRow(List<ColumnOutputInfo> entryList, boolean existProjection) {
        List<ColumnOutputInfo> newCols = Lists.newArrayList();
        if (existProjection) {
            newCols.addAll(getColumnEntries());
            for (ColumnOutputInfo entry : entryList) {
                ColumnOutputInfo newEntry = rewriteColWithRowInfo(entry);
                newCols.add(newEntry);
            }
        } else {
            for (ColumnOutputInfo entry : getColumnEntries()) {
                newCols.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
            }
            newCols.addAll(entryList);
        }
        return new RowOutputInfo(newCols);
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
        if (!(obj instanceof RowOutputInfo)) {
            return false;
        }

        RowOutputInfo that = (RowOutputInfo) obj;

        return Objects.equals(getOutputColumnRefSet(), that.getOutputColumnRefSet());
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        for (ColumnOutputInfo entry : columnOutputInfoMap.values()) {
            joiner.add(entry.toString());
        }
        return joiner.toString();
    }
}
