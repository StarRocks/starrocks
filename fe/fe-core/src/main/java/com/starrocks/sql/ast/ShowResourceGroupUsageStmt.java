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

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.structure.Pair;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.ComputeNode;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShowResourceGroupUsageStmt extends ShowStmt {
    private static final List<Pair<Column, Function<ShowItem, String>>> META_DATA =
            ImmutableList.of(
                    Pair.create(new Column("Name", ScalarType.createVarchar(64)),
                            item -> item.usage.getGroup().getName()),
                    Pair.create(new Column("Id", ScalarType.createVarchar(64)),
                            item -> Long.toString(item.usage.getGroup().getId())),
                    Pair.create(new Column("Backend", ScalarType.createVarchar(64)),
                            item -> item.worker.getHost()),
                    Pair.create(new Column("BEInUseCpuCores", ScalarType.createVarchar(64)),
                            item -> Double.toString(item.usage.getCpuCoreUsagePermille() / 1000.0D)),
                    Pair.create(new Column("BEInUseMemBytes", ScalarType.createVarchar(64)),
                            item -> Long.toString(item.usage.getMemUsageBytes())),
                    Pair.create(new Column("BERunningQueries", ScalarType.createVarchar(64)),
                            item -> Integer.toString(item.usage.getNumRunningQueries()))
            );

    private static final ShowResultSetMetaData COLUMN_META_DATA;

    private static final List<Function<ShowItem, String>> COLUMN_SUPPLIERS = META_DATA.stream()
            .map(item -> item.second).collect(Collectors.toList());

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        META_DATA.forEach(item -> builder.addColumn(item.first));
        COLUMN_META_DATA = builder.build();
    }

    private final String groupName;

    public ShowResourceGroupUsageStmt(String groupName, NodePosition pos) {
        super(pos);
        this.groupName = groupName;
    }

    public static List<Function<ShowItem, String>> getColumnSuppliers() {
        return COLUMN_SUPPLIERS;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowResourceGroupUsageStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return COLUMN_META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public String getGroupName() {
        return groupName;
    }

    public static class ShowItem implements Comparable<ShowItem> {
        final ComputeNode worker;
        final ComputeNode.ResourceGroupUsage usage;

        public ShowItem(ComputeNode worker, ComputeNode.ResourceGroupUsage usage) {
            this.worker = worker;
            this.usage = usage;
        }

        @Override
        public int compareTo(@NotNull ShowItem rhs) {
            return Comparator
                    .comparing((Function<ShowItem, Long>) item -> item.usage.getGroup().getId())
                    .thenComparing(item -> item.worker.getId())
                    .compare(this, rhs);
        }

        public ComputeNode getWorker() {
            return worker;
        }

        public ComputeNode.ResourceGroupUsage getUsage() {
            return usage;
        }
    }
}
