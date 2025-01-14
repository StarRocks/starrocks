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

package com.starrocks.sql.optimizer.cost;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import org.apache.commons.collections.CollectionUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanFeatures {
    private List<Long> vector = Lists.newArrayList();
    private Map<OperatorType, SummarizedFeature> operatorFeatures = Maps.newHashMap();
    private Set<Table> tables = Sets.newHashSet();

    public PlanFeatures() {
    }

    public PlanFeatures(List<Long> vector) {
        this.vector = vector;
    }

    public String toFeatureString() {
        return Joiner.on(",").join(vector);
    }

    public void addTableFeatures(Set<Table> tables) {
        this.tables.addAll(tables);
    }

    public void addOperatorFeature(PlanFeatures vector) {
        if (vector.vector != null) {
            this.vector.addAll(vector.vector);
        }
    }

    private List<Long> extractTopTables() {
        final int TOP_N_TABLES = 3;

        List<Long> result = Lists.newArrayList(0L, 0L, 0L);
        if (CollectionUtils.isNotEmpty(tables)) {
            result.addAll(tables.stream()
                    .sorted(Comparator.comparing(x -> ((OlapTable) x).getRowCount()).reversed())
                    .limit(TOP_N_TABLES)
                    .map(Table::getId)
                    .toList());
            result = result.subList(result.size() - TOP_N_TABLES, result.size());
        }
        return result;
    }

    static class SummarizedFeature {
        OperatorType opType;
        int count = 0;
        PlanFeatures vector;
        Set<Table> tableSet;

        SummarizedFeature(OperatorType type) {
            this.opType = type;
            if (type == OperatorType.PHYSICAL_OLAP_SCAN) {
                this.tableSet = Sets.newHashSet();
            }
        }

        public void merge(OperatorFeatures node) {
            this.count++;
            if (this.vector == null) {
                this.vector = new PlanFeatures(node.toVector());
            } else {
                // A + B => C
                List<Long> vector1 = node.toVector();
                for (int i = 0; i < vector.vector.size(); i++) {
                    this.vector.vector.set(i, this.vector.vector.get(i) + vector1.get(i));
                }
            }
            if (node instanceof OperatorFeatures.ScanOperatorFeatures scanNode) {
                this.tableSet.add(scanNode.getTable());
            }
        }

        public PlanFeatures finish() {
            List<Long> result = Lists.newArrayList();
            result.add((long) opType.ordinal());
            result.add((long) count);
            if (vector != null) {
                result.addAll(vector.vector);
            }
            return new PlanFeatures(result);
        }

        public static PlanFeatures empty(OperatorType type) {
            List<Long> result = Lists.newArrayList();
            result.add((long) type.ordinal());
            result.add((long) 0);
            for (int i = 0; i < OperatorFeatures.numFeatures(type); i++) {
                result.add(0L);
            }
            return new PlanFeatures(result);
        }

    }
}
