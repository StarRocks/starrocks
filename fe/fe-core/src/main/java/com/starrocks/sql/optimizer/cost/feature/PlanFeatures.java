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

package com.starrocks.sql.optimizer.cost.feature;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
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

    private static final ImmutableSet<OperatorType> EXCLUDE_OPERATORS = ImmutableSet.of(
            OperatorType.PHYSICAL_MERGE_JOIN,
            OperatorType.PHYSICAL_STREAM_SCAN,
            OperatorType.PHYSICAL_STREAM_JOIN,
            OperatorType.PHYSICAL_STREAM_AGG
    );

    // query plan
    private static final int TOP_N_TABLES = 3;
    private static final int NUM_ENV_FEATURES = 3;
    private static final int NUM_VAR_FEATURES = 1;
    private static final int NUM_OPERATOR_FEATURES;

    static {
        int num = 0;
        for (int start = OperatorType.PHYSICAL.ordinal() + 1;
                start < OperatorType.SCALAR.ordinal();
                start++) {
            OperatorType opType = OperatorType.values()[start];
            if (skipOperator(opType)) {
                continue;
            }
            num += OperatorFeatures.vectorLength(opType);
            num += AggregatedFeature.numExtraFeatures(opType);
        }
        NUM_OPERATOR_FEATURES = num;
    }

    private List<Long> operatorFeatureVectors;
    private final Set<Table> tables = Sets.newHashSet();

    // environment
    private long numBeNodes;
    private long avgCpuCoreOfBE;
    private long memCapacityOfBE;

    // variables
    private long dop;

    public static String featuresHeader() {
        final String DELIMITER = ",";
        List<String> fields = Lists.newArrayList();
        for (int i = 0; i < TOP_N_TABLES; i++) {
            fields.add("tables_" + i);
        }
        for (int i = 0; i < NUM_ENV_FEATURES; i++) {
            fields.add("env_" + i);
        }
        for (int i = 0; i < NUM_VAR_FEATURES; i++) {
            fields.add("var_" + i);
        }
        for (int i = 0; i < NUM_OPERATOR_FEATURES; i++) {
            fields.add("operators_" + i);
        }

        return Joiner.on(DELIMITER).join(fields);
    }

    /**
     * The string representation like: tables=[1,2,3]|operators=[4,5,6]|....
     */
    public String toFeatureString() {
        StringBuilder sb = new StringBuilder();

        var topTables = extractTopTables();
        sb.append("tables=[").append(Joiner.on(",").join(topTables));
        sb.append("]|");

        sb.append(String.format("env=[%d,%d,%d]|", numBeNodes, avgCpuCoreOfBE, (long) Math.log1p(memCapacityOfBE)));
        sb.append(String.format("var=[%d]|", dop));
        sb.append("operators=[").append(Joiner.on(",").join(operatorFeatureVectors));
        sb.append("]");

        return sb.toString();
    }

    /**
     * The CSV representation like tables_0,tables_1,tables_2,operators_0,operators_1....
     *
     * @return CSV format string
     */
    public String toFeatureCsv() {
        final String DELIMITER = ",";
        StringBuilder sb = new StringBuilder();

        var topTables = extractTopTables();
        for (var tableId : topTables) {
            sb.append(String.valueOf(tableId)).append(DELIMITER);
        }
        // env
        sb.append(numBeNodes).append(DELIMITER);
        sb.append(avgCpuCoreOfBE).append(DELIMITER);
        sb.append((long) Math.log1p(memCapacityOfBE)).append(DELIMITER);
        // var
        sb.append(dop).append(DELIMITER);
        // operators
        for (long operatorFeature : operatorFeatureVectors) {
            sb.append(operatorFeature).append(DELIMITER);
        }

        String result = sb.toString();
        return result.substring(0, result.length() - 1);
    }

    public void setNumBeNodes(int numBeNodes) {
        this.numBeNodes = numBeNodes;
    }

    public void setAvgCpuCoreOfBe(int avgCpuCoreOfBe) {
        this.avgCpuCoreOfBE = avgCpuCoreOfBe;
    }

    public void setMemCapacityOfBE(long memCapacityOfBE) {
        this.memCapacityOfBE = memCapacityOfBE;
    }

    public void setDop(int dop) {
        this.dop = dop;
    }

    public void addTableFeatures(Set<Table> tables) {
        this.tables.addAll(tables);
    }

    public void addOperatorFeatures(Map<OperatorType, AggregatedFeature> operatorFeatures) {
        // Make sure all plan have equal-size vector
        List<Long> operatorVector = Lists.newArrayList();
        for (int start = OperatorType.PHYSICAL.ordinal() + 1;
                start < OperatorType.SCALAR.ordinal();
                start++) {
            OperatorType opType = OperatorType.values()[start];
            if (skipOperator(opType)) {
                continue;
            }
            AggregatedFeature vector = operatorFeatures.get(opType);
            if (vector != null) {
                operatorVector.addAll(vector.finish());
            } else {
                operatorVector.addAll(AggregatedFeature.empty(opType));
            }
        }
        this.operatorFeatureVectors = operatorVector;
    }

    public static boolean skipOperator(OperatorType operatorType) {
        if (EXCLUDE_OPERATORS.contains(operatorType)) {
            return true;
        }
        /*
          {@link OperatorFeatures.ScanOperatorFeatures}
         */
        if (operatorType.isPhysicalScan() && (operatorType != OperatorType.PHYSICAL_OLAP_SCAN)) {
            return true;
        }
        return false;
    }

    public static Map<OperatorType, AggregatedFeature> aggregate(OperatorFeatures tree) {
        Map<OperatorType, AggregatedFeature> sum = Maps.newHashMap();
        aggregate(tree, sum);
        return sum;
    }

    private static void aggregate(OperatorFeatures tree, Map<OperatorType, AggregatedFeature> sum) {
        OperatorType opType = tree.opType;
        AggregatedFeature exist = sum.computeIfAbsent(opType, (x) -> new AggregatedFeature(opType));
        exist.merge(tree);

        for (var child : tree.getChildren()) {
            aggregate(child, sum);
        }
    }


    private List<Long> extractTopTables() {

        List<Long> result = Lists.newArrayList();
        for (int i = 0; i < TOP_N_TABLES; i++) {
            result.add(0L);
        }
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

    /**
     * Aggregate vectors of same operators
     */
    public static class AggregatedFeature {
        private final OperatorType opType;
        private final List<Long> vector;
        private int count = 0;

        public AggregatedFeature(OperatorType type) {
            this.opType = type;
            int len = OperatorFeatures.vectorLength(type);
            this.vector = Lists.newArrayListWithCapacity(len);
            for (int i = 0; i < len; i++) {
                this.vector.add(0L);
            }
        }

        public void merge(OperatorFeatures node) {
            this.count++;
            // vector add
            List<Long> vector1 = node.toVector();
            for (int i = 0; i < vector.size(); i++) {
                this.vector.set(i, this.vector.get(i) + vector1.get(i));
            }
        }

        /**
         * Build a real vector, with OPERATOR_TYPE and COUNT prefix
         */
        public List<Long> finish() {
            List<Long> result = Lists.newArrayList();
            result.add((long) opType.ordinal());
            result.add((long) count);
            if (CollectionUtils.isNotEmpty(vector)) {
                result.addAll(vector);
            }
            return result;
        }

        public static int numExtraFeatures(OperatorType type) {
            return 2;
        }

        public static List<Long> empty(OperatorType type) {
            List<Long> result = Lists.newArrayList();
            result.add((long) type.ordinal());
            result.add((long) 0);
            int len = OperatorFeatures.vectorLength(type);
            for (int i = 0; i < len; i++) {
                result.add(0L);
            }
            return result;
        }

    }
}
