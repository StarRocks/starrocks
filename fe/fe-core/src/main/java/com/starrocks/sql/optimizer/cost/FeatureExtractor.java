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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;
import java.util.Map;

/**
 * Extract features from physical plan
 */
public class FeatureExtractor {

    private static final ImmutableSet<OperatorType> EXCLUDE_OPERATORS = ImmutableSet.of(
            OperatorType.PHYSICAL_MERGE_JOIN,
            OperatorType.PHYSICAL_ICEBERG_SCAN,
            OperatorType.PHYSICAL_HIVE_SCAN,
            OperatorType.PHYSICAL_FILE_SCAN,
            OperatorType.PHYSICAL_ICEBERG_EQUALITY_DELETE_SCAN,
            OperatorType.PHYSICAL_HUDI_SCAN,
            OperatorType.PHYSICAL_DELTALAKE_SCAN,
            OperatorType.PHYSICAL_PAIMON_SCAN,
            OperatorType.PHYSICAL_ODPS_SCAN,
            OperatorType.PHYSICAL_ICEBERG_METADATA_SCAN,
            OperatorType.PHYSICAL_KUDU_SCAN,
            OperatorType.PHYSICAL_SCHEMA_SCAN,
            OperatorType.PHYSICAL_MYSQL_SCAN,
            OperatorType.PHYSICAL_META_SCAN,
            OperatorType.PHYSICAL_ES_SCAN,
            OperatorType.PHYSICAL_JDBC_SCAN,
            OperatorType.PHYSICAL_STREAM_SCAN,
            OperatorType.PHYSICAL_STREAM_JOIN,
            OperatorType.PHYSICAL_STREAM_AGG,
            OperatorType.PHYSICAL_TABLE_FUNCTION_TABLE_SCAN
    );

    // A trivial implement of feature extracting
    // TODO: implement sophisticated feature extraction methods
    public static PlanFeatures flattenFeatures(OptExpression plan) {
        Extractor extractor = new Extractor();
        OperatorFeatures root = plan.getOp().accept(extractor, plan, null);

        // summarize by operator type
        Map<OperatorType, PlanFeatures.SummarizedFeature> sumVector = Maps.newHashMap();
        sumByOperatorType(root, sumVector);
        PlanFeatures result = new PlanFeatures();

        // tables
        PlanFeatures.SummarizedFeature scanOperators = sumVector.get(OperatorType.PHYSICAL_OLAP_SCAN);
        result.addTableFeatures(scanOperators.tableSet);

        // Add operator features
        for (int start = OperatorType.PHYSICAL.ordinal();
                start < OperatorType.SCALAR.ordinal();
                start++) {
            OperatorType opType = OperatorType.values()[start];
            if (EXCLUDE_OPERATORS.contains(opType)) {
                continue;
            }
            PlanFeatures.SummarizedFeature vector = sumVector.get(opType);
            if (vector != null) {
                result.addOperatorFeature(vector.finish());
            } else {
                result.addOperatorFeature(PlanFeatures.SummarizedFeature.empty(opType));
            }
        }

        return result;
    }

    private static void sumByOperatorType(OperatorFeatures tree,
                                          Map<OperatorType, PlanFeatures.SummarizedFeature> sum) {
        List<Long> vector = tree.toVector();
        OperatorType opType = tree.opType;
        PlanFeatures.SummarizedFeature exist =
                sum.computeIfAbsent(opType, (x) -> new PlanFeatures.SummarizedFeature(opType));
        exist.merge(tree);

        // recursive
        for (var child : tree.getChildren()) {
            sumByOperatorType(child, sum);
        }
    }

    static class Extractor extends OptExpressionVisitor<OperatorFeatures, Void> {

        @Override
        public OperatorFeatures visit(OptExpression optExpression, Void context) {
            Statistics stats = optExpression.getStatistics();
            CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));

            OperatorFeatures node = OperatorFeatures.build(optExpression, cost, stats);

            // recursive visit
            for (var child : optExpression.getInputs()) {
                OperatorFeatures childNode = visit(child, context);
                node.addChild(childNode);
            }

            return node;
        }

    }
}
