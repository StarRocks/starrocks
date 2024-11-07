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


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

// For meta scan query:
//     select flat_json_meta(a) from test_all_type [_META_]
// to: select flat_json_meta_a  from test_all_type [_META_] group by flat_json_meta_a
public class PushDownFlatJsonMetaToMetaScanRule extends TransformationRule {
    public PushDownFlatJsonMetaToMetaScanRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_META_SCAN,
                Pattern.create(OperatorType.LOGICAL_AGGR).
                        addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.LOGICAL_META_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        boolean hasFlatJsonMeta = false;
        for (CallOperator aggCall : agg.getAggregations().values()) {
            String aggFuncName = aggCall.getFnName();
            if (aggFuncName.equalsIgnoreCase(FunctionSet.FLAT_JSON_META)) {
                if (!aggCall.isVariable()) {
                    throw new SemanticException("flat_json_meta must query on column");
                }
                hasFlatJsonMeta = true;
            }
        }

        if (!hasFlatJsonMeta) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(agg.getGroupingKeys())) {
            throw new SemanticException("flat_json_meta don't support group-by");
        }

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.inputAt(0).getOp();
        if (projectOperator.getColumnRefMap().entrySet().stream().anyMatch(e -> !e.getKey().equals(e.getValue()))) {
            throw new SemanticException("flat_json_meta don't support complex project");
        }

        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();
        if (!metaScan.getAggColumnIdToNames().isEmpty()) {
            throw new SemanticException("flat_json_meta don't support complex meta");
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalMetaScanOperator metaScan = (LogicalMetaScanOperator) input.inputAt(0).inputAt(0).getOp();

        Map<ColumnRefOperator, CallOperator> newAggCalls = Maps.newHashMap();
        List<ColumnRefOperator> newAggGroupBys = Lists.newArrayList();

        Map<Integer, String> aggColumnIdToNames = Maps.newHashMap();
        Map<ColumnRefOperator, Column> newScanColumnRefs = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> kv : agg.getAggregations().entrySet()) {
            CallOperator aggCall = kv.getValue();
            ColumnRefOperator metaRef = kv.getKey();
            if (!aggCall.getFnName().equals(FunctionSet.FLAT_JSON_META)) {
                newAggCalls.put(kv.getKey(), kv.getValue());
                continue;
            }
            ColumnRefOperator usedColumn = aggCall.getColumnRefs().get(0);
            String metaColumnName = aggCall.getFnName() + "_" + usedColumn.getName();
            aggColumnIdToNames.put(metaRef.getId(), metaColumnName);
            Column c = metaScan.getColRefToColumnMetaMap().get(usedColumn);
            newScanColumnRefs.put(metaRef, c);
            newAggGroupBys.add(metaRef);
        }

        LogicalMetaScanOperator newMetaScan = LogicalMetaScanOperator.builder().withOperator(metaScan)
                .setColRefToColumnMetaMap(newScanColumnRefs)
                .setAggColumnIdToNames(aggColumnIdToNames)
                .build();

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator(
                agg.getType(), newAggGroupBys, newAggCalls);

        // all used columns from aggCalls are from newMetaScan, we can remove the old project directly.
        return Lists.newArrayList(OptExpression.create(newAggOperator, OptExpression.create(newMetaScan)));
    }
}