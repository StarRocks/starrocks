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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.ArrayList;
import java.util.List;

/*
Phase 1: for the most common case, subfield expr only exists in one on the ColumnRefMap's value of projection.
 */

public class SubfieldExprNoCopyRule extends TransformationRule {
    public SubfieldExprNoCopyRule() {
        super(RuleType.TF_SUBFILED_NOCOPY, Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // project expression
        LogicalProjectOperator project = input.getOp().cast();
        List<ScalarOperator> projectMapValues = new ArrayList<>(project.getColumnRefMap().values());
        for (int i = 0; i < projectMapValues.size(); i++) {
            ScalarOperator value = projectMapValues.get(i);
            // only deal with subfield expr of slotRef
            if (value instanceof SubfieldOperator && value.getChild(0) instanceof ColumnRefOperator) {
                SubfieldOperator subfield = value.cast();
                ColumnRefOperator col = value.getChild(0).cast();
                SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
                // collect other expr that used the same root slot
                for (int j = 0; j < projectMapValues.size(); j++) {
                    if (j != i && projectMapValues.get(j).getUsedColumns().contains(col)) {
                        projectMapValues.get(j).accept(collector, null);
                    }
                }
                List<ScalarOperator> allSubfieldExpr = Lists.newArrayList();
                allSubfieldExpr.addAll(collector.getComplexExpressions());
                // normalize access path
                SubfieldAccessPathNormalizer normalizer = new SubfieldAccessPathNormalizer();
                normalizer.collect(allSubfieldExpr);
                // no other usage
                if (!normalizer.hasPath(col)) {
                    subfield.setCopyFlag(false);
                    continue;
                }
                ColumnAccessPath p = normalizer.normalizePath(col, col.getName());
                // no overlap, overlap means other expr use father or child or self
                if (!p.hasOverlap(subfield.getFieldNames())) {
                    subfield.setCopyFlag(false);
                }
            }
        }

        return Lists.newArrayList(input);
    }
}
