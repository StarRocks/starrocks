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
package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PhysicalSplitConsumeOperator extends PhysicalOperator {
    private final int splitId;
    private final List<ColumnRefOperator> outputColumnRefOp;
    private final ScalarOperator splitPredicate;
    // Global dictionaries and derived-dictionary expressions the consuming fragment needs, carried over
    // from the exchange this operator replaces (see SkewShuffleJoinEliminationRule). The low-cardinality
    // rewrite attaches them to the exchange; the fragment built for this consumer must still receive them.
    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
    private Map<Integer, ScalarOperator> globalDictsExpr = Maps.newHashMap();

    public PhysicalSplitConsumeOperator(int splitId, ScalarOperator splitPredicate, DistributionSpec distributionSpec,
                                        List<ColumnRefOperator> outputColumnRefOp) {
        // distributionSpec specifies the distribution of the input of this operator
        super(OperatorType.PHYSICAL_SPLIT_CONSUME, distributionSpec);
        this.splitId = splitId;
        this.splitPredicate = splitPredicate;
        this.outputColumnRefOp = outputColumnRefOp;
    }

    public int getSplitId() {
        return splitId;
    }

    public ScalarOperator getSplitPredicate() {
        return splitPredicate;
    }

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(List<Pair<Integer, ColumnDict>> globalDicts) {
        this.globalDicts = globalDicts;
    }

    public Map<Integer, ScalarOperator> getGlobalDictsExpr() {
        return globalDictsExpr;
    }

    public void setGlobalDictsExpr(Map<Integer, ScalarOperator> globalDictsExpr) {
        this.globalDictsExpr = globalDictsExpr;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> columnOutputInfoList = Lists.newArrayList();
        outputColumnRefOp.stream().forEach(e -> columnOutputInfoList.add(new ColumnOutputInfo(e, e)));
        return new RowOutputInfo(columnOutputInfoList, outputColumnRefOp);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalSplitConsumer(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSplitConsumeOperator that = (PhysicalSplitConsumeOperator) o;
        return Objects.equals(splitId, that.splitId) && Objects.equals(splitPredicate, that.splitPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), splitId, splitPredicate);
    }

    @Override
    public String toString() {
        return "PhysicalSplitConsumeOperator{" + "splitId='" + splitId + '\'' + ", predicate=" + splitPredicate + '}';
    }

}
