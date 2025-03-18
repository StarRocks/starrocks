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

package com.starrocks.qe.feedback.guide;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.qe.feedback.skeleton.JoinNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;

import static com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityRule.JOIN_COMMUTATIVITY_MAP;

public abstract class JoinTuningGuide implements TuningGuide {

    protected JoinNode joinNode;

    protected EstimationErrorType type;

    protected static final long BROADCAST_THRESHOLD = 1000000;

    protected PhysicalHashJoinOperator buildJoinOperator(PhysicalHashJoinOperator joinOperator, boolean needCommute) {
        JoinOperator joinType = joinOperator.getJoinType();
        if (needCommute) {
            joinType = JOIN_COMMUTATIVITY_MAP.get(joinOperator.getJoinType());
        }
        return new PhysicalHashJoinOperator(
                joinType,
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection(),
                joinOperator.getSkewColumn(),
                joinOperator.getSkewValues());
    }

    protected boolean isColocateJoin(OptExpression optExpression) {
        // through the required properties type check if it is colocate join
        return optExpression.getRequiredProperties().stream().allMatch(
                physicalPropertySet -> {
                    if (!physicalPropertySet.getDistributionProperty().isShuffle()) {
                        return false;
                    }
                    HashDistributionDesc.SourceType hashSourceType =
                            ((HashDistributionSpec) (physicalPropertySet.getDistributionProperty().getSpec()))
                                    .getHashDistributionDesc().getSourceType();
                    return hashSourceType.equals(HashDistributionDesc.SourceType.LOCAL);
                });
    }

    protected boolean isShuffleJoin(OptExpression leftChild, OptExpression rightChild) {
        DistributionSpec.DistributionType leftDistributionType = getDistributionType(leftChild);
        DistributionSpec.DistributionType rightDistributionType = getDistributionType(rightChild);
        return leftDistributionType == DistributionSpec.DistributionType.SHUFFLE
                && rightDistributionType == DistributionSpec.DistributionType.SHUFFLE;
    }
    protected boolean isBroadcastJoin(OptExpression rightChild) {
        return getDistributionType(rightChild) == DistributionSpec.DistributionType.BROADCAST;
    }

    protected DistributionSpec.DistributionType getDistributionType(OptExpression optExpression) {
        if (optExpression.getOp() instanceof PhysicalDistributionOperator) {
            return ((PhysicalDistributionOperator) optExpression.getOp()).getDistributionSpec().getType();
        }
        return null;
    }

    protected PhysicalPropertySet createBroadcastPropertySet() {
        return new PhysicalPropertySet(DistributionProperty.createProperty(
                DistributionSpec.createReplicatedDistributionSpec()));
    }

    protected PhysicalPropertySet createShufflePropertySet(DistributionSpec spec) {
        return new PhysicalPropertySet(DistributionProperty.createProperty(spec));
    }


    public enum EstimationErrorType {
        LEFT_INPUT_UNDERESTIMATED,
        LEFT_INPUT_OVERESTIMATED,
        RIGHT_INPUT_UNDERESTIMATED,
        RIGHT_INPUT_OVERESTIMATED
    }
}
