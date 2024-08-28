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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.feedback.skeleton.StreamingAggNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;

import java.util.Optional;

public class StreamingAggTuningGuide implements TuningGuide {

    private final StreamingAggNode skeletonNode;

    public StreamingAggTuningGuide(StreamingAggNode skeletonNode) {
        this.skeletonNode = skeletonNode;
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reason: AggNode ").append(skeletonNode.getNodeId()).append(", ");
        sb.append("group by keys: ");
        sb.append(skeletonNode.getGroupBys().toString()).append(" ");
        sb.append("has good aggregation effect.");
        return sb.toString();
    }

    @Override
    public String getAdvice() {
        return "Advice: Use force_preaggregation mode in this streaming agg node.";
    }

    @Override
    public Optional<OptExpression> applyImpl(OptExpression optExpression) {
        PhysicalHashAggregateOperator aggregateOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
        if (ConnectContext.get().getSessionVariable().getStreamingPreaggregationMode().equals("auto")) {
            aggregateOperator.setForcePreAggregation(true);
        }
        OptExpression.Builder builder = OptExpression.builder().with(optExpression).setOp(aggregateOperator);

        return Optional.of(builder.build());
    }


}
