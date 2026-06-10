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

package com.starrocks.qe.feedback;

import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.feedback.guide.JoinTuningGuide;
import com.starrocks.qe.feedback.guide.StreamingAggTuningGuide;
import com.starrocks.qe.feedback.guide.TuningGuide;

public final class PlanAdvisorMetrics {
    public static final String JOIN_OPERATOR_TYPE = "join";
    public static final String AGG_OPERATOR_TYPE = "agg";

    private PlanAdvisorMetrics() {
    }

    public static void increaseGuideGenerated(TuningGuide guide) {
        String operatorType = resolveOperatorType(guide);
        if (operatorType != null) {
            MetricRepo.COUNTER_PLAN_ADVISOR_GUIDE_GENERATED_TOTAL.getMetric(operatorType).increase(1L);
        }
    }

    public static void increaseGuideApplied(TuningGuide guide) {
        String operatorType = resolveOperatorType(guide);
        if (operatorType != null) {
            MetricRepo.COUNTER_PLAN_ADVISOR_GUIDE_APPLIED_TOTAL.getMetric(operatorType).increase(1L);
        }
    }

    public static void increaseOptimizationDuration(long durationMs) {
        if (durationMs > 0) {
            MetricRepo.COUNTER_PLAN_ADVISOR_OPTIMIZATION_DURATION_MS_TOTAL.increase(durationMs);
        }
    }

    private static String resolveOperatorType(TuningGuide guide) {
        if (guide instanceof JoinTuningGuide) {
            return JOIN_OPERATOR_TYPE;
        }
        if (guide instanceof StreamingAggTuningGuide) {
            return AGG_OPERATOR_TYPE;
        }
        return null;
    }
}
