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

package com.starrocks.qe.feedback.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.feedback.OperatorTuningGuides;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.OptExpression;

import java.util.List;
import java.util.Map;

public class PlanTuningAnalyzer {

    private static final PlanTuningAnalyzer INSTANCE = new PlanTuningAnalyzer();

    private final List<Analyzer> analyzerList;

    private PlanTuningAnalyzer() {
        analyzerList = ImmutableList.of(StreamingAggTuningAnalyzer.getInstance(), JoinTuningAnalyzer.getInstance());
    }

    public static PlanTuningAnalyzer getInstance() {
        return INSTANCE;
    }

    public void analyzePlan(OptExpression root, Map<Integer, SkeletonNode> skeletonNodeMap, OperatorTuningGuides tuningGuides) {
        for (Analyzer analyzer : analyzerList) {
            analyzer.analyze(root, skeletonNodeMap, tuningGuides);
        }
    }

    public interface Analyzer {
        void analyze(OptExpression root, Map<Integer, SkeletonNode> skeletonNodeMap, OperatorTuningGuides tuningGuides);
    }
}
