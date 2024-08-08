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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.sql.automv.util.MetaUtil;

public class PostTransferActions {
    public static final PostTransferAction CRADLE_POST_ACTION = PostTransferAction.DO_NOTHING;
    public static final PostTransferAction INTERN_POST_ACTION = PostTransferAction.DO_NOTHING;
    public static final PostTransferAction GRAVE_POST_ACTION = ((mvLifecycle, policy) -> {
        if (mvLifecycle.isAbsent()) {
            mvLifecycle.detach();
        }

        mvLifecycle.getMVPlus().ifPresent(mvPlus -> {
            mvPlus.getMv().setInactiveAndReason("Unsatisfactory performance MV: " + mvPlus.getFqName().toSql());
        });
    });
    public static final PostTransferAction EXTINCTION_POST_ACTION = ((mvLifecycle, policy) -> {
        mvLifecycle.getMVPlus().ifPresent(mvPlus -> {
            MetaUtil.dropMV(mvPlus.getFqName().toSql());
            mvLifecycle.detach();
        });
    });
    private static final PostTransferAction NEXT_TURN_TO_PERF_EVAL = ((mvLifecycle, policy) -> {
        if (policy.getReachPerformanceEvaluationTimeDictator().test(mvLifecycle)) {
            mvLifecycle.commit(mvLifecycle.getPhase());
        }
    });
    public static final PostTransferAction TENURED_POST_ACTION = NEXT_TURN_TO_PERF_EVAL;
    public static final PostTransferAction RETIRED_POST_ACTION = NEXT_TURN_TO_PERF_EVAL;
}
