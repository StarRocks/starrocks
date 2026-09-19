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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.sql.plan.AIInputTokenEstimate;
import com.starrocks.sql.plan.ExecPlan;

import java.math.BigInteger;

/** Checks the estimate and policy snapshot belonging to the plan that will actually execute. */
public final class AIQueryAdmission {
    private AIQueryAdmission() {
    }

    public static void check(ExecPlan plan) throws StarRocksException {
        if (plan == null || plan.getAIInputTokenEstimate().getStatus() == AIInputTokenEstimate.Status.NONE) {
            return;
        }
        long limit = plan.getAIInputTokenLimit();
        if (limit == 0) {
            return;
        }
        if (limit < 0) {
            throw new StarRocksException("AI input token limit must be non-negative");
        }
        AIInputTokenEstimate estimate = plan.getAIInputTokenEstimate();
        if (estimate.getStatus() == AIInputTokenEstimate.Status.UNKNOWN) {
            throw new StarRocksException("AI input token admission rejected: estimate is unknown ("
                    + estimate.getReason() + "), limit=" + limit);
        }
        if (estimate.getTokens().compareTo(BigInteger.valueOf(limit)) > 0) {
            throw new StarRocksException("AI input token admission rejected: estimated input tokens "
                    + estimate.getTokens() + " exceeds limit " + limit);
        }
    }
}
