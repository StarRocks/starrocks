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

package com.starrocks.sql.plan;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.math.BigInteger;

/** Checks a complete physical plan before it can be scheduled for execution. */
final class AIQueryAdmission {
    private AIQueryAdmission() {
    }

    static void check(ExecPlan plan, long limit) {
        AIInputTokenEstimate estimate = plan.getAIInputTokenEstimate();
        if (limit == 0 || estimate.getStatus() == AIInputTokenEstimate.Status.NONE) {
            return;
        }
        ConnectContext context = plan.getConnectContext();
        StatementBase.ExplainLevel level = context == null ? null : context.getExplainLevel();
        // EXPLAIN ANALYZE executes the statement; all other EXPLAIN levels only build its plan.
        if (level != null && level != StatementBase.ExplainLevel.ANALYZE) {
            return;
        }
        boolean unknown = estimate.getStatus() == AIInputTokenEstimate.Status.UNKNOWN;
        if (unknown || estimate.getTokens().compareTo(BigInteger.valueOf(limit)) > 0) {
            String reason = unknown ? "Cannot estimate AI input tokens" : "Exceeded the limit of estimated AI input tokens";
            throw new StarRocksPlannerException(reason + ". Tokens allowed: " + limit + ", estimated tokens: " + estimate
                    + ". Please adjust the SQL or ask the administrator to change FE config "
                    + "ai_query_admission_max_estimated_input_tokens.", ErrorType.USER_ERROR);
        }
    }
}
