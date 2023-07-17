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

package com.starrocks.qe.scheduler.state;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.statemachine.StateContext;
import com.starrocks.qe.scheduler.state.statemachine.TransitionAction;
import com.starrocks.sql.PlannerProfile;

public class ExecutingState implements JobState {
    private final Context ctx;

    public ExecutingState(Context ctx) {
        this.ctx = ctx;
    }

    @Override
    public JobStateType getType() {
        return JobStateType.EXECUTING;
    }

    @Override
    public void onEnter() {
        try {
            try (PlannerProfile.ScopedTimer ignore1 = PlannerProfile.getScopedTimer("Scheduler.Prepare")) {
                ctx.initializeExecutionDAG();
                ctx.initializeResultSink();
                ctx.initializeProfile();
            }

            try (PlannerProfile.ScopedTimer ignore2 = PlannerProfile.getScopedTimer("Scheduler.Deploy")) {
                ctx.deploy();
            }
        } catch (UserException e) {
            ctx.onFailure(Status.createInternalError(e.getMessage()), e);
        }
    }

    public interface Context {
        void initializeExecutionDAG() throws UserException;

        void initializeResultSink() throws AnalysisException;

        void initializeProfile();

        void deploy();

        void onFailure(Status status, Throwable failure);
    }

    public static final class GotoExecuting implements TransitionAction {
        @Override
        public JobState transition(StateContext ctx, JobState fromState, JobEvent event) {
            return new ExecutingState(ctx);
        }
    }

}
