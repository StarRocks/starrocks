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

import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.statemachine.StateContext;
import com.starrocks.qe.scheduler.state.statemachine.TransitionAction;

public class WaitingForResourceState implements JobState {
    private final Context ctx;

    public WaitingForResourceState(Context ctx) {
        this.ctx = ctx;
    }

    @Override
    public JobStateType getType() {
        return JobStateType.WAITING_FOR_RESOURCE;
    }

    @Override
    public void onEnter() {
        ctx.requireSlot();
    }

    @Override
    public void onLeave() {
        ctx.cancelSlotRequirement();
    }

    public interface Context {
        void resetJobDAG();

        void resetAvailableComputeNodes();

        void requireSlot();

        void cancelSlotRequirement();
    }

    public static final class GotoWaitingForResource implements TransitionAction {
        @Override
        public JobState transition(StateContext ctx, JobState fromState, JobEvent event) {
            ctx.resetJobDAG();
            ctx.resetAvailableComputeNodes();

            return new WaitingForResourceState(ctx);
        }
    }
}
