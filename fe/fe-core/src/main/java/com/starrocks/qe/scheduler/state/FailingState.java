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

import com.google.common.base.Preconditions;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.scheduler.ExecutionFragmentInstance;
import com.starrocks.qe.scheduler.state.event.FailureJobEvent;
import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.statemachine.StateContext;
import com.starrocks.qe.scheduler.state.statemachine.TransitionAction;
import com.starrocks.thrift.TStatusCode;

public class FailingState implements JobState {

    @Override
    public JobStateType getType() {
        return JobStateType.FAILING;
    }

    public static final class GotoFailing implements TransitionAction {
        @Override
        public JobState transition(StateContext ctx, JobState fromState, JobEvent event) {
            Preconditions.checkState(event instanceof FailureJobEvent,
                    "The transition GotoFailing cannot handle event {}", event);
            FailureJobEvent failureEvent = (FailureJobEvent) event;

            ctx.cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR, event.getStatus(), failureEvent.getFailure());

            ExecutionFragmentInstance execution = failureEvent.getExecution();
            if (execution != null && event.getStatus().getErrorCode() == TStatusCode.THRIFT_RPC_ERROR) {
                SimpleScheduler.addToBlacklist(execution.getBackend().getId());
            }

            return new FailingState();
        }
    }
}
