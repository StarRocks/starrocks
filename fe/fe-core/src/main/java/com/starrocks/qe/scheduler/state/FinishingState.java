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
import com.starrocks.qe.scheduler.state.event.FinishingJobEvent;
import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.statemachine.StateContext;
import com.starrocks.qe.scheduler.state.statemachine.TransitionAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FinishingState implements JobState {
    private static final Logger LOG = LogManager.getLogger(FinishingState.class);

    @Override
    public JobStateType getType() {
        return JobStateType.FINISHING;
    }

    public static final class GotoFinishing implements TransitionAction {
        @Override
        public JobState transition(StateContext ctx, JobState fromState, JobEvent event) {
            Preconditions.checkState(event instanceof FinishingJobEvent,
                    "The transition GotoFailing cannot handle event {}", event);
            FinishingJobEvent finishingEvent = (FinishingJobEvent) event;

            if (finishingEvent.isReachLimit()) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                ctx.cancelInternal(PPlanFragmentCancelReason.LIMIT_REACH, event.getStatus(), null);
            }

            return new FinishingState();
        }
    }
}
