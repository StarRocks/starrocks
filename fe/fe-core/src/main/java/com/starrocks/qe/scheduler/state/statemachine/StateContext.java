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

package com.starrocks.qe.scheduler.state.statemachine;

import com.starrocks.common.Status;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.scheduler.state.ExecutingState;
import com.starrocks.qe.scheduler.state.WaitingForResourceState;

public interface StateContext extends
        WaitingForResourceState.Context,
        ExecutingState.Context {
    void releaseSharingSlot();

    void cancelInternal(PPlanFragmentCancelReason cancelReason, Status status, Throwable failure);
}
