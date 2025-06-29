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

#pragma once

#include "exec/pipeline/schedule/observer.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "runtime/descriptors.h"

namespace starrocks::pipeline {
class FragmentContext;

// TimerTask object, fragment->cancel is called if the timeout is reached.
class CheckFragmentTimeout final : public PipelineTimerTask {
public:
    CheckFragmentTimeout(FragmentContext* fragment_ctx) : _fragment_ctx(fragment_ctx) {}
    void Run() override;

private:
    FragmentContext* _fragment_ctx;
};

// If the timeout is reached, a cancel_update event is sent to all objects observing _timeout.
class RFScanWaitTimeout final : public PipelineTimerTask {
public:
    RFScanWaitTimeout(FragmentContext* fragment_ctx, bool all_rf_timeout = false)
            : _fragment_ctx(fragment_ctx), _all_rf_timeout(all_rf_timeout) {}
    void add_observer(RuntimeState* state, PipelineObserver* observer) { _timeout.add_observer(state, observer); }
    void Run() override;

private:
    FragmentContext* _fragment_ctx;
    bool _all_rf_timeout = false;
    Observable _timeout;
};

} // namespace starrocks::pipeline