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
#include <atomic>

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {

/**
 * `StreamPipelineDriver` is used in Incremental MV which is different from `PipelineDriver`:
 *  - `StreamPipelineDriver` has EPOCH_FINISHING/EPOCH_FINISHED state which represents 
 *      an Epoch's processing lifecycle.
 *  - When a `StreamPipelineDriver` enters `EPOCH_FINISHED` state, it will be put a `parked`
 *      queue which will do nothing in the queue, and in the next epoch it will call `reset_epoch`
 *      to reset driver's and operators' state at first.
 */
class StreamPipelineDriver final : public PipelineDriver {
public:
    StreamPipelineDriver(const Operators& operators, QueryContext* query_ctx, FragmentContext* fragment_ctx,
                         Pipeline* pipeline, int32_t driver_id)
            : PipelineDriver(operators, query_ctx, fragment_ctx, pipeline, driver_id) {}
    ~StreamPipelineDriver() override = default;

    StatusOr<DriverState> process(RuntimeState* runtime_state, int worker_id) override;

    bool is_query_never_expired() override { return true; }

    void epoch_finalize(RuntimeState* runtime_state, DriverState state);
    [[nodiscard]] Status reset_epoch(RuntimeState* state);

private:
    StatusOr<DriverState> _handle_finish_operators(RuntimeState* runtime_state);

    [[nodiscard]] Status _mark_operator_epoch_finishing(OperatorPtr& op, RuntimeState* state);
    [[nodiscard]] Status _mark_operator_epoch_finished(OperatorPtr& op, RuntimeState* state);

private:
    // index of the first epoch-unfisheded operator
    size_t _first_epoch_unfinished{0};
};

} // namespace starrocks::pipeline
