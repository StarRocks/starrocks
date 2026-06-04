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

#include "compute_env/spill/restore_task.h"

#include <glog/logging.h>

#include "common/runtime_profile.h"
#include "compute_env/spill/yield.h"

namespace starrocks::spill {

Status YieldableRestoreTask::do_read(workgroup::YieldContext& yield_ctx, SerdeContext& context) {
    size_t num_eos = 0;
    yield_ctx.total_yield_point_cnt = _sub_stream.size();
    auto wg = yield_ctx.wg;
    while (yield_ctx.yield_point < yield_ctx.total_yield_point_cnt) {
        {
            SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
            size_t i = yield_ctx.yield_point;
            if (!_sub_stream[i]->eof()) {
                DCHECK(_sub_stream[i]->enable_prefetch());
                auto status = _sub_stream[i]->prefetch(yield_ctx, context);
                if (!status.ok() && !status.is_end_of_file() && !status.is_yield()) {
                    return status;
                }
                if (status.is_yield()) {
                    yield_ctx.need_yield = true;
                    return Status::OK();
                }
            }
            yield_ctx.yield_point++;
            num_eos += _sub_stream[i]->eof();
        }

        BREAK_IF_YIELD(wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
    }

    if (num_eos == _sub_stream.size()) {
        _input_stream->mark_is_eof();
        return Status::EndOfFile("eos");
    }
    return Status::OK();
}

} // namespace starrocks::spill
