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

#include "service/service.h"

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace starrocks {
void wait_for_fragments_finish(ExecEnv* exec_env, size_t max_loop_cnt_cfg) {
    if (max_loop_cnt_cfg == 0) {
        return;
    }

    size_t running_fragments = exec_env->fragment_mgr()->running_fragment_count();
    size_t loop_cnt = 0;

    while (running_fragments && loop_cnt < max_loop_cnt_cfg) {
        DLOG(INFO) << running_fragments << " fragment(s) are still running...";
        sleep(10);
        running_fragments = exec_env->fragment_mgr()->running_fragment_count();
        loop_cnt++;
    }
}
} // namespace starrocks
