// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
