// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>

#include "util/logging.h"

namespace starrocks {
class ExecEnv;

extern std::atomic<bool> k_starrocks_exit;

void wait_for_fragments_finish(ExecEnv* exec_env, size_t max_loop_cnt_cfg);
} // namespace starrocks

void start_cn();
void start_be();
