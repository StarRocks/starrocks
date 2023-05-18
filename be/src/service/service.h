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

#include "util/logging.h"

namespace starrocks {
class ExecEnv;

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_exit_quick;

void wait_for_fragments_finish(ExecEnv* exec_env, size_t max_loop_cnt_cfg);
} // namespace starrocks

void start_cn();
void start_be();
