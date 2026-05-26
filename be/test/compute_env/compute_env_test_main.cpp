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

#include <gtest/gtest.h>

#include "common/config_exec_flow_fwd.h"
#include "common/config_thrift_server_fwd.h"

int main(int argc, char** argv) {
    starrocks::config::max_memory_sink_batch_count = 20;
    starrocks::config::thrift_max_message_size = 1073741824;
    starrocks::config::thrift_max_frame_size = 16384000;
    starrocks::config::thrift_max_recursion_depth = 64;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
