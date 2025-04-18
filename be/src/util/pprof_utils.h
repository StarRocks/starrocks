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

#include <stdint.h>

#include <iostream>
#include <string>

#include "common/status.h"

namespace starrocks {

class PprofUtils {
public:
    static Status get_perf_cmd(std::string* cmd);

    static Status generate_flamegraph(int32_t sample_seconds, const std::string& flame_graph_tool_dir, bool return_file,
                                      std::string* svg_file_or_content);
};

} // namespace starrocks

