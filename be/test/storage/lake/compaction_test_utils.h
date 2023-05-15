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

#include <gtest/gtest.h>

#include "storage/compaction_utils.h"

namespace starrocks::lake {

struct CompactionParam {
    CompactionAlgorithm algorithm = HORIZONTAL_COMPACTION;
    uint32_t vertical_compaction_max_columns_per_group = 5;
};

static std::string to_string_param_name(const testing::TestParamInfo<CompactionParam>& info) {
    std::stringstream ss;
    ss << CompactionUtils::compaction_algorithm_to_string(info.param.algorithm) << "_"
       << info.param.vertical_compaction_max_columns_per_group;
    return ss.str();
}

} // namespace starrocks::lake
