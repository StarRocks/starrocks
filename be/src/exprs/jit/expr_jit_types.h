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

#include <cstdint>

namespace starrocks {

struct JitScore {
    int64_t score = 0;
    int64_t num = 0;
};

enum CompilableExprType : int32_t {
    ARITHMETIC = 2, // except /, %
    CAST = 4,
    CASE = 8,
    CMP = 16,
    LOGICAL = 32,
    DIV = 64,
    MOD = 128,
};

inline constexpr double kExprJitScoreRatio = 0.88;

} // namespace starrocks
