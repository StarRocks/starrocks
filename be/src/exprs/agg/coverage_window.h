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

#include "exprs/agg/window.h"

namespace starrocks {
struct CoverageWindowState {
    struct Impl;
    CoverageWindowState();
    ~CoverageWindowState();
    std::unique_ptr<Impl> impl;
};

// Window-only: no aggregate update/merge/serialization semantics. Analytor
// admits whole partitions and preallocates output chunks before evaluation.
class CoverageWindowFunction final : public WindowFunction<CoverageWindowState> {
public:
    Status validate_window_plan(FunctionContext*) const;
    Status admit_window_segment(FunctionContext*, AggDataPtr, const Column*, size_t, size_t, size_t, bool,
                                const Chunk*) const;
    MutableColumnPtr window_result_column(FunctionContext*, AggDataPtr) const;
    void update_batch_single_state_with_frame(FunctionContext*, AggDataPtr, const Column**, int64_t, int64_t, int64_t,
                                              int64_t) const override;
    void get_values(FunctionContext*, ConstAggDataPtr, Column*, size_t, size_t) const override;
    void reset(FunctionContext*, const Columns&, AggDataPtr) const override;
    std::string get_name() const override { return "st_coveragesimplify"; }
    size_t kernel_calls(ConstAggDataPtr) const;
};
} // namespace starrocks
