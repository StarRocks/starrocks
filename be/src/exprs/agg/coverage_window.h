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
#include "geo/geo_coverage_simplify.h"

namespace starrocks {
namespace coverage_window_detail {
// BinaryColumn limits each value to 32 bits, but its adaptive offsets allow
// the aggregate payload to exceed that range.
StatusOr<size_t> output_reserve_size(size_t current_bytes, size_t value_bytes);
} // namespace coverage_window_detail

struct CoverageWindowState {
    std::unique_ptr<GeoCoverageSimplify> core;
    std::optional<double> tolerance;
    std::optional<bool> boundary;
    size_t emitted = 0;
    size_t calls = 0;
    bool initialized = false;
};

// Uses the normal full-partition window lifecycle. The state owns the kernel;
// output columns own their WKB and do not retain the partition state.
class CoverageWindowFunction final : public WindowFunction<CoverageWindowState> {
public:
    void create(FunctionContext* ctx, AggDataPtr state) const override;
    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr state, const Column** columns,
                                              int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                              int64_t frame_end) const override;
    void get_values(FunctionContext* ctx, ConstAggDataPtr state, Column* dst, size_t start, size_t end) const override;
    void reset(FunctionContext* ctx, const Columns& columns, AggDataPtr state) const override;
    std::string get_name() const override { return "st_coveragesimplify"; }
    size_t kernel_calls(ConstAggDataPtr state) const;

private:
    Status initialize(FunctionContext* ctx, CoverageWindowState& state) const;
    Status prepare_partition(FunctionContext* ctx, CoverageWindowState& state, const Column* input, int64_t start,
                             int64_t end) const;
};
} // namespace starrocks
