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
struct CoverageWindowState {
    std::unique_ptr<GeoCoverageSimplify> core;
    GeoCoverageLimits limits;
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
    void create(FunctionContext*, AggDataPtr) const override;
    void update_batch_single_state_with_frame(FunctionContext*, AggDataPtr, const Column**, int64_t, int64_t, int64_t,
                                              int64_t) const override;
    void get_values(FunctionContext*, ConstAggDataPtr, Column*, size_t, size_t) const override;
    void reset(FunctionContext*, const Columns&, AggDataPtr) const override;
    std::string get_name() const override { return "st_coveragesimplify"; }
    size_t kernel_calls(ConstAggDataPtr) const;

private:
    Status initialize(FunctionContext*, CoverageWindowState&) const;
    Status prepare_partition(FunctionContext*, CoverageWindowState&, const Column*, int64_t, int64_t) const;
};
} // namespace starrocks
