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

#include <memory>
#include <optional>

#include "base/string/slice.h"
#include "common/statusor.h"

namespace starrocks {

// One complete partition. Admission precedes payload copies/decode; no row is
// available until finish succeeds. Holds original and output native WKB with
// positional mapping, including NULL/EMPTY and MULTIPOLYGON members. The caller
// owns descriptor compatibility. Standard allocations use the BE memory hooks;
// the aggregation framework handles cancellation and query-state checks.
class GeoCoverageSimplify {
public:
    static StatusOr<std::unique_ptr<GeoCoverageSimplify>> create();
    ~GeoCoverageSimplify();

    Status append(std::optional<Slice> wkb);
    Status finish(std::optional<double> tolerance, std::optional<bool> simplify_boundary = true);
    StatusOr<std::optional<Slice>> result(size_t position) const;
    // Contiguous immutable output, valid only after a successful finish. Used
    // by the native window adapter without a second WKB payload copy. Keep this
    // partition alive while borrowing it.
    StatusOr<Slice> result_buffer() const;
    size_t rows() const;
    size_t kernel_calls() const;

private:
    struct Impl;
    explicit GeoCoverageSimplify(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

} // namespace starrocks
