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

#include <functional>
#include <memory>
#include <string>

#include "base/string/slice.h"
#include "common/statusor.h"

namespace starrocks {

inline constexpr size_t kGeoTopologySimplifyMaxCoordinates = 5'000;
inline constexpr size_t kGeoTopologySimplifyMaxComponents = 1'024;
inline constexpr size_t kGeoTopologySimplifyMaxBytes = 256 * 1024;
inline constexpr size_t kGeoTopologySimplifyMaxWork = 20'000'000;

// Owns the original WKB/tree and immutable exact coordinates/contact anchors.
// Evaluation owns its mutable retained indices and R-tree. It emits only source
// vertices, preserves the tree/families/holes/contacts, and bounds two-sided path
// and polygon-boundary Hausdorff distance by tolerance. Different SQL rows do not
// share a boundary constraint. Standard allocations use the BE memory hooks;
// the caller's checkpoint supplies cancellation/query-status/memory checks.
class PreparedGeoTopologySimplify {
public:
    ~PreparedGeoTopologySimplify();
    static StatusOr<std::unique_ptr<PreparedGeoTopologySimplify>> prepare(
            Slice wkb, const std::function<Status()>& checkpoint = {}, size_t work_limit = kGeoTopologySimplifyMaxWork);
    // Zero returns the original bytes after preparation validated every component.
    StatusOr<std::string> simplify(double tolerance, const std::function<Status()>& checkpoint = {}) const;

private:
    struct Impl;
    explicit PreparedGeoTopologySimplify(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

} // namespace starrocks
