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

#include "common/statusor.h"
#include "geo/wkb.h"

namespace starrocks {

inline constexpr size_t kGeoBufferPointsPerCircle = 32;
inline constexpr size_t kGeoBufferMaxInputCoordinates = 5'000;
inline constexpr size_t kGeoBufferMaxOutputCoordinates = 10'000;

// Immutable, owned Cartesian models; Boost scratch is local to each buffer call.
// Limits bound input/output sizes, not the time or peak memory of one Boost call.
class PreparedGeoBuffer {
public:
    ~PreparedGeoBuffer();
    static StatusOr<std::unique_ptr<PreparedGeoBuffer>> prepare(Slice wkb,
                                                                const std::function<Status()>& checkpoint = {});
    StatusOr<WkbGeometry> buffer(double distance, const std::function<Status()>& checkpoint = {}) const;

private:
    struct Impl;
    explicit PreparedGeoBuffer(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

} // namespace starrocks
