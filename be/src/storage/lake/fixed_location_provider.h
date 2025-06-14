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

#include <utility>

#include "gutil/macros.h"
#include "storage/lake/location_provider.h"

namespace starrocks::lake {

class FixedLocationProvider : public LocationProvider {
public:
    explicit FixedLocationProvider(std::string root) : _root(std::move(std::move(root))) {
        while (!_root.empty() && _root.back() == '/') {
            _root.pop_back();
        }
    }

    ~FixedLocationProvider() override = default;

    // No usage now.
    DISALLOW_COPY_AND_MOVE(FixedLocationProvider);

    std::string root_location(int64_t tablet_id) const override { return _root; }

private:
    std::string _root;
};

} // namespace starrocks::lake
