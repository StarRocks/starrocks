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
#include <utility>
#include <vector>

#include "exec/pipeline/scan/morsel.h"

namespace starrocks::pipeline {
class DriverSource;
using DriverSourcePtr = std::unique_ptr<DriverSource>;
using DriverSources = std::vector<DriverSourcePtr>;
class DriverSource {
public:
    DriverSource(Morsels morsels, int32_t source_id) : _morsels(std::move(morsels)), _source_id(source_id) {}

    const Morsels& get_morsels() const { return _morsels; }

    int32_t get_source_id() const { return _source_id; }

private:
    Morsels _morsels;
    int32_t _source_id;
};
} // namespace starrocks::pipeline
