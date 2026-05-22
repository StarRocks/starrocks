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

#include "exec/pipeline/scan/morsel_queue.h"

namespace starrocks::pipeline {

class MorselQueueBuilder {
public:
    virtual ~MorselQueueBuilder() = default;

    virtual size_t num_original_morsels() const = 0;
    virtual size_t max_degree_of_parallelism() const = 0;
    virtual bool can_uniform_distribute() const = 0;

    virtual bool has_more_scan_ranges() const = 0;
    virtual void set_has_more_scan_ranges(bool value) = 0;
    virtual bool has_more_from_split() const = 0;
    virtual void set_has_more_from_split(bool value) = 0;

    virtual StatusOr<MorselQueuePtr> build() = 0;
    virtual Morsels take_morsels() = 0;
    virtual StatusOr<MorselQueuePtr> build_from_morsels(Morsels&& morsels) const = 0;
};

using MorselQueueBuilderPtr = std::unique_ptr<MorselQueueBuilder>;

} // namespace starrocks::pipeline
