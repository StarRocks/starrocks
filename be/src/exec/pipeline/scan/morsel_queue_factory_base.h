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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "common/statusor.h"
#include "exec/pipeline/scan/scan_morsel.h"

namespace starrocks::pipeline {

class MorselQueue;

/// MorselQueueFactory.
class MorselQueueFactory {
public:
    virtual ~MorselQueueFactory() = default;

    virtual MorselQueue* create(int driver_sequence) = 0;
    virtual size_t size() const = 0;
    virtual size_t num_original_morsels() const = 0;

    virtual bool is_shared() const = 0;
    virtual bool could_local_shuffle() const = 0;

    virtual Status append_morsels(int driver_seq, Morsels&& morsels);
    virtual StatusOr<int> next_driver_seq();
    virtual bool enable_random_append_split_morsel() const { return false; }
    virtual void set_has_more_scan_ranges(bool v) {}
    virtual Status mark_split_source_morsel_finished();
    virtual bool reach_limit() const { return false; }
};

using MorselQueueFactoryPtr = std::unique_ptr<MorselQueueFactory>;
using MorselQueueFactoryMap = std::unordered_map<int32_t, MorselQueueFactoryPtr>;

} // namespace starrocks::pipeline
