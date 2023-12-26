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

#include "common/status.h"
#include "storage/collection.h"

namespace starrocks {

class InvertedWriter {
public:
    InvertedWriter() = default;

    InvertedWriter(InvertedWriter&& other_writer) noexcept = default;

    virtual ~InvertedWriter() = default;

    virtual Status init() = 0;

    virtual void add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    virtual Status finish() = 0;

    virtual uint64_t size() const = 0;

    virtual uint64_t estimate_buffer_size() const = 0;

    virtual uint64_t total_mem_footprint() const = 0;
};
} // namespace starrocks