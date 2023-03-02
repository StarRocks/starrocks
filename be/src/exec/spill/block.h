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
#include <vector>

#include "common/status.h"
#include "util/slice.h"

namespace starrocks {
namespace spill {

typedef uint64_t BlockId;

// represent a continous space in disk
// the smallest read/write unit for spilling result
// Block is immutable
// @TODO split to WritableBlock and ReadablBlock??
class Block {
public:
    virtual ~Block() = default;

    // append data into block
    virtual Status append(const std::vector<Slice>& data) = 0;

    virtual Status flush() = 0;

    // virtual Status read_all(std::string* output) = 0;
    virtual Status read_fully(void* data, int64_t count) = 0;

    virtual std::string debug_string() = 0;
};

using BlockPtr = std::shared_ptr<Block>;
}
}