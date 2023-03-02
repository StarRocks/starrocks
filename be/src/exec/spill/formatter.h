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

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/spill/block.h"
#include "util/raw_container.h"

namespace starrocks {
class SpilledOptions;
namespace spill {

enum class FormatterType {
    BY_COLUMN,
};

struct FormatterContext {
    std::string serialize_buffer;
    raw::RawString compress_buffer;
};

class Formatter {
public:
    virtual ~Formatter() = default;

    virtual Status serialize(FormatterContext& ctx, const ChunkPtr& chunk, BlockPtr block) = 0;
    virtual StatusOr<ChunkUniquePtr> deserialize(FormatterContext& ctx, const BlockPtr block) = 0;
};
using FormatterPtr = std::shared_ptr<Formatter>;

StatusOr<FormatterPtr> create_formatter(SpilledOptions* options);
} // namespace spill
} // namespace starrocks