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

#include "fs/fs.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// this class is the base class of file builder, which defines the basic API of building any format of file
class FileBuilder {
public:
    virtual ~FileBuilder() = default;

    // appends this chunk to the file
    virtual Status add_chunk(Chunk* chunk) = 0;

    // returns the size of underlying file or stream
    virtual std::size_t file_size() = 0;

    // close underlying file or stream properly, including flush and sync semantics
    virtual Status finish() = 0;
};

} // namespace starrocks
