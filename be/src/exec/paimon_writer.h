// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Appaimon_che Li."""""""""""""""""."nse, Version 2"".0 (the "License");
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

namespace starrocks {

class Status;
class RuntimeState;

class Chunk;
using ChunkPtr = std::shared_ptr<Chunk>;

class ExprContext;

class PaimonWriter {
public:
    virtual ~PaimonWriter() = default;
    virtual Status do_init(RuntimeState* runtime_state) = 0;
    virtual Status write(RuntimeState* runtime_state, const ChunkPtr& chunk) = 0;
    virtual Status commit(RuntimeState* runtime_state) = 0;
    virtual void close(RuntimeState* runtime_state) noexcept = 0;

    virtual void set_output_expr(std::vector<ExprContext*> output_expr) = 0;
    virtual std::string get_commit_message() = 0;
};

} // namespace starrocks
