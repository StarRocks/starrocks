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

#include <atomic>

#include "exec/hash_joiner.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks {

struct MORParams {
    TupleDescriptor* tuple_desc = nullptr;
    std::vector<SlotDescriptor*> equality_slots;
    RuntimeProfile* runtime_profile = nullptr;
    int mor_tuple_id;
};

class DefaultMORProcessor {
public:
    DefaultMORProcessor() = default;
    virtual ~DefaultMORProcessor() = default;

    virtual Status init(RuntimeState* runtime_state, const MORParams& params) { return Status::OK(); }
    virtual Status get_next(RuntimeState* state, ChunkPtr* chunk) { return Status::OK(); }

    virtual void close(RuntimeState* runtime_state) {}

    virtual Status build_hash_table(RuntimeState* runtime_state) { return Status::OK(); }
    virtual Status append_chunk_to_hashtable(RuntimeState* runtime_state, ChunkPtr& chunk) { return Status::OK(); }
};

class IcebergMORProcessor final : public DefaultMORProcessor {
public:
    IcebergMORProcessor(RuntimeProfile* runtime_profile) : _runtime_profile(runtime_profile) {}
    ~IcebergMORProcessor() override = default;

    Status init(RuntimeState* runtime_state, const MORParams& params) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    void close(RuntimeState* runtime_state) override;
    Status build_hash_table(RuntimeState* runtime_state) override;
    Status append_chunk_to_hashtable(RuntimeState* runtime_state, ChunkPtr& chunk) override;

protected:
    std::vector<ExprContext*> _join_exprs;

private:
    HashJoiner* _hash_joiner = nullptr;
    std::unique_ptr<RowDescriptor> _build_row_desc;
    std::unique_ptr<RowDescriptor> _probe_row_desc;
    ObjectPool _pool;
    std::atomic<bool> _prepared_probe = false;
    RuntimeProfile* _runtime_profile = nullptr;
};

} // namespace starrocks