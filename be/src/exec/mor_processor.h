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

struct HdfsScannerParams;
class MorProcessor {
public:
    MorProcessor() = default;
    virtual ~MorProcessor() = default;

    virtual Status init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) { return Status::OK(); }
    virtual Status get_next(RuntimeState* state, RuntimeProfile* profile, ChunkPtr* chunk) { return Status::OK(); }

    virtual void close(RuntimeState* runtime_state) { }

    virtual Status build_hash_table(RuntimeState* runtime_state) { return Status::OK(); }

    virtual std::shared_ptr<HashJoiner> hash_joiner() {
        return nullptr;
    }
};

class IcebergMorProcessor final: public MorProcessor {
public:
    IcebergMorProcessor() = default;
    ~IcebergMorProcessor() override = default;

    Status init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    Status get_next(RuntimeState* state, RuntimeProfile* prifile, ChunkPtr* chunk) override;
    void close(RuntimeState* runtime_state) override;
    Status build_hash_table(RuntimeState* runtime_state) override;

    std::shared_ptr<HashJoiner> hash_joiner() override {
        return _hash_joiner;
    }

protected:
    std::vector<ExprContext*> _join_exprs;

private:
    std::shared_ptr<HashJoiner> _hash_joiner = nullptr;
    std::unique_ptr<RowDescriptor> _build_row_desc;
    std::unique_ptr<RowDescriptor> _probe_row_desc;
    ObjectPool _pool;
    std::atomic<bool> _prepared_probe = false;
};

}