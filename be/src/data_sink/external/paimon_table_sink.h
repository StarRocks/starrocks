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

#include "exec_primitive/data_sink.h"

namespace starrocks {

// Pipeline composition and the SDK live above/outside this lifecycle-only DataSink.
class PaimonTableSink final : public DataSink {
public:
    PaimonTableSink(ObjectPool* pool, const std::vector<TExpr>& output_exprs);
    ~PaimonTableSink() override;

    Status init(const TDataSink& sink, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState*) override { return Status::OK(); }
    Status send_chunk(RuntimeState*, Chunk*) override;
    Status close(RuntimeState*, const Status&) override { return Status::OK(); }
    RuntimeProfile* profile() override { return _profile; }
    const std::vector<TExpr>& get_output_expr() const { return _output_exprs; }

private:
    ObjectPool* _pool;
    std::vector<TExpr> _output_exprs;
    RuntimeProfile* _profile = nullptr;
};

} // namespace starrocks
