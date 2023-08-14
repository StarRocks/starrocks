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

#include "common/logging.h"
#include "exec/data_sink.h"

namespace starrocks {

class ExprContext;

class IcebergTableSink : public DataSink {
public:
    IcebergTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs);

    ~IcebergTableSink() override;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

    std::vector<TExpr> get_output_expr() const { return _t_output_expr; }

private:
    ObjectPool* _pool;
    const std::vector<TExpr>& _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    RuntimeProfile* _profile = nullptr;
};

} // namespace starrocks