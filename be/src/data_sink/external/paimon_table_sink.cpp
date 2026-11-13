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

#include "data_sink/external/paimon_table_sink.h"

#include "base/uid_util.h"
#include "common/runtime_profile.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/runtime_state.h"

namespace starrocks {

PaimonTableSink::PaimonTableSink(ObjectPool* pool, const std::vector<TExpr>& output_exprs)
        : _pool(pool), _output_exprs(output_exprs) {}
PaimonTableSink::~PaimonTableSink() = default;

Status PaimonTableSink::init(const TDataSink& sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(sink, state));
    if (!sink.__isset.paimon_table_sink) return Status::InvalidArgument("Missing Paimon table sink");
    if (!state->enable_pipeline_engine()) return Status::NotSupported("Paimon writes require the pipeline engine");
    return prepare(state);
}

Status PaimonTableSink::prepare(RuntimeState* state) {
    if (_profile != nullptr) return Status::OK();
    RETURN_IF_ERROR(DataSink::prepare(state));
    _profile =
            _pool->add(new RuntimeProfile("PaimonTableSink (frag_id=" + print_id(state->fragment_instance_id()) + ")"));
    return Status::OK();
}

Status PaimonTableSink::send_chunk(RuntimeState*, Chunk*) {
    return Status::NotSupported("Paimon writes must execute through the connector sink pipeline");
}

} // namespace starrocks
