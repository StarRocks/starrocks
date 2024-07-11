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

#include "runtime/multi_cast_data_stream_sink.h"

namespace starrocks {

MultiCastDataStreamSink::MultiCastDataStreamSink(RuntimeState* state) : _sinks() {}

void MultiCastDataStreamSink::add_data_stream_sink(std::unique_ptr<DataStreamSender> data_stream_sink) {
    _sinks.emplace_back(std::move(data_stream_sink));
}

Status MultiCastDataStreamSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    for (auto& s : _sinks) {
        RETURN_IF_ERROR(s->init(thrift_sink, state));
    }
    return Status::OK();
}

Status MultiCastDataStreamSink::prepare(RuntimeState* state) {
    return Status::NotSupported("Don't support non-pipelined query engine");
}

Status MultiCastDataStreamSink::open(RuntimeState* state) {
    return Status::NotSupported("Don't support non-pipelined query engine");
}

Status MultiCastDataStreamSink::close(RuntimeState* state, Status exec_status) {
    return Status::NotSupported("Don't support non-pipelined query engine");
}

Status MultiCastDataStreamSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::NotSupported("Don't support non-pipelined query engine");
}

Status SplitDataStreamSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    const TSplitDataStreamSink& split_sink = thrift_sink.split_stream_sink;
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, split_sink.splitExprs, &_split_expr_ctxs, state));
    TDataSink fakeDataSink{};
    for (size_t i = 0; i < _sinks.size(); i++) {
        auto& s = _sinks[i];
        // super ugly
        fakeDataSink.stream_sink = split_sink.sinks[i];
        RETURN_IF_ERROR(s->init(fakeDataSink, state));
    }
    return Status::OK();
}
} // namespace starrocks
