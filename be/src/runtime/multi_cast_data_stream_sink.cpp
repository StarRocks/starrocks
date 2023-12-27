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

static Status kOnlyPipelinedEngine = Status::NotSupported("Don't support non-pipelined query engine");

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
    return kOnlyPipelinedEngine;
}

Status MultiCastDataStreamSink::open(RuntimeState* state) {
    return kOnlyPipelinedEngine;
}

Status MultiCastDataStreamSink::close(RuntimeState* state, Status exec_status) {
    return kOnlyPipelinedEngine;
}

Status MultiCastDataStreamSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return kOnlyPipelinedEngine;
}
} // namespace starrocks
