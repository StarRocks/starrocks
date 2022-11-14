// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/multi_cast_data_stream_sink.h"

namespace starrocks {

static Status kOnlyPipelinedEngine = Status::NotSupported("Don't support non-pipelined query engine");

MultiCastDataStreamSink::MultiCastDataStreamSink(RuntimeState* state) : _sinks() {}

void MultiCastDataStreamSink::add_data_stream_sink(std::unique_ptr<DataStreamSender> data_stream_sink) {
    _sinks.emplace_back(std::move(data_stream_sink));
}

Status MultiCastDataStreamSink::init(const TDataSink& thrift_sink) {
    for (auto& s : _sinks) {
        RETURN_IF_ERROR(s->init(thrift_sink));
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

Status MultiCastDataStreamSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    return kOnlyPipelinedEngine;
}
} // namespace starrocks
