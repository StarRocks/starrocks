// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/mcast_data_stream_sink.h"
namespace starrocks {

static Status kOnlyPipelinedEngine = Status::NotSupported("Don't support non-pipelined query engine");

MCastDataStreamSink::MCastDataStreamSink(ObjectPool* pool) : _pool(pool), _profile(nullptr), _sinks() {}

void MCastDataStreamSink::add_data_stream_sink(std::unique_ptr<DataStreamSender> data_stream_sink) {
    _sinks.emplace_back(std::move(data_stream_sink));
}

Status MCastDataStreamSink::init(const TDataSink& thrift_sink) {
    for (auto& s : _sinks) {
        RETURN_IF_ERROR(s->init(thrift_sink));
    }
    return Status::OK();
}

Status MCastDataStreamSink::prepare(RuntimeState* state) {
    return kOnlyPipelinedEngine;
}

void MCastDataStreamSink::_create_profile() {
    // todo(yan):
    std::string title("MCastDataStreamSink");
    _profile = _pool->add(new RuntimeProfile(title));
}

Status MCastDataStreamSink::open(RuntimeState* state) {
    return kOnlyPipelinedEngine;
}

Status MCastDataStreamSink::close(RuntimeState* state, Status exec_status) {
    return kOnlyPipelinedEngine;
}

Status MCastDataStreamSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    return kOnlyPipelinedEngine;
}
} // namespace starrocks
