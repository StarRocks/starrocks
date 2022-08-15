// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/data_stream_sender.h"

namespace starrocks {

class MultiCastDataStreamSink : public DataSink {
public:
    MultiCastDataStreamSink(RuntimeState* state);
    void add_data_stream_sink(std::unique_ptr<DataStreamSender> data_stream_sink);
    ~MultiCastDataStreamSink() override = default;

    Status init(const TDataSink& thrift_sink) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile* profile() override { return nullptr; }
    std::vector<std::unique_ptr<DataStreamSender> >& get_sinks() { return _sinks; }
    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) override;

private:
    RuntimeState* _state;
    std::vector<std::unique_ptr<DataStreamSender> > _sinks;
};

} // namespace starrocks
