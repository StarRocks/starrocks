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

#include "runtime/data_stream_sender.h"

namespace starrocks {

class MultiCastDataStreamSink : public DataSink {
public:
    MultiCastDataStreamSink(RuntimeState* state);
    void add_data_stream_sink(std::unique_ptr<DataStreamSender> data_stream_sink);
    ~MultiCastDataStreamSink() override = default;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    RuntimeProfile* profile() override { return nullptr; }
    std::vector<std::unique_ptr<DataStreamSender>>& get_sinks() { return _sinks; }
    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

protected:
    std::vector<std::unique_ptr<DataStreamSender>> _sinks;
};

class SplitDataStreamSink : public MultiCastDataStreamSink {
public:
    SplitDataStreamSink(RuntimeState* state) : MultiCastDataStreamSink(state), _pool(state->obj_pool()){};
    ~SplitDataStreamSink() override = default;

    // create split exprs and init data stream senders
    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;
    std::vector<ExprContext*>& get_split_expr_ctxs() { return _split_expr_ctxs; }

private:
    ObjectPool* _pool;
    // init here and move to SplitLocalExchanger when split pipeline
    std::vector<ExprContext*> _split_expr_ctxs;
};

} // namespace starrocks
