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

#include "exec/pipeline/sink/connector_sink_operator.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/connector_chunk_sink.h"
#include "connector/hive_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "formats/utils.h"
#include "io/async_flush_output_stream.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using Stream = io::AsyncFlushOutputStream;

class NoopWritableFile : public WritableFile {
public:
    explicit NoopWritableFile(std::string filename) : _filename(std::move(filename)) {}

    Status append(const Slice& data) override {
        _size += data.size;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; ++i) {
            _size += data[i].size;
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    Status flush(FlushMode mode) override { return Status::OK(); }
    Status sync() override { return Status::OK(); }
    uint64_t size() const override { return _size; }
    const std::string& filename() const override { return _filename; }

private:
    std::string _filename;
    uint64_t _size = 0;
};

class ReleaseOnDestructWritableFile final : public NoopWritableFile {
public:
    ReleaseOnDestructWritableFile(std::string filename, int64_t bytes)
            : NoopWritableFile(std::move(filename)), _bytes(bytes) {}

    ~ReleaseOnDestructWritableFile() override { CurrentThread::mem_release_without_cache(_bytes); }

private:
    int64_t _bytes;
};

class NoopPartitionChunkWriterFactory final : public connector::PartitionChunkWriterFactory {
public:
    Status init() override { return Status::OK(); }

    connector::PartitionChunkWriterPtr create(std::string partition,
                                              std::vector<int8_t> partition_field_null_list) const override {
        return nullptr;
    }
};

class TestConnectorChunkSink final : public connector::ConnectorChunkSink {
public:
    explicit TestConnectorChunkSink(RuntimeState* state)
            : ConnectorChunkSink({}, {}, std::make_unique<NoopPartitionChunkWriterFactory>(), state, false) {}

    void callback_on_commit(const CommitResult& result) override {}

    Status finish() override {
        _finished = true;
        return Status::OK();
    }

    bool is_finished() override { return _finished; }

    void set_finished(bool finished) { _finished = finished; }

private:
    bool _finished = true;
};

class TestPartitionChunkWriter final : public connector::PartitionChunkWriter {
public:
    TestPartitionChunkWriter(RuntimeState* state, int64_t flushable_bytes)
            : PartitionChunkWriter("", {}, std::make_shared<connector::PartitionChunkWriterContext>()),
              _flushable_bytes(flushable_bytes) {
        _out_stream = std::make_shared<Stream>(std::make_unique<NoopWritableFile>("writer.out"), nullptr, state);
    }

    Status init() override { return Status::OK(); }
    Status write(const ChunkPtr& chunk) override { return Status::OK(); }

    Status flush() override {
        if (_flushable_bytes > 0) {
            CurrentThread::mem_release_without_cache(_flushable_bytes);
            _flushable_bytes = 0;
        }
        return Status::OK();
    }

    Status wait_flush() override { return Status::OK(); }
    Status finish() override { return Status::OK(); }
    bool is_finished() override { return true; }
    int64_t get_written_bytes() override { return _flushable_bytes; }
    int64_t get_flushable_bytes() override { return _flushable_bytes; }

private:
    int64_t _flushable_bytes;
};

class ConnectorSinkOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = _pool.add(new FragmentContext);
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>(TUniqueId(), TUniqueId(), TQueryOptions(),
                                                                            TQueryGlobals(), ExecEnv::GetInstance()));
        _runtime_state = _fragment_context->runtime_state();
        _runtime_state->set_fragment_ctx(_fragment_context);
    }

    void TearDown() override {}

    void init_mem_trackers() {
        auto* process_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
        _query_pool_tracker =
                std::make_shared<MemTracker>(MemTrackerType::QUERY_POOL, 100, "query_pool_ut", process_tracker);
        _query_tracker =
                std::make_shared<MemTracker>(MemTrackerType::QUERY, 100, "query_ut", _query_pool_tracker.get());
        _runtime_state->init_mem_trackers(_query_tracker);
    }

    std::shared_ptr<MemTracker> _query_pool_tracker;
    std::shared_ptr<MemTracker> _query_tracker;
    ObjectPool _pool;
    FragmentContext* _fragment_context;
    RuntimeState* _runtime_state;
};

TEST_F(ConnectorSinkOperatorTest, test_factory) {
    {
        auto provider = std::make_unique<connector::HiveChunkSinkProvider>();
        auto sink_ctx = std::make_shared<connector::HiveChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->data_column_names = {"k1"};
        sink_ctx->partition_column_names = {"k2"};
        sink_ctx->data_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        sink_ctx->partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->fragment_context = _fragment_context;
        auto op_factory =
                std::make_unique<ConnectorSinkOperatorFactory>(0, std::move(provider), sink_ctx, _fragment_context);
        auto op = op_factory->create(1, 0);
        EXPECT_OK(op->prepare(_runtime_state));
    }
}

TEST_F(ConnectorSinkOperatorTest, need_input_releases_flush_memory_under_instance_tracker) {
    auto* process_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    init_mem_trackers();

    constexpr int64_t kTrackedBytes = 100;
    connector::AsyncFlushStreamPoller poller;
    std::vector<connector::PartitionChunkWriterPtr> writers;
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
        CurrentThread::mem_consume_without_cache(kTrackedBytes);
        writers.emplace_back(std::make_shared<TestPartitionChunkWriter>(_runtime_state, kTrackedBytes));
    }
    ASSERT_EQ(_query_pool_tracker->consumption(), kTrackedBytes);
    ASSERT_EQ(_query_tracker->consumption(), kTrackedBytes);

    auto sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(_query_pool_tracker.get(), _query_tracker.get());
    auto* op_mem_mgr = sink_mem_mgr->create_child_manager();
    ASSERT_OK(op_mem_mgr->init(&writers, &poller, [](const CommitResult&) {}));

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    auto op = std::make_shared<ConnectorSinkOperator>(
            nullptr, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0, std::move(chunk_sink),
            std::make_unique<connector::AsyncFlushStreamPoller>(), sink_mem_mgr, op_mem_mgr, _fragment_context);

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        EXPECT_TRUE(op->need_input());
    }

    EXPECT_EQ(_query_pool_tracker->consumption(), 0);
    EXPECT_EQ(_query_tracker->consumption(), 0);
}

TEST_F(ConnectorSinkOperatorTest, is_finished_releases_polled_stream_under_instance_tracker) {
    auto* process_tracker = GlobalEnv::GetInstance()->process_mem_tracker();
    init_mem_trackers();

    constexpr int64_t kTrackedBytes = 64;
    auto stream = [&]() {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
        CurrentThread::mem_consume_without_cache(kTrackedBytes);
        return std::make_shared<Stream>(std::make_unique<ReleaseOnDestructWritableFile>("stream.out", kTrackedBytes),
                                        nullptr, _runtime_state);
    }();
    ASSERT_EQ(_query_pool_tracker->consumption(), kTrackedBytes);
    ASSERT_EQ(_query_tracker->consumption(), kTrackedBytes);
    ASSERT_OK(stream->close());

    auto io_poller = std::make_unique<connector::AsyncFlushStreamPoller>();
    io_poller->enqueue(stream);
    stream.reset();

    auto sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(_query_pool_tracker.get(), _query_tracker.get());
    auto* op_mem_mgr = sink_mem_mgr->create_child_manager();
    std::vector<connector::PartitionChunkWriterPtr> writers;
    connector::AsyncFlushStreamPoller empty_poller;
    ASSERT_OK(op_mem_mgr->init(&writers, &empty_poller, [](const CommitResult&) {}));

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    chunk_sink->set_finished(true);
    auto op = std::make_shared<ConnectorSinkOperator>(nullptr, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0,
                                                      std::move(chunk_sink), std::move(io_poller), sink_mem_mgr,
                                                      op_mem_mgr, _fragment_context);
    ASSERT_OK(op->set_finishing(_runtime_state));

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        EXPECT_TRUE(op->is_finished());
    }

    EXPECT_EQ(_query_pool_tracker->consumption(), 0);
    EXPECT_EQ(_query_tracker->consumption(), 0);
}

} // namespace
} // namespace starrocks::pipeline
