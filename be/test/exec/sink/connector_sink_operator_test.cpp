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

#include "connector/async_flush_stream_poller.h"
#include "connector/connector_chunk_sink.h"
#include "connector/hive_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group.h"
#include "formats/utils.h"
#include "io/async_flush_output_stream.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

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

// A partition writer whose flush() always fails. ConnectorChunkSink::finish() is not
// virtual, so an Iceberg / Hive commit failure is simulated by handing the sink a writer
// that fails on the first step of finish().
class FailingPartitionChunkWriter final : public connector::PartitionChunkWriter {
public:
    explicit FailingPartitionChunkWriter(Status status)
            : PartitionChunkWriter("", {}, std::make_shared<connector::PartitionChunkWriterContext>()),
              _status(std::move(status)) {}

    Status init() override { return Status::OK(); }
    Status write(const ChunkPtr& chunk) override { return Status::OK(); }
    Status flush() override { return _status; }
    Status wait_flush() override { return Status::OK(); }
    Status finish() override { return Status::OK(); }
    bool is_finished() override { return true; }
    int64_t get_written_bytes() override { return 0; }
    int64_t get_flushable_bytes() override { return 0; }

private:
    Status _status;
};

class TestConnectorChunkSink final : public connector::ConnectorChunkSink {
public:
    explicit TestConnectorChunkSink(RuntimeState* state)
            : ConnectorChunkSink({}, {}, std::make_unique<NoopPartitionChunkWriterFactory>(), state, false) {}

    void callback_on_commit(const CommitResult& result) override {}

    void set_finished(bool finished) { _finished = finished; }

    // Makes the inherited finish() return `status`.
    void set_finish_status(Status status) {
        _partition_chunk_writers[connector::PartitionKey{"", {}}] =
                std::make_shared<FailingPartitionChunkWriter>(std::move(status));
    }

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

    // Marks a fresh QueryContext as final sink and wires it into _fragment_context /
    // _runtime_state the way FragmentExecutor::_prepare_pipeline_driver does, so the
    // last-sinker report path can reach QueryContext::final_query_statistic().
    void prepare_query_ctx() {
        _query_ctx = std::make_shared<QueryContext>();
        _query_ctx->set_final_sink();
        auto* query_pool_tracker = GlobalEnv::GetInstance()->query_pool_mem_tracker();
        _query_ctx->init_mem_tracker(query_pool_tracker->limit(), query_pool_tracker);

        _fragment_context->set_workgroup(ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup());
        _runtime_state->set_query_ctx(_query_ctx.get());
    }

    // Requires init_mem_trackers(). Builds the sink-level memory manager plus one registered
    // per-operator manager over the fixture's _writers / _poller (both may stay empty).
    connector::SinkOperatorMemoryManager* init_op_mem_mgr() {
        _sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(_query_pool_tracker.get(), _query_tracker.get());
        auto* op_mem_mgr = _sink_mem_mgr->create_child_manager();
        op_mem_mgr->init(&_writers, &_poller, [](const CommitResult&) {});
        return op_mem_mgr;
    }

    // Wraps a test sink in a ConnectorSinkOperator that shares the fixture's _num_sinkers counter
    // and the _sink_mem_mgr built by init_op_mem_mgr().
    std::shared_ptr<ConnectorSinkOperator> make_operator(std::unique_ptr<connector::ConnectorChunkSink> chunk_sink,
                                                         connector::SinkOperatorMemoryManager* op_mem_mgr) {
        return std::make_shared<ConnectorSinkOperator>(nullptr, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0,
                                                       std::move(chunk_sink),
                                                       std::make_unique<connector::AsyncFlushStreamPoller>(),
                                                       _sink_mem_mgr, op_mem_mgr, _fragment_context, _num_sinkers);
    }

    std::shared_ptr<MemTracker> _query_pool_tracker;
    std::shared_ptr<MemTracker> _query_tracker;
    ObjectPool _pool;
    FragmentContext* _fragment_context;
    RuntimeState* _runtime_state;
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<connector::SinkMemoryManager> _sink_mem_mgr;
    std::map<connector::PartitionKey, connector::PartitionChunkWriterPtr> _writers;
    connector::AsyncFlushStreamPoller _poller;
    std::atomic<int32_t> _num_sinkers{0};
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
    std::map<connector::PartitionKey, connector::PartitionChunkWriterPtr> writers;
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());
        CurrentThread::mem_consume_without_cache(kTrackedBytes);
        writers.emplace(connector::PartitionKey{"", {}},
                        std::make_shared<TestPartitionChunkWriter>(_runtime_state, kTrackedBytes));
    }
    ASSERT_EQ(_query_pool_tracker->consumption(), kTrackedBytes);
    ASSERT_EQ(_query_tracker->consumption(), kTrackedBytes);

    auto sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(_query_pool_tracker.get(), _query_tracker.get());
    auto* op_mem_mgr = sink_mem_mgr->create_child_manager();
    op_mem_mgr->init(&writers, &poller, [](const CommitResult&) {});

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    auto op = std::make_shared<ConnectorSinkOperator>(nullptr, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0,
                                                      std::move(chunk_sink),
                                                      std::make_unique<connector::AsyncFlushStreamPoller>(),
                                                      sink_mem_mgr, op_mem_mgr, _fragment_context, _num_sinkers);

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
    std::map<connector::PartitionKey, connector::PartitionChunkWriterPtr> writers;
    connector::AsyncFlushStreamPoller empty_poller;
    op_mem_mgr->init(&writers, &empty_poller, [](const CommitResult&) {});

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    chunk_sink->set_finished(true);
    // _num_sinkers stays at 0, so set_finishing decrements to -1, skips the last-sinker
    // audit report path, and only exercises the memory-tracker behavior under test.
    auto op = std::make_shared<ConnectorSinkOperator>(nullptr, 0, Operator::s_pseudo_plan_node_id_for_final_sink, 0,
                                                      std::move(chunk_sink), std::move(io_poller), sink_mem_mgr,
                                                      op_mem_mgr, _fragment_context, _num_sinkers);
    ASSERT_OK(op->set_finishing(_runtime_state));

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        EXPECT_TRUE(op->is_finished());
    }

    EXPECT_EQ(_query_pool_tracker->consumption(), 0);
    EXPECT_EQ(_query_tracker->consumption(), 0);
}

// Regression for missing audit statistics on Iceberg / Hive / table-function file sinks
// (issue #62262). When the sink fragment has degree-of-parallelism > 1, only the last
// sinker to finish may invoke report_audit_statistics; earlier sinkers must just hand the
// counter on.
TEST_F(ConnectorSinkOperatorTest, set_finishing_does_not_report_on_non_last_sinker) {
    prepare_query_ctx();
    init_mem_trackers();

    // Two sinkers were created, so this one is not the last to finish.
    _num_sinkers = 2;

    auto* op_mem_mgr = init_op_mem_mgr();
    auto op = make_operator(std::make_unique<TestConnectorChunkSink>(_runtime_state), op_mem_mgr);

    ASSERT_OK(op->set_finishing(_runtime_state));
    EXPECT_EQ(_num_sinkers.load(), 1) << "a non-last sinker must only decrement the counter";
}

// When the sink fragment is the final sinker (DOP=1, or the last of N), set_finishing
// must invoke report_audit_statistics so the FE can populate scanRows / scanBytes /
// cpuCostNs / memCostBytes in audit_db.audit_log. Without this, every Iceberg/Hive
// INSERT shows up in the audit log with all-zero statistics. The report path runs through
// QueryContext::final_query_statistic(), which DCHECKs is_final_sink().
TEST_F(ConnectorSinkOperatorTest, set_finishing_reports_on_last_sinker) {
    prepare_query_ctx();
    init_mem_trackers();

    // A single sinker, so this operator IS the last one when set_finishing decrements.
    _num_sinkers = 1;

    auto* op_mem_mgr = init_op_mem_mgr();
    auto op = make_operator(std::make_unique<TestConnectorChunkSink>(_runtime_state), op_mem_mgr);

    ASSERT_OK(op->set_finishing(_runtime_state));
    EXPECT_EQ(_num_sinkers.load(), 0);
}

// When the connector sink's finish() fails (for example an Iceberg / Hive commit error or
// an S3 write failure), set_finishing must propagate the error and skip the audit report,
// while still decrementing the counter so the parallelism bookkeeping stays correct.
TEST_F(ConnectorSinkOperatorTest, set_finishing_propagates_finish_failure_without_reporting) {
    prepare_query_ctx();
    init_mem_trackers();

    // Single sinker so this op IS the "last" sinker when set_finishing decrements.
    _num_sinkers = 1;

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    chunk_sink->set_finish_status(Status::InternalError("simulated iceberg commit failure"));

    auto* op_mem_mgr = init_op_mem_mgr();
    auto op = make_operator(std::move(chunk_sink), op_mem_mgr);

    Status finishing_status = op->set_finishing(_runtime_state);
    EXPECT_FALSE(finishing_status.ok()) << "set_finishing should propagate finish() failure";
    EXPECT_EQ(_num_sinkers.load(), 0) << "the counter must be decremented even when finish() fails";
}

} // namespace
} // namespace starrocks::pipeline
