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
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "compute_env/workgroup/work_group.h"
#include "compute_env/workgroup/work_group_manager.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/connector_chunk_sink.h"
#include "connector/hive_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/query_context_manager.h"
#include "formats/io/async_flush_output_stream.h"
#include "formats/utils.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {
namespace {

using CommitResult = formats::FileWriter::CommitResult;
using Stream = formats::AsyncFlushOutputStream;

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
        if (!_finish_status.ok()) {
            return _finish_status;
        }
        _finished = true;
        return Status::OK();
    }

    bool is_finished() override { return _finished; }

    void set_finished(bool finished) { _finished = finished; }
    void set_finish_status(Status status) { _finish_status = std::move(status); }

private:
    bool _finished = true;
    Status _finish_status = Status::OK();
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
        _runtime_state->set_fragment_ctx(_fragment_context, &_fragment_context->fragment_runtime_state());
        _runtime_state->set_fragment_dict_state(_fragment_context->dict_state());
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

std::shared_ptr<connector::HiveChunkSinkContext> make_hive_sink_context(FragmentContext* fragment_context) {
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
    sink_ctx->options = {};
    sink_ctx->max_file_size = 1 << 30;
    sink_ctx->fragment_context = fragment_context;
    return sink_ctx;
}

// Build a real factory so direct-construction tests can pass `&factory` instead of `nullptr`
// to satisfy the DCHECK(_runtime_access != nullptr) added by the Operator refactor.
std::unique_ptr<ConnectorSinkOperatorFactory> make_factory(FragmentContext* fragment_context) {
    auto provider = std::make_unique<connector::HiveChunkSinkProvider>();
    return std::make_unique<ConnectorSinkOperatorFactory>(/*id=*/0, std::move(provider),
                                                          make_hive_sink_context(fragment_context), fragment_context);
}

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
    std::atomic<int32_t> num_sinkers{0};
    auto factory = make_factory(_fragment_context);
    auto op = std::make_shared<ConnectorSinkOperator>(factory.get(), 0, Operator::s_pseudo_plan_node_id_for_final_sink,
                                                      0, std::move(chunk_sink),
                                                      std::make_unique<connector::AsyncFlushStreamPoller>(),
                                                      sink_mem_mgr, op_mem_mgr, _fragment_context, num_sinkers);

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
    // num_sinkers stays at 0, so set_finishing decrements to -1, skips the
    // last-sinker audit report path, and only exercises the memory-tracker behavior under test.
    std::atomic<int32_t> num_sinkers{0};
    auto factory = make_factory(_fragment_context);
    auto op = std::make_shared<ConnectorSinkOperator>(factory.get(), 0, Operator::s_pseudo_plan_node_id_for_final_sink,
                                                      0, std::move(chunk_sink), std::move(io_poller), sink_mem_mgr,
                                                      op_mem_mgr, _fragment_context, num_sinkers);
    ASSERT_OK(op->set_finishing(_runtime_state));

    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(process_tracker);
        EXPECT_TRUE(op->is_finished());
    }

    EXPECT_EQ(_query_pool_tracker->consumption(), 0);
    EXPECT_EQ(_query_tracker->consumption(), 0);
}

// Regression for missing audit statistics on Iceberg / Hive / table-function file sinks
// (see PR #62263). When the sink fragment has degree-of-parallelism > 1, only the last
// sinker to finish must invoke report_audit_statistics. Earlier sinkers must not.
TEST_F(ConnectorSinkOperatorTest, set_finishing_does_not_report_on_non_last_sinker) {
    auto* exec_env = ExecEnv::GetInstance();
    auto query_id = generate_uuid();
    auto fragment_id = generate_uuid();

    pipeline::QueryContext* query_ctx = nullptr;
    ASSIGN_OR_ASSERT_FAIL(query_ctx, exec_env->query_context_mgr()->get_or_register(query_id));
    query_ctx->set_query_id(query_id);
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(60);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->set_final_sink();
    query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_context->set_query_id(query_id);
    _fragment_context->set_fragment_instance_id(fragment_id);
    _fragment_context->set_workgroup(exec_env->workgroup_manager()->get_default_workgroup());
    _runtime_state->set_query_ctx(query_ctx);

    auto provider = std::make_unique<connector::HiveChunkSinkProvider>();
    ConnectorSinkOperatorFactory factory(0, std::move(provider), make_hive_sink_context(_fragment_context),
                                         _fragment_context);

    auto op0 = factory.create(/*degree_of_parallelism=*/2, /*driver_sequence=*/0);
    auto op1 = factory.create(/*degree_of_parallelism=*/2, /*driver_sequence=*/1);

    ASSERT_OK(op0->set_finishing(_runtime_state));

    // mark_audit_statistics_reported() is a one-shot false->true CAS. If the operator's
    // set_finishing had fired the report path, the marker would already be true and our
    // claim here would return false.
    EXPECT_TRUE(query_ctx->mark_audit_statistics_reported())
            << "report_audit_statistics fired prematurely on a non-last sinker";

    // Drain the remaining sinker so the factory's _num_sinkers settles back to 0.
    ASSERT_OK(op1->set_finishing(_runtime_state));
}

// When the sink fragment is the final sinker (DOP=1, or the last of N), set_finishing
// must invoke report_audit_statistics so the FE can populate scanRows / scanBytes /
// cpuCostNs / memCostBytes in audit_db.audit_log. Without this, every Iceberg/Hive
// INSERT shows up in the audit log with all-zero statistics.
TEST_F(ConnectorSinkOperatorTest, set_finishing_reports_on_last_sinker) {
    auto* exec_env = ExecEnv::GetInstance();
    auto query_id = generate_uuid();
    auto fragment_id = generate_uuid();

    pipeline::QueryContext* query_ctx = nullptr;
    ASSIGN_OR_ASSERT_FAIL(query_ctx, exec_env->query_context_mgr()->get_or_register(query_id));
    query_ctx->set_query_id(query_id);
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(60);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->set_final_sink();
    query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_context->set_query_id(query_id);
    _fragment_context->set_fragment_instance_id(fragment_id);
    _fragment_context->set_workgroup(exec_env->workgroup_manager()->get_default_workgroup());
    _runtime_state->set_query_ctx(query_ctx);

    auto provider = std::make_unique<connector::HiveChunkSinkProvider>();
    ConnectorSinkOperatorFactory factory(0, std::move(provider), make_hive_sink_context(_fragment_context),
                                         _fragment_context);

    auto op = factory.create(/*degree_of_parallelism=*/1, /*driver_sequence=*/0);

    ASSERT_OK(op->set_finishing(_runtime_state));

    // mark_audit_statistics_reported() should now return false: the report path already
    // CAS'd the flag to true synchronously at the top of
    // GlobalDriverExecutor::report_audit_statistics.
    EXPECT_FALSE(query_ctx->mark_audit_statistics_reported())
            << "report_audit_statistics did not fire when the last sinker finished";
}

// When the connector sink's finish() fails (for example an Iceberg / Hive commit error or
// an S3 write failure), set_finishing must propagate the error WITHOUT first claiming the
// one-shot audit-reported marker. Otherwise FragmentContext::cancel ->
// report_audit_statistics_on_failure would early-return on the CAS and the failure-path
// audit snapshot would be lost.
TEST_F(ConnectorSinkOperatorTest, set_finishing_does_not_claim_audit_marker_on_finish_failure) {
    auto* exec_env = ExecEnv::GetInstance();
    auto query_id = generate_uuid();
    auto fragment_id = generate_uuid();

    pipeline::QueryContext* query_ctx = nullptr;
    ASSIGN_OR_ASSERT_FAIL(query_ctx, exec_env->query_context_mgr()->get_or_register(query_id));
    query_ctx->set_query_id(query_id);
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(60);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->set_final_sink();
    query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_context->set_query_id(query_id);
    _fragment_context->set_fragment_instance_id(fragment_id);
    _fragment_context->set_workgroup(exec_env->workgroup_manager()->get_default_workgroup());
    _runtime_state->set_query_ctx(query_ctx);

    init_mem_trackers();

    // Single sinker so num_sinkers starts at 1 and this op IS the "last" sinker.
    std::atomic<int32_t> num_sinkers{1};

    auto chunk_sink = std::make_unique<TestConnectorChunkSink>(_runtime_state);
    chunk_sink->set_finish_status(Status::InternalError("simulated iceberg commit failure"));

    auto sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(_query_pool_tracker.get(), _query_tracker.get());
    auto* op_mem_mgr = sink_mem_mgr->create_child_manager();
    std::vector<connector::PartitionChunkWriterPtr> writers;
    connector::AsyncFlushStreamPoller empty_poller;
    ASSERT_OK(op_mem_mgr->init(&writers, &empty_poller, [](const CommitResult&) {}));

    auto factory = make_factory(_fragment_context);
    auto op = std::make_shared<ConnectorSinkOperator>(factory.get(), 0, Operator::s_pseudo_plan_node_id_for_final_sink,
                                                      0, std::move(chunk_sink),
                                                      std::make_unique<connector::AsyncFlushStreamPoller>(),
                                                      sink_mem_mgr, op_mem_mgr, _fragment_context, num_sinkers);

    Status finishing_status = op->set_finishing(_runtime_state);
    EXPECT_FALSE(finishing_status.ok()) << "set_finishing should propagate finish() failure";

    // The audit-reported marker must still be claimable. If set_finishing had reported before
    // finish() failed, the CAS here would observe true and return false, blocking the
    // cancellation path's report_audit_statistics_on_failure from sending the real failure snapshot.
    EXPECT_TRUE(query_ctx->mark_audit_statistics_reported())
            << "report_audit_statistics fired before finish() completed; cancellation path will be muted";
}

} // namespace
} // namespace starrocks::pipeline
