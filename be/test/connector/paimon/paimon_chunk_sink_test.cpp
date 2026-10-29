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

#include "connector/hive/paimon/paimon_chunk_sink.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <paimon/bucket/bucket_id_calculator.h>
#include <paimon/commit_message.h>
#include <paimon/memory/memory_pool.h>

#include <chrono>
#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/thread/priority_thread_pool.hpp"
#include "connector/hive/paimon/paimon_cpp_loader.h"
#include "connector_primitive/sink_memory_manager.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

TExpr slot_expr(int slot, const TypeDescriptor& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(type.to_thrift());
    node.__set_num_children(0);
    node.__set_output_scale(0);
    node.__set_is_nullable(true);
    TSlotRef ref;
    ref.__set_slot_id(slot);
    ref.__set_tuple_id(0);
    node.__set_slot_ref(ref);
    TExpr expr;
    expr.__set_nodes({node});
    return expr;
}

class PaimonChunkSinkTest : public ::testing::Test {
protected:
    void SetUp() override {
        root = std::filesystem::absolute("./ut_dir/paimon_chunk_sink_" + generate_uuid_string()).string();
        fs = FileSystem::Default();
        ASSERT_OK(fs->create_dir_recursive(root + "/schema"));
        state.init_instance_mem_tracker();
        executor = std::make_unique<PriorityThreadPool>("paimon_sink_test", 1, 8);
    }
    void TearDown() override {
        executor.reset();
        ASSERT_OK(fs->delete_dir_recursive(root));
    }

    std::shared_ptr<connector::PaimonChunkSinkContext> context(bool fixed = false, bool partitioned = false) {
        auto ctx = std::make_shared<connector::PaimonChunkSinkContext>();
        auto& sink = ctx->sink;
        sink.__set_writer_protocol_version(1);
        sink.__set_location(root);
        sink.__set_table_identity("db.table");
        sink.__set_schema_id(0);
        sink.__set_column_names(partitioned ? std::vector<std::string>{"id", "p"} : std::vector<std::string>{"id"});
        sink.__set_partition_columns(partitioned ? std::vector<std::string>{"p"} : std::vector<std::string>{});
        sink.__set_bucket_columns({}); // Public append-table default: hash an empty key row.
        sink.__set_primary_key_columns({});
        sink.__set_bucket_count(fixed ? 8 : -1);
        sink.__set_options({{"bucket", fixed ? "8" : "-1"}, {"file.format", "parquet"}});
        sink.__set_commit_user("starrocks-unit-test");
        sink.__set_commit_identifier(std::numeric_limits<int64_t>::max());
        sink.__set_operation_id("op-1");
        sink.__set_overwrite_mode("APPEND");
        sink.__set_time_zone("UTC");
        sink.__set_cloud_configuration(TCloudConfiguration{});
        if (fixed) {
            sink.__set_routing_bucket_slot_id(99);
            sink.__set_bucket_routed(true);
        }
        const std::string fields =
                partitioned ? R"({"id":0,"name":"id","type":"INT"},{"id":1,"name":"p","type":"STRING"})"
                            : R"({"id":0,"name":"id","type":"INT"})";
        const std::string schema = "{\"version\":3,\"id\":0,\"fields\":[" + fields + "],\"highestFieldId\":" +
                                   (partitioned ? "1" : "0") + ",\"partitionKeys\":" +
                                   (partitioned ? "[\"p\"]" : "[]") +
                                   ",\"primaryKeys\":[],\"options\":{\"bucket\":\"" + (fixed ? "8" : "-1") +
                                   "\",\"file.format\":\"parquet\"},\"timeMillis\":0}";
        sink.__set_schema_json(schema);
        auto file = fs->new_writable_file(root + "/schema/schema-0");
        EXPECT_TRUE(file.ok());
        if (file.ok()) {
            EXPECT_OK(file.value()->append(schema));
            EXPECT_OK(file.value()->close());
        }
        ctx->output_exprs = {slot_expr(0, TypeDescriptor(TYPE_INT))};
        if (partitioned) ctx->output_exprs.push_back(slot_expr(1, TypeDescriptor(TYPE_VARCHAR)));
        ctx->runtime_state = &state;
        ctx->executor = executor.get();
        return ctx;
    }

    ChunkPtr chunk(const std::vector<int32_t>& values) {
        auto chunk = std::make_shared<Chunk>();
        auto ids = Int32Column::create();
        for (auto value : values) ids->append(value);
        chunk->append_column(std::move(ids), 0);
        return chunk;
    }

    bool await(connector::PaimonChunkSink& sink) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!sink.is_finished() && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        return sink.is_finished();
    }

    std::string root;
    FileSystem* fs = nullptr;
    RuntimeState state;
    RuntimeProfile profile{"PaimonChunkSinkTest"};
    connector::SinkMemoryManager manager{nullptr, nullptr};
    std::unique_ptr<PriorityThreadPool> executor;
};

TEST_F(PaimonChunkSinkTest, EmptyDriverReportsVersionedTerminalProducerAndNoSnapshot) {
    auto ctx = context();
    connector::PaimonChunkSink sink(ctx, 3);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    ASSERT_OK(sink.finish());
    ASSERT_TRUE(await(sink));
    ASSERT_OK(sink.status());
    ASSERT_EQ(1, state.sink_commit_infos().size());
    const auto& info = state.sink_commit_infos()[0].paimon_commit_info;
    EXPECT_TRUE(info.prepare_complete);
    EXPECT_EQ(print_id(state.fragment_instance_id()) + ":3", info.producer_id);
    EXPECT_EQ(0, info.attempt_id);
    EXPECT_EQ(12, info.serializer_version);
    EXPECT_EQ(0, info.row_count);
    EXPECT_EQ(std::string(4, '\0'), info.commit_messages); // SerializeList: big-endian count, no version prefix.
    EXPECT_TRUE(info.owned_files.empty());
    EXPECT_EQ(0, state.num_rows_load_sink());
    EXPECT_FALSE(std::filesystem::exists(root + "/snapshot"));
    ASSERT_OK(sink.finish()); // one-shot and idempotent at connector lifecycle boundary
    EXPECT_EQ(1, state.sink_commit_infos().size());
}

TEST_F(PaimonChunkSinkTest, FullRowsGroupedByActualPartitionAndPreparedFilesSurviveLateCancellation) {
    auto ctx = context(false, true);
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    auto rows = chunk({1, 2, 3, 4});
    auto strings = BinaryColumn::create();
    for (const auto& value : {"a/b=c%", "", "a/b=c%", "中文"}) strings->append(Slice(value));
    auto nulls = NullColumn::create();
    for (uint8_t value : {0, 1, 0, 0}) nulls->append(value);
    rows->append_column(NullableColumn::create(std::move(strings), std::move(nulls)), 1);
    ASSERT_OK(sink.add(rows));
    ASSERT_OK(sink.finish());
    ASSERT_TRUE(await(sink));
    ASSERT_OK(sink.status());
    ASSERT_EQ(1, state.sink_commit_infos().size());
    const auto& info = state.sink_commit_infos()[0].paimon_commit_info;
    EXPECT_EQ(4, info.row_count);
    EXPECT_EQ(4, state.num_rows_load_sink());
    ASSERT_FALSE(info.owned_files.empty());
    auto pool = paimon::GetDefaultPool();
    auto messages = paimon::CommitMessage::DeserializeList(info.serializer_version, info.commit_messages.data(),
                                                          info.commit_messages.size(), pool);
    ASSERT_TRUE(messages.ok());
    EXPECT_EQ(3, messages.value().size());
    EXPECT_TRUE(std::filesystem::exists(root + "/p=a%2Fb%3Dc%25"));
    EXPECT_TRUE(std::filesystem::exists(root + "/p=__DEFAULT_PARTITION__"));
    EXPECT_FALSE(std::filesystem::exists(root + "/snapshot"));
    state.set_is_cancelled(true);
    sink.rollback(); // FE may already have committed. Never locally delete after handoff.
    for (const auto& path : info.owned_files) EXPECT_TRUE(std::filesystem::exists(path));
}

TEST_F(PaimonChunkSinkTest, NativeFixedBucketRoutingMatchesPublicCalculatorAndChecksRouteColumn) {
    auto ctx = context(true);
    auto rows = chunk({1, 2, 100});
    ObjectPool pool;
    auto* expr = pool.add(new ExprContext(pool.add(new ColumnRef(TypeDescriptor(TYPE_INT), 0))));
    ASSERT_OK(ExprExecutor::prepare(expr, &state));
    ASSERT_OK(ExprExecutor::open(expr, &state));
    auto routed = calculate_paimon_bucket_ids_in_shim(ctx->sink, rows, {expr});
    ASSERT_TRUE(routed.ok()) << routed.status();
    auto native_pool = paimon::GetDefaultPool();
    auto calc = paimon::BucketIdCalculator::Create(false, 8, native_pool);
    ASSERT_TRUE(calc.ok());
    auto empty = arrow::RecordBatch::Make(arrow::schema({}), 3, std::vector<std::shared_ptr<arrow::Array>>{});
    ArrowArray array{};
    ArrowSchema schema{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*empty, &array, &schema).ok());
    std::vector<int32_t> expected(3);
    ASSERT_TRUE(calc.value()->CalculateBucketIds(&array, &schema, expected.data()).ok());
    EXPECT_EQ(expected, routed.value());
    auto route = Int32Column::create();
    for (auto id : expected) route->append(id);
    rows->append_column(std::move(route), 99);
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    ASSERT_OK(sink.add(rows));
    ASSERT_OK(sink.finish());
    ASSERT_TRUE(await(sink));
    ASSERT_OK(sink.status());
    EXPECT_EQ(3, state.sink_commit_infos()[0].paimon_commit_info.row_count);
    ExprExecutor::close(expr, &state);
}

TEST_F(PaimonChunkSinkTest, FailedRouteAndWriterBoundsFailBeforeNativeRowWrite) {
    auto ctx = context(true);
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    auto rows = chunk({1});
    auto bad = Int32Column::create();
    bad->append(-1);
    rows->append_column(std::move(bad), 99);
    EXPECT_FALSE(sink.add(rows).ok());
    EXPECT_FALSE(sink.finish().ok());
    EXPECT_TRUE(sink.is_finished()); // An add failure must not hang cancellation/pending_finish.
    sink.rollback();
    EXPECT_TRUE(state.sink_commit_infos().empty());
}

TEST_F(PaimonChunkSinkTest, RejectsSchemaChangeBeforeWritingAndNeverRemovesSchema) {
    auto ctx = context();
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    auto changed = ctx->sink.schema_json;
    changed.replace(changed.find("\"id\":0"), 6, "\"id\":1");
    ASSIGN_OR_ABORT(auto file, fs->new_writable_file(root + "/schema/schema-1"));
    ASSERT_OK(file->append(changed));
    ASSERT_OK(file->close());
    EXPECT_FALSE(sink.add(chunk({1})).ok());
    sink.rollback();
    EXPECT_TRUE(std::filesystem::exists(root + "/schema/schema-0"));
    EXPECT_TRUE(std::filesystem::exists(root + "/schema/schema-1"));
    EXPECT_TRUE(state.sink_commit_infos().empty());
}

TEST_F(PaimonChunkSinkTest, RejectsNonNullEmptyPartitionInsteadOfWritingNullPartition) {
    auto ctx = context(false, true);
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    auto rows = chunk({1});
    auto strings = BinaryColumn::create();
    strings->append(Slice(""));
    rows->append_column(std::move(strings), 1);
    EXPECT_TRUE(sink.add(rows).is_not_supported());
    sink.rollback();
    EXPECT_TRUE(state.sink_commit_infos().empty());
    EXPECT_FALSE(std::filesystem::exists(root + "/p=__DEFAULT_PARTITION__"));
}

TEST_F(PaimonChunkSinkTest, BoundsActivePartitionWritersWithoutMidStatementPrepareCommit) {
    auto ctx = context(false, true);
    ctx->sink.options["starrocks.paimon.max-active-writers"] = "1";
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    auto rows = chunk({1, 2});
    auto strings = BinaryColumn::create();
    strings->append(Slice("a"));
    strings->append(Slice("b"));
    rows->append_column(std::move(strings), 1);
    EXPECT_TRUE(sink.add(rows).is_mem_limit_exceeded());
    sink.rollback();
    EXPECT_TRUE(state.sink_commit_infos().empty());
}

TEST_F(PaimonChunkSinkTest, CancellationBeforePrepareRollsBackOnlyUnpublishedAttemptFiles) {
    auto ctx = context();
    connector::PaimonChunkSink sink(ctx, 0);
    ASSERT_OK(sink.init(nullptr, &profile, &manager));
    ASSERT_OK(sink.add(chunk({1, 2})));
    state.set_is_cancelled(true);
    ASSERT_OK(sink.finish());
    ASSERT_TRUE(await(sink));
    EXPECT_TRUE(sink.status().is_cancelled());
    sink.rollback();
    EXPECT_TRUE(state.sink_commit_infos().empty());
    EXPECT_TRUE(std::filesystem::exists(root + "/schema/schema-0"));
    EXPECT_FALSE(std::filesystem::exists(root + "/snapshot"));
}

TEST_F(PaimonChunkSinkTest, RejectsUnsupportedDataLayoutBeforePreparingAnyFiles) {
    for (const auto& [key, value] : std::vector<std::pair<std::string, std::string>>{
                 {"data-file.path-directory", "data"}, {"data-file.external-paths", root + "/external"}}) {
        auto ctx = context();
        auto schema = ctx->sink.schema_json;
        const auto options = schema.find("\"options\":{");
        ASSERT_NE(std::string::npos, options);
        schema.insert(options + std::string("\"options\":{").size(), "\"" + key + "\":\"" + value + "\",");
        ctx->sink.__set_schema_json(schema);
        ctx->sink.options[key] = value;
        ASSIGN_OR_ABORT(auto file, fs->new_writable_file(root + "/schema/schema-0"));
        ASSERT_OK(file->append(schema));
        ASSERT_OK(file->close());
        connector::PaimonChunkSink sink(ctx, 0);
        EXPECT_TRUE(sink.init(nullptr, &profile, &manager).is_not_supported());
        sink.rollback();
        EXPECT_TRUE(state.sink_commit_infos().empty());
        EXPECT_FALSE(std::filesystem::exists(root + "/bucket-0"));
        EXPECT_FALSE(std::filesystem::exists(root + "/snapshot"));
    }
}

TEST_F(PaimonChunkSinkTest, LoaderReturnsDeferredSinkRatherThanFailingProviderValueAccess) {
    auto deferred = create_paimon_cpp_chunk_sink(nullptr, 0);
    ASSERT_NE(nullptr, deferred);
    // No dlopen is attempted at construction; prepare/init is the recoverable Status boundary.
    EXPECT_FALSE(deferred->status().ok());
}

} // namespace
} // namespace starrocks
