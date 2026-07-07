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

#include "storage/segment_flush_executor.h"

#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <gtest/gtest.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <thread>
#include <utility>

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_exec_fwd.h"
#include "common/thread/threadpool.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/walltime.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "service/brpc_service_test_util.h"
#include "storage/async_delta_writer.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/segment_replicate_executor.h"
#include "storage/segment_request_ref.h"
#include "storage/storage_engine.h"
#include "storage/storage_metrics.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"

namespace starrocks {

class SegmentFlushExecutorTest : public ::testing::Test {
public:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        _partition_id = 1;
        _index_id = 1;
        _tablet = create_tablet(rand(), rand());
        _mem_tracker = std::make_unique<MemTracker>(-1);
    }

    void TearDown() override {
        if (_tablet) {
            auto st = StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            CHECK(st.ok()) << st.to_string();
            _tablet.reset();
        }

        if (!_primary_tablet_segment_dir.empty()) {
            fs::remove_all(_primary_tablet_segment_dir);
        }
    }

    Status prepare_primary_tablet_segment_dir(const std::string& path) {
        _primary_tablet_segment_dir = std::move(path);
        RETURN_IF_ERROR(fs::remove_all(_primary_tablet_segment_dir));
        return fs::create_directories(_primary_tablet_segment_dir);
    }

    void write_file(const std::string& path, const std::string& content) {
        std::ofstream output(path, std::ios::binary);
        ASSERT_TRUE(output.good());
        output.write(content.data(), content.size());
        ASSERT_TRUE(output.good());
    }

    DeltaWriterOptions create_replicate_options() {
        DeltaWriterOptions options{};
        options.tablet_id = _tablet->tablet_id();
        options.schema_hash = _tablet->schema_hash();
        options.txn_id = rand();
        options.partition_id = _partition_id;
        options.sink_id = 0;
        options.load_id.set_lo(rand());
        options.load_id.set_hi(rand());
        options.index_id = _index_id;
        options.node_id = 0;
        options.timeout_ms = 3600000;
        options.write_quorum = WriteQuorumTypePB::ONE;
        options.replica_state = ReplicaState::Primary;
        return options;
    }

    Status replicate_segment_once(std::unique_ptr<SegmentPB> segment) {
        std::unique_ptr<ThreadPool> pool;
        RETURN_IF_ERROR(ThreadPoolBuilder("seg_repl_once").set_min_threads(1).set_max_threads(1).build(&pool));
        auto options = create_replicate_options();
        ReplicateToken token(pool->new_token(ThreadPool::ExecutionMode::SERIAL), &options);
        RETURN_IF_ERROR(token.submit(std::move(segment), false));
        return token.wait();
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::DUP_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        TColumn c0;
        c0.column_name = "c0";
        c0.__set_is_key(true);
        c0.__set_is_allow_null(false);
        c0.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.emplace_back(c0);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TupleDescriptor* _create_tuple_desc() {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->tablet_schema()->num_columns(); i++) {
            auto& column = _tablet->tablet_schema()->column(i);
            TSlotDescriptorBuilder builder;
            std::string column_name{column.name()};
            TSlotDescriptor slot_desc = builder.type(column.type())
                                                .column_name(column_name)
                                                .column_pos(i)
                                                .nullable(column.is_nullable())
                                                .build();
            tuple_builder.add_slot(slot_desc);
        }
        TDescriptorTableBuilder table_builder;
        tuple_builder.build(&table_builder);
        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);
        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
        auto* tuple_desc = row_desc->tuple_descriptors()[0];

        return tuple_desc;
    }

    std::unique_ptr<AsyncDeltaWriter> create_delta_writer(int64_t tablet_id, int32_t schema_hash,
                                                          MemTracker* mem_tracker) {
        DeltaWriterOptions options;
        options.tablet_id = tablet_id;
        options.schema_hash = schema_hash;
        options.txn_id = rand();
        options.partition_id = _partition_id;
        options.load_id.set_lo(rand());
        options.load_id.set_hi(rand());
        options.index_id = _index_id;
        options.node_id = 0;
        options.timeout_ms = 3600000;
        options.write_quorum = WriteQuorumTypePB::MAJORITY;
        options.replica_state = ReplicaState::Secondary;
        TupleDescriptor* tuple_desc = _create_tuple_desc();
        options.slots = &tuple_desc->slots();

        auto status_or = AsyncDeltaWriter::open(options, mem_tracker);
        CHECK(status_or.ok()) << status_or.status().to_string();
        return std::move(status_or.value());
    }

    void create_single_seg_rowset(Tablet* tablet, int num_rows, std::string& path, RowsetSharedPtr& rowset,
                                  SegmentPB* segment_pb) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id;
        rowset_id.init(10000);
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = tablet->partition_id();
        writer_context.rowset_path_prefix = _tablet->schema_hash_path();
        writer_context.rowset_state = VISIBLE;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());
        std::vector<uint32_t> column_indexes{0};
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema(), column_indexes);
        auto chunk = ChunkFactory::new_chunk(schema, num_rows);
        for (auto i = 0; i < num_rows; ++i) {
            chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        }
        ASSERT_OK(rowset_writer->flush_chunk(*chunk, segment_pb));
        rowset = rowset_writer->build().value();
    }

    void attach_segment_data(SegmentPB& segment_pb, brpc::Controller* controller) {
        std::shared_ptr<FileSystem> fs;
        ASSIGN_OR_ABORT(fs, FileSystemFactory::CreateSharedFromString(segment_pb.path()));
        auto res = fs->new_random_access_file(segment_pb.path());
        ASSERT_TRUE(res.ok());
        auto rfile = std::move(res.value());
        auto buf = new uint8[segment_pb.data_size()];
        butil::IOBuf data;
        data.append_user_data(buf, segment_pb.data_size(), [](void* buf) { delete[](uint8*) buf; });
        auto st = rfile->read_fully(buf, segment_pb.data_size());
        ASSERT_OK(st);
        controller->request_attachment().append(data);
    }

    Status get_prepared_rowset(int64_t tablet_id, int64_t txn_id, int64_t partition_id, RowsetSharedPtr* rowset) {
        std::map<TabletInfo, std::pair<RowsetSharedPtr, bool>> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &tablet_infos);
        for (auto& [tablet_info, rs] : tablet_infos) {
            if (tablet_info.tablet_id == tablet_id) {
                (*rowset) = rs.first;
                return Status::OK();
            }
        }
        return Status::NotFound(fmt::format("Rowset not found. tablet_id: {}, txn_id: {}, partition_id: {}", tablet_id,
                                            txn_id, partition_id));
    }

    void check_single_segment_rowset_result(RowsetSharedPtr& rowset, int num_rows) {
        ASSERT_EQ(1, rowset->rowset_meta()->num_segments());
        SegmentReadOptions seg_options;
        ASSIGN_OR_ABORT(seg_options.fs, FileSystemFactory::CreateSharedFromString("posix://"));
        OlapReaderStatistics stats;
        seg_options.stats = &stats;
        std::string segment_file = Rowset::segment_file_path(_tablet->schema_hash_path(), rowset->rowset_id(), 0);
        auto segment = *Segment::open(seg_options.fs, FileInfo{segment_file}, 0, _tablet->tablet_schema());
        ASSERT_EQ(segment->num_rows(), num_rows);
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto res = segment->new_iterator(schema, seg_options);
        ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

        const auto& seg_iterator = res.value();
        ASSERT_TRUE(seg_iterator->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        auto chunk = ChunkFactory::new_chunk(seg_iterator->schema(), 100);
        int count = 0;
        while (true) {
            auto st = seg_iterator->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_FALSE(!st.ok());
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(count, chunk->get(i)[0].get_int32());
                count += 1;
            }
            chunk->reset();
        }
        ASSERT_EQ(num_rows, count);
    }

protected:
    int64_t _partition_id;
    int64_t _index_id;
    TabletSharedPtr _tablet;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::string _primary_tablet_segment_dir;
    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TEST_F(SegmentFlushExecutorTest, test_write_and_commit_segment) {
    ASSERT_OK(prepare_primary_tablet_segment_dir("./ut_dir/SegmentFlushExecutorTest_test_write_segment"));
    // the rowset on the primary tablet
    RowsetSharedPtr primary_rowset;
    std::unique_ptr<SegmentPB> segment_pb = std::make_unique<SegmentPB>();
    create_single_seg_rowset(_tablet.get(), 10, _primary_tablet_segment_dir, primary_rowset, segment_pb.get());

    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    DeltaWriter* delta_writer = async_delta_writer->writer();
    PTabletWriterAddSegmentRequest request;
    std::unique_ptr<starrocks::PUniqueId> id = std::make_unique<starrocks::PUniqueId>();
    id->set_lo(delta_writer->load_id().lo());
    id->set_hi(delta_writer->load_id().hi());
    request.set_allocated_id(id.release());
    request.set_txn_id(delta_writer->txn_id());
    request.set_index_id(delta_writer->index_id());
    request.set_tablet_id(delta_writer->tablet()->tablet_id());
    request.set_eos(true);

    brpc::Controller controller;
    attach_segment_data(*segment_pb.get(), &controller);
    request.set_allocated_segment(segment_pb.release());

    PTabletWriterAddSegmentResult response;
    MockClosure closure;
    AsyncDeltaWriterSegmentRequest async_request{.cntl = &controller,
                                                 .request = SegmentRequestRef::borrowed(&request),
                                                 .response = &response,
                                                 .done = &closure};
    async_delta_writer->write_segment(async_request);
    ASSERT_OK(delta_writer->segment_flush_token()->wait());
    ASSERT_TRUE(closure.has_run());
    RowsetSharedPtr prepared_rowset;
    ASSERT_OK(get_prepared_rowset(_tablet->tablet_id(), delta_writer->txn_id(), _partition_id, &prepared_rowset));
    check_single_segment_rowset_result(prepared_rowset, 10);
    ASSERT_OK(StorageEngine::instance()->txn_manager()->delete_txn(_partition_id, _tablet, delta_writer->txn_id()));

    // just verify the metrics have value, rather than verify it accurately
    // because other test cases may also update the metrics concurrently if
    // run tests in parallel, and it's hard to get the accurate value
    ASSERT_TRUE(StorageMetrics::instance()->segment_flush_total.value() > 0);
    ASSERT_TRUE(StorageMetrics::instance()->segment_flush_bytes_total.value() > 0);
}

TEST_F(SegmentFlushExecutorTest, test_owned_segment_request_ref_keeps_request_alive) {
    ASSERT_OK(prepare_primary_tablet_segment_dir("./ut_dir/SegmentFlushExecutorTest_test_owned_segment_request_ref"));
    RowsetSharedPtr primary_rowset;
    std::unique_ptr<SegmentPB> segment_pb = std::make_unique<SegmentPB>();
    create_single_seg_rowset(_tablet.get(), 10, _primary_tablet_segment_dir, primary_rowset, segment_pb.get());

    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    DeltaWriter* delta_writer = async_delta_writer->writer();

    brpc::Controller controller;
    attach_segment_data(*segment_pb.get(), &controller);

    PTabletWriterAddSegmentResult response;
    MockClosure closure;
    std::weak_ptr<PTabletWriterAddSegmentRequest> weak_request;
    {
        auto request = std::make_shared<PTabletWriterAddSegmentRequest>();
        weak_request = request;
        request->mutable_id()->set_lo(delta_writer->load_id().lo());
        request->mutable_id()->set_hi(delta_writer->load_id().hi());
        request->set_txn_id(delta_writer->txn_id());
        request->set_index_id(delta_writer->index_id());
        request->set_tablet_id(delta_writer->tablet()->tablet_id());
        request->set_eos(true);
        request->set_allocated_segment(segment_pb.release());

        AsyncDeltaWriterSegmentRequest async_request{.cntl = &controller,
                                                     .request = SegmentRequestRef::owned(std::move(request)),
                                                     .response = &response,
                                                     .done = &closure};
        async_delta_writer->write_segment(async_request);
    }

    ASSERT_OK(delta_writer->segment_flush_token()->wait());
    ASSERT_TRUE(closure.has_run());
    EXPECT_TRUE(weak_request.expired());
    RowsetSharedPtr prepared_rowset;
    ASSERT_OK(get_prepared_rowset(_tablet->tablet_id(), delta_writer->txn_id(), _partition_id, &prepared_rowset));
    check_single_segment_rowset_result(prepared_rowset, 10);
    ASSERT_OK(StorageEngine::instance()->txn_manager()->delete_txn(_partition_id, _tablet, delta_writer->txn_id()));
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reads_local_files) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reads_local_files"));
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("seg_repl_read_files").set_min_threads(1).set_max_threads(1).build(&pool));
    auto options = create_replicate_options();
    ReplicateToken token(pool->new_token(ThreadPool::ExecutionMode::SERIAL), &options);

    const std::string segment_path = _primary_tablet_segment_dir + "/segment.dat";
    const std::string delete_path = _primary_tablet_segment_dir + "/delete.dat";
    const std::string update_path = _primary_tablet_segment_dir + "/update.dat";
    const std::string index_path = _primary_tablet_segment_dir + "/vector_index.vi";
    const std::string missing_index_path = _primary_tablet_segment_dir + "/missing_vector_index.vi";
    const std::string segment_content = "segment-data";
    const std::string delete_content = "delete-data";
    const std::string update_content = "update-data";
    const std::string index_content = "index-data";
    write_file(segment_path, segment_content);
    write_file(delete_path, delete_content);
    write_file(update_path, update_content);
    write_file(index_path, index_content);

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(segment_path);
    segment->set_data_size(segment_content.size());
    segment->set_delete_path(delete_path);
    segment->set_delete_data_size(delete_content.size());
    segment->set_update_path(update_path);
    segment->set_update_data_size(update_content.size());

    auto* existing_index = segment->add_seg_indexes();
    existing_index->set_index_type(VECTOR);
    existing_index->set_index_path(index_path);
    auto* missing_index = segment->add_seg_indexes();
    missing_index->set_index_type(VECTOR);
    missing_index->set_index_path(missing_index_path);

    ASSERT_OK(token.submit(std::move(segment), false));
    ASSERT_OK(token.wait());
    EXPECT_TRUE(token.failed_node_ids().empty());
    EXPECT_EQ(1, token.get_stat().num_finished_tasks.load());
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_skips_zero_size_segment_file) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_skips_zero_size_segment_file"));

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(_primary_tablet_segment_dir + "/missing-zero-size-segment.dat");
    segment->set_data_size(0);

    ASSERT_OK(replicate_segment_once(std::move(segment)));
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_sends_segment_to_secondary) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_sends_segment_to_secondary"));
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("seg_repl_send_secondary").set_min_threads(1).set_max_threads(1).build(&pool));

    auto options = create_replicate_options();
    options.timeout_ms = 10;
    PNetworkAddress primary_replica;
    primary_replica.set_host("127.0.0.1");
    primary_replica.set_port(0);
    primary_replica.set_node_id(0);
    PNetworkAddress secondary_replica;
    secondary_replica.set_host("127.0.0.1");
    secondary_replica.set_port(1);
    secondary_replica.set_node_id(1);
    options.replicas = {primary_replica, secondary_replica};

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(_primary_tablet_segment_dir + "/missing-zero-size-segment.dat");
    segment->set_data_size(0);

    ReplicateToken token(pool->new_token(ThreadPool::ExecutionMode::SERIAL), &options);
    ASSERT_OK(token.submit(std::move(segment), true));
    ASSERT_OK(token.wait());
    EXPECT_EQ(1, token.failed_node_ids().size());
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reports_segment_read_error) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reports_segment_read_error"));
    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("seg_repl_read_err").set_min_threads(1).set_max_threads(1).build(&pool));
    auto options = create_replicate_options();
    ReplicateToken token(pool->new_token(ThreadPool::ExecutionMode::SERIAL), &options);

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(_primary_tablet_segment_dir + "/segment.dat");
    segment->set_data_size(-1);

    ASSERT_OK(token.submit(std::move(segment), false));
    auto st = token.wait();
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("negative size"));
    EXPECT_EQ(1, token.get_stat().num_finished_tasks.load());
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reports_short_segment_read_error) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reports_short_segment_read_error"));
    const std::string segment_path = _primary_tablet_segment_dir + "/segment.dat";
    const std::string segment_content = "segment-data";
    write_file(segment_path, segment_content);

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(segment_path);
    segment->set_data_size(segment_content.size() + 1);

    auto st = replicate_segment_once(std::move(segment));
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("Failed to read segment file"));
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reports_delete_read_error) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reports_delete_read_error"));
    const std::string segment_path = _primary_tablet_segment_dir + "/segment.dat";
    const std::string segment_content = "segment-data";
    write_file(segment_path, segment_content);

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(segment_path);
    segment->set_data_size(segment_content.size());
    segment->set_delete_path(_primary_tablet_segment_dir + "/missing-delete.dat");
    segment->set_delete_data_size(1);

    auto st = replicate_segment_once(std::move(segment));
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("delete"));
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reports_update_read_error) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reports_update_read_error"));
    const std::string segment_path = _primary_tablet_segment_dir + "/segment.dat";
    const std::string segment_content = "segment-data";
    write_file(segment_path, segment_content);

    auto segment = std::make_unique<SegmentPB>();
    segment->set_path(segment_path);
    segment->set_data_size(segment_content.size());
    segment->set_update_path(_primary_tablet_segment_dir + "/missing-update.dat");
    segment->set_update_data_size(1);

    auto st = replicate_segment_once(std::move(segment));
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("update"));
}

TEST_F(SegmentFlushExecutorTest, test_segment_replicate_token_reports_index_read_error) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_segment_replicate_token_reports_index_read_error"));
    const std::string index_path = _primary_tablet_segment_dir + "/vector_index_dir.vi";
    ASSERT_OK(fs::create_directories(index_path));

    auto segment = std::make_unique<SegmentPB>();
    auto* index = segment->add_seg_indexes();
    index->set_index_type(VECTOR);
    index->set_index_path(index_path);

    auto st = replicate_segment_once(std::move(segment));
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("index"));
}

TEST_F(SegmentFlushExecutorTest, test_submit_after_cancel) {
    ASSERT_OK(prepare_primary_tablet_segment_dir("./ut_dir/SegmentFlushExecutorTest_test_submit_after_cancel"));
    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    DeltaWriter* delta_writer = async_delta_writer->writer();
    PTabletWriterAddSegmentRequest request;
    std::unique_ptr<starrocks::PUniqueId> id = std::make_unique<starrocks::PUniqueId>();
    id->set_lo(delta_writer->load_id().lo());
    id->set_hi(delta_writer->load_id().hi());
    request.set_allocated_id(id.release());
    request.set_txn_id(delta_writer->txn_id());
    request.set_index_id(delta_writer->index_id());
    request.set_tablet_id(delta_writer->tablet()->tablet_id());
    request.set_eos(true);

    brpc::Controller controller;
    PTabletWriterAddSegmentResult response;
    MockClosure closure;
    // submit should fail after the writer is canceled, and the closure should be run to respond the brpc
    async_delta_writer->cancel(Status::Cancelled("Artificial cancel"));
    Status st = delta_writer->segment_flush_token()->submit(delta_writer, &controller,
                                                            SegmentRequestRef::borrowed(&request), &response, &closure);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(closure.has_run());
}

TEST_F(SegmentFlushExecutorTest, test_abort) {
    ASSERT_OK(prepare_primary_tablet_segment_dir("./ut_dir/SegmentFlushExecutorTest_test_abort"));
    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    async_delta_writer->abort();
    ASSERT_EQ(kAborted, async_delta_writer->writer()->get_state());
}

// Regression test for the duplicate-eos race fixed by serialising the body
// of DeltaWriter::commit() with std::call_once.
//
// In the original code path, two SegmentFlushTask threads servicing
// duplicate tablet_writer_add_segment(eos=true) RPCs would both call
// DeltaWriter::commit() concurrently. Both would observe state == kClosed
// outside _state_lock, fall through into _rowset_writer->build(), race on
// the destination TabletSchemaPB's RepeatedPtrField via concurrent
// add_column() calls, and the loser would crash on a garbage ColumnPB*
// returned by the corrupted Add().
//
// With the fix in place, exactly one caller runs the body; concurrent
// duplicates block inside std::call_once until the body finishes and then
// return the same captured Status. Both calls therefore return Status::OK()
// for a successful commit, which is what SegmentFlushTask relies on (any
// non-OK from commit() triggers _writer->cancel()).
TEST_F(SegmentFlushExecutorTest, test_concurrent_commit_is_serialized) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_concurrent_commit_is_serialized"));
    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    DeltaWriter* delta_writer = async_delta_writer->writer();

    // After open() the inner writer is in kWriting. Transition to kClosed
    // (the state that allows commit() to enter its body and made the race
    // reachable in production).
    ASSERT_OK(delta_writer->close());
    ASSERT_EQ(kClosed, delta_writer->get_state());

    // Two threads race into commit(). A spin-barrier maximises the window
    // for the race; if either thread reached _rowset_writer->build()
    // concurrently with the other under the old code it would corrupt the
    // protobuf and crash. Under the new code exactly one thread runs the
    // body inside std::call_once; the other blocks inside call_once and
    // then reads the same captured Status.
    std::atomic<int> num_ok{0};
    std::atomic<int> num_other{0};
    std::atomic<int> ready{0};
    auto commit_fn = [&] {
        ready.fetch_add(1, std::memory_order_acq_rel);
        while (ready.load(std::memory_order_acquire) < 2) {
            // spin until the sibling has also entered
        }
        Status st = delta_writer->commit();
        if (st.ok()) {
            num_ok.fetch_add(1, std::memory_order_relaxed);
        } else {
            num_other.fetch_add(1, std::memory_order_relaxed);
        }
    };
    std::thread t1(commit_fn);
    std::thread t2(commit_fn);
    t1.join();
    t2.join();

    // Both threads must observe Status::OK() for a successful commit: the
    // one that ran the body from running it, the other from reading the
    // captured _commit_result after call_once returns. No transient or
    // duplicate status is allowed to leak out, since SegmentFlushTask
    // would otherwise cancel the writer on any non-OK return.
    ASSERT_EQ(num_ok.load(), 2) << "both threads must return OK; got num_ok=" << num_ok.load()
                                << " num_other=" << num_other.load();
    ASSERT_EQ(num_other.load(), 0);
    ASSERT_EQ(kCommitted, delta_writer->get_state());

    ASSERT_OK(StorageEngine::instance()->txn_manager()->delete_txn(_partition_id, _tablet, delta_writer->txn_id()));
}

// Sequential variant of the above: a duplicate commit() that arrives after
// the first one has already succeeded must short-circuit to Status::OK()
// via the top-of-function state switch (case kCommitted -> OK). After the
// first commit() returns, _state is kCommitted; subsequent callers observe
// it at the head of commit() and never reach the std::call_once.
TEST_F(SegmentFlushExecutorTest, test_duplicate_commit_after_committed_returns_ok) {
    ASSERT_OK(prepare_primary_tablet_segment_dir(
            "./ut_dir/SegmentFlushExecutorTest_test_duplicate_commit_after_committed_returns_ok"));
    std::shared_ptr<AsyncDeltaWriter> async_delta_writer =
            create_delta_writer(_tablet->tablet_id(), _tablet->schema_hash(), _mem_tracker.get());
    DeltaWriter* delta_writer = async_delta_writer->writer();

    ASSERT_OK(delta_writer->close());
    ASSERT_EQ(kClosed, delta_writer->get_state());

    ASSERT_OK(delta_writer->commit());
    ASSERT_EQ(kCommitted, delta_writer->get_state());

    // The second commit() must be a no-op success: a duplicate
    // tablet_writer_add_segment(eos=true) that arrives after the first
    // already finished should not error or alter state.
    ASSERT_OK(delta_writer->commit());
    ASSERT_EQ(kCommitted, delta_writer->get_state());

    ASSERT_OK(StorageEngine::instance()->txn_manager()->delete_txn(_partition_id, _tablet, delta_writer->txn_id()));
}

} // namespace starrocks
