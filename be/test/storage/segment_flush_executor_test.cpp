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
#include <gtest/gtest.h>

#include <utility>

#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/async_delta_writer.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "testutil/assert.h"

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

    Status prepare_primary_tablet_segment_dir(std::string path) {
        _primary_tablet_segment_dir = std::move(path);
        RETURN_IF_ERROR(fs::remove_all(_primary_tablet_segment_dir));
        return fs::create_directories(_primary_tablet_segment_dir);
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
        request.tablet_schema.columns.push_back(c0);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    TupleDescriptor* _create_tuple_desc() {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->tablet_schema().num_columns(); i++) {
            auto& column = _tablet->tablet_schema().column(i);
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
        std::vector<bool> nullable_tuples = std::vector<bool>{false};
        DescriptorTbl* tbl = nullptr;
        DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size);
        auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
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
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());
        std::vector<uint32_t> column_indexes{0};
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema(), column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, num_rows);
        for (auto i = 0; i < num_rows; ++i) {
            chunk->columns()[0]->append_datum(Datum(static_cast<int32_t>(i)));
        }
        ASSERT_OK(rowset_writer->flush_chunk(*chunk, segment_pb));
        rowset = rowset_writer->build().value();
    }

    void attach_segment_data(SegmentPB& segment_pb, brpc::Controller* controller) {
        std::shared_ptr<FileSystem> fs;
        ASSIGN_OR_ABORT(fs, FileSystem::CreateSharedFromString(segment_pb.path()));
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
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(txn_id, partition_id, &tablet_infos);
        for (auto& [tablet_info, rs] : tablet_infos) {
            if (tablet_info.tablet_id == tablet_id) {
                (*rowset) = rs;
                return Status::OK();
            }
        }
        return Status::NotFound(fmt::format("Rowset not found. tablet_id: {}, txn_id: {}, partition_id: {}", tablet_id,
                                            txn_id, partition_id));
    }

    void check_single_segment_rowset_result(RowsetSharedPtr& rowset, int num_rows) {
        ASSERT_EQ(1, rowset->rowset_meta()->num_segments());
        SegmentReadOptions seg_options;
        ASSIGN_OR_ABORT(seg_options.fs, FileSystem::CreateSharedFromString("posix://"));
        OlapReaderStatistics stats;
        seg_options.stats = &stats;
        std::string segment_file = Rowset::segment_file_path(_tablet->schema_hash_path(), rowset->rowset_id(), 0);
        auto segment = *Segment::open(seg_options.fs, segment_file, 0, &_tablet->tablet_schema());
        ASSERT_EQ(segment->num_rows(), num_rows);
        auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
        auto res = segment->new_iterator(schema, seg_options);
        ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

        auto seg_iterator = res.value();
        auto chunk = ChunkHelper::new_chunk(seg_iterator->schema(), 100);
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

class MockClosure : public ::google::protobuf::Closure {
public:
    MockClosure() = default;
    ~MockClosure() override = default;

    void Run() override { _run.store(true); }

    bool has_run() { return _run.load(); }

private:
    std::atomic_bool _run = false;
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
    AsyncDeltaWriterSegmentRequest async_request{
            .cntl = &controller, .request = &request, .response = &response, .done = &closure};
    async_delta_writer->write_segment(async_request);
    ASSERT_OK(delta_writer->segment_flush_token()->wait());
    ASSERT_TRUE(closure.has_run());
    RowsetSharedPtr prepared_rowset;
    ASSERT_OK(get_prepared_rowset(_tablet->tablet_id(), delta_writer->txn_id(), _partition_id, &prepared_rowset));
    check_single_segment_rowset_result(prepared_rowset, 10);
    ASSERT_OK(StorageEngine::instance()->txn_manager()->delete_txn(_partition_id, _tablet, delta_writer->txn_id()));
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
    Status st = delta_writer->segment_flush_token()->submit(delta_writer, &controller, &request, &response, &closure);
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
} // namespace starrocks
