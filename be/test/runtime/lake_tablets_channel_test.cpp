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

#include "runtime/lake_tablets_channel.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/load_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/async_delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"
#include "util/bthreads/util.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks {

// 2 senders, 1 index, each index has 2 partitions, each partition has 2 tablets, each tablet has 2 columns
// partition id: 10, 11
// tablet id: 10086, 10087, 10088, 10089
class LakeTabletsChannelTestBase : public testing::Test {
public:
    LakeTabletsChannelTestBase(const char* test_directory) : _test_directory(test_directory) {
        _schema_id = next_id();
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _root_profile = std::make_unique<RuntimeProfile>("LoadChannel");
        _location_provider = std::make_unique<lake::FixedLocationProvider>(_test_directory);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);

        _load_channel_mgr = std::make_unique<LoadChannelMgr>();

        auto metadata = new_tablet_metadata(10086);
        _tablet_schema = TabletSchema::create(metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        // init _open_request
        _open_request.mutable_id()->set_hi(456789);
        _open_request.mutable_id()->set_lo(987654);
        _open_request.set_index_id(kIndexId);
        _open_request.set_txn_id(kTxnId);
        _open_request.set_num_senders(2);
        _open_request.set_need_gen_rollup(false);
        _open_request.set_load_channel_timeout_s(10);
        _open_request.set_is_vectorized(true);

        _open_request.mutable_schema()->set_db_id(100);
        _open_request.mutable_schema()->set_table_id(101);
        _open_request.mutable_schema()->set_version(1);
        auto index = _open_request.mutable_schema()->add_indexes();
        index->set_id(kIndexId);
        index->set_schema_hash(0);
        for (int i = 0, sz = metadata->schema().column_size(); i < sz; i++) {
            auto slot = _open_request.mutable_schema()->add_slot_descs();
            slot->set_id(i);
            slot->set_byte_offset(i * sizeof(int) /*unused*/);
            slot->set_col_name(metadata->schema().column(i).name() /*unused*/);
            slot->set_slot_idx(i);
            slot->set_is_materialized(true);
            slot->mutable_slot_type()->add_types()->mutable_scalar_type()->set_type(TYPE_INT);

            index->add_columns(metadata->schema().column(i).name());
        }
        _open_request.mutable_schema()->mutable_tuple_desc()->set_id(1);
        _open_request.mutable_schema()->mutable_tuple_desc()->set_byte_size(8 /*unused*/);
        _open_request.mutable_schema()->mutable_tuple_desc()->set_num_null_bytes(0 /*unused*/);
        _open_request.mutable_schema()->mutable_tuple_desc()->set_table_id(10 /*unused*/);

        _schema_param.reset(new OlapTableSchemaParam());
        _schema_param->init(_open_request.schema());

        // partition 10 tablet 10086
        auto tablet0 = _open_request.add_tablets();
        tablet0->set_partition_id(10);
        tablet0->set_tablet_id(10086);
        // partition 10 tablet 10087
        auto tablet1 = _open_request.add_tablets();
        tablet1->set_partition_id(10);
        tablet1->set_tablet_id(10087);
        // partition 11 tablet 10088
        auto tablet2 = _open_request.add_tablets();
        tablet2->set_partition_id(11);
        tablet2->set_tablet_id(10088);
        // partition 11 tablet 10089
        auto tablet3 = _open_request.add_tablets();
        tablet3->set_partition_id(11);
        tablet3->set_tablet_id(10089);
    }

    std::unique_ptr<TabletMetadata> new_tablet_metadata(int64_t tablet_id) {
        auto metadata = std::make_unique<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = metadata->mutable_schema();
        schema->set_id(_schema_id);
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(0);
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(1);
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        return metadata;
    }

    Chunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        Chunk chunk({std::move(c0), std::move(c1)}, _schema);
        chunk.set_slot_id_to_index(0, 0);
        chunk.set_slot_id_to_index(1, 1);
        return chunk;
    }

    template <class Iterator, class T>
    static bool contains(const Iterator& begin, const Iterator& end, T element) {
        return std::find(begin, end, element) != end;
    }

    template <class Container, class T>
    static bool contains(const Container& container, T element) {
        return contains(container.begin(), container.end(), element);
    }

protected:
    void SetUp() override {
        (void)fs::remove_all(_test_directory);
        CHECK_OK(fs::create_directories(lake::join_path(_test_directory, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_directory, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_directory, lake::kTxnLogDirectoryName)));

        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10086)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10087)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10088)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10089)));

        auto load_mem_tracker = std::make_unique<MemTracker>(-1, "", _mem_tracker.get());
        _load_channel =
                std::make_shared<LoadChannel>(_load_channel_mgr.get(), _tablet_manager.get(), UniqueId::gen_uid(),
                                              next_id(), string(), 1000, std::move(load_mem_tracker));
        TabletsChannelKey key{UniqueId::gen_uid().to_proto(), 0, kIndexId};
        _tablets_channel = new_lake_tablets_channel(_load_channel.get(), _tablet_manager.get(), key,
                                                    _load_channel->mem_tracker(), _root_profile.get());
    }

    void TearDown() override {
        _tablets_channel.reset();
        _load_channel.reset();
        (void)fs::remove_all(_test_directory);
        _tablet_manager->prune_metacache();
    }

    std::shared_ptr<Chunk> read_segment(int64_t tablet_id, const std::string& filename) {
        // Check segment file
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(_test_directory));
        auto path = _location_provider->segment_location(tablet_id, filename);
        std::cerr << path << '\n';

        ASSIGN_OR_ABORT(auto seg, Segment::open(fs, FileInfo{path}, 0, _tablet_schema));

        OlapReaderStatistics statistics;
        SegmentReadOptions opts;
        opts.fs = fs;
        opts.tablet_id = tablet_id;
        opts.stats = &statistics;
        opts.chunk_size = 1024;

        ASSIGN_OR_ABORT(auto seg_iter, seg->new_iterator(*_schema, opts));
        auto read_chunk_ptr = ChunkHelper::new_chunk(*_schema, 1024);
        while (true) {
            auto tmp_chunk = ChunkHelper::new_chunk(*_schema, 1024);
            auto st = seg_iter->get_next(tmp_chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            read_chunk_ptr->append(*tmp_chunk);
        }
        seg_iter->close();
        return read_chunk_ptr;
    }

    static constexpr int kIndexId = 1;
    static constexpr int64_t kTxnId = 12345;

    const char* const _test_directory;
    int64_t _schema_id;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<RuntimeProfile> _root_profile;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::TabletManager> _tablet_manager;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
    PTabletWriterOpenRequest _open_request;
    PTabletWriterOpenResult _open_response;

    std::shared_ptr<LoadChannel> _load_channel;
    std::shared_ptr<TabletsChannel> _tablets_channel;
};

class LakeTabletsChannelTest : public LakeTabletsChannelTestBase {
public:
    LakeTabletsChannelTest() : LakeTabletsChannelTestBase("test_lake_tablets_channel") {}
};

TEST_F(LakeTabletsChannelTest, test_simple_write) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);

    for (int i = 0; i < kChunkSize; i++) {
        int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
        add_chunk_request.add_tablet_ids(tablet_id);
        add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
    }

    bool close_channel;
    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // Duplicated request, should be ignored.
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // Out-of-order request, should return error.
    add_chunk_request.set_packet_seq(10);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // No packet_seq
    add_chunk_request.clear_packet_seq();
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // No sender_id
    add_chunk_request.set_packet_seq(1);
    add_chunk_request.clear_sender_id();
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // sender_id < 0
    add_chunk_request.set_sender_id(-1);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // sender_id too large
    add_chunk_request.set_sender_id(1);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // no chunk and no eos
    add_chunk_request.set_sender_id(0);
    add_chunk_request.clear_chunk();
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    config::stale_memtable_flush_time_sec = 30;
    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
    config::stale_memtable_flush_time_sec = 0;
    ASSERT_EQ(TStatusCode::OK, finish_response.status().status_code());
    ASSERT_EQ(4, finish_response.tablet_vec_size());
    ASSERT_TRUE(close_channel);

    std::vector<int64_t> finished_tablets;
    for (auto& info : finish_response.tablet_vec()) {
        finished_tablets.emplace_back(info.tablet_id());
    }
    std::sort(finished_tablets.begin(), finished_tablets.end());
    ASSERT_EQ(10086, finished_tablets[0]);
    ASSERT_EQ(10087, finished_tablets[1]);
    ASSERT_EQ(10088, finished_tablets[2]);
    ASSERT_EQ(10089, finished_tablets[3]);

    for (auto tablet_id : finished_tablets) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        ASSERT_EQ(1, txnlog->op_write().rowset().segments().size());
        auto tmp_chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSizePerTablet, tmp_chunk->num_rows());
    }

    // Duplicated eos request should return error
    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
    ASSERT_EQ(TStatusCode::DUPLICATE_RPC_INVOCATION, finish_response.status().status_code());
    ASSERT_FALSE(close_channel);
}

TEST_F(LakeTabletsChannelTest, test_write_partial_partition) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 2;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);

    for (int i = 0; i < kChunkSize; i++) {
        int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
        add_chunk_request.add_tablet_ids(tablet_id);
        add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
    }

    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    bool close_channel;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    // Does not contain partition 11
    finish_request.add_partition_ids(10);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
    ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
    ASSERT_EQ(2, finish_response.tablet_vec_size());
    ASSERT_TRUE(close_channel);

    std::vector<int64_t> finished_tablets;
    for (auto& info : finish_response.tablet_vec()) {
        finished_tablets.emplace_back(info.tablet_id());
    }
    std::sort(finished_tablets.begin(), finished_tablets.end());
    ASSERT_EQ(10086, finished_tablets[0]);
    ASSERT_EQ(10087, finished_tablets[1]);

    for (auto tablet_id : finished_tablets) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        ASSERT_EQ(1, txnlog->op_write().rowset().segments().size());
        auto tmp_chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSizePerTablet, tmp_chunk->num_rows());
    }
}

TEST_F(LakeTabletsChannelTest, test_write_concurrently) {
    ASSERT_OK(_tablets_channel->open(_open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    constexpr int kNumSenders = 2;
    constexpr int kLookCount = 10;
    constexpr int kSegmentRows = kChunkSizePerTablet * kNumSenders * kLookCount;
    auto chunk = generate_data(kChunkSize);

    std::atomic<bool> started{false};
    std::atomic<int32_t> close_channel_count{0};

    auto do_write = [&](int sender_id) {
        while (!started) {
        }
        bool close_channel;
        for (int i = 0; i < kLookCount; i++) {
            PTabletWriterAddChunkRequest add_chunk_request;
            PTabletWriterAddBatchResult add_chunk_response;
            add_chunk_request.set_index_id(kIndexId);
            add_chunk_request.set_sender_id(sender_id);
            add_chunk_request.set_eos(false);
            add_chunk_request.set_packet_seq(i);

            for (int j = 0; j < kChunkSize; j++) {
                int64_t tablet_id = 10086 + (j / kChunkSizePerTablet);
                add_chunk_request.add_tablet_ids(tablet_id);
                add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
            }

            ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
            add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

            _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
            ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
            ASSERT_FALSE(close_channel);
        }

        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(sender_id);
        finish_request.set_eos(true);
        finish_request.set_packet_seq(kLookCount);
        finish_request.add_partition_ids(10);
        finish_request.add_partition_ids(11);

        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
        ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
        if (close_channel) {
            close_channel_count.fetch_add(1);
        }
    };

    auto t0 = std::thread([&]() { do_write(0); });
    auto t1 = std::thread([&]() { do_write(1); });
    started = true;
    t0.join();
    t1.join();

    for (auto tablet_id : std::vector<int64_t>{10086, 10087, 10088, 10089}) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        ASSERT_EQ(1, txnlog->op_write().rowset().segments().size());
        auto tmp_chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kSegmentRows, tmp_chunk->num_rows());
    }
    ASSERT_EQ(1, close_channel_count.load());
}

TEST_F(LakeTabletsChannelTest, DISABLED_test_abort) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);
    std::atomic<int> write_count{0};
    std::atomic<bool> stopped{false};
    auto t0 = std::thread([&]() {
        int64_t packet_seq = 0;
        bool close_channel;
        while (true) {
            PTabletWriterAddChunkRequest add_chunk_request;
            PTabletWriterAddBatchResult add_chunk_response;
            add_chunk_request.set_index_id(kIndexId);
            add_chunk_request.set_sender_id(0);
            add_chunk_request.set_eos(false);
            add_chunk_request.set_packet_seq(packet_seq++);

            for (int i = 0; i < kChunkSize; i++) {
                int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
                add_chunk_request.add_tablet_ids(tablet_id);
                add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
            }

            ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
            add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

            _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
            ASSERT_FALSE(close_channel);
            if (add_chunk_response.status().status_code() != TStatusCode::OK) {
                break;
            }
            write_count.fetch_add(1);
        }
        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(0);
        finish_request.set_eos(true);
        finish_request.set_packet_seq(packet_seq++);
        finish_request.add_partition_ids(10);
        finish_request.add_partition_ids(11);
        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
        ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
        ASSERT_TRUE(close_channel);
        stopped.store(true);
    });

    while (write_count.load() < 5 && !stopped.load()) {
        std::this_thread::yield();
    }
    ASSERT_FALSE(stopped.load());
    ASSERT_GT(write_count.load(), 0);
    _tablets_channel->abort();

    t0.join();

    ASSERT_TRUE(_tablet_manager->get_tablet(10086)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10087)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10088)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10089)->get_txn_log(kTxnId).status().is_not_found());
}

TEST_F(LakeTabletsChannelTest, test_write_failed) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);
    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);

    for (int i = 0; i < kChunkSize; i++) {
        int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
        add_chunk_request.add_tablet_ids(tablet_id);
        add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
    }

    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    {
        DeleteTabletRequest request;
        DeleteTabletResponse response;
        request.add_tablet_ids(10089);
        lake::delete_tablets(_tablet_manager.get(), request, &response);

        _tablet_manager->prune_metacache();
    }

    bool close_channel;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_NE(TStatusCode::OK, add_chunk_response.status().status_code());
    ASSERT_FALSE(close_channel);

    _tablets_channel->cancel();

    ASSERT_FALSE(fs::path_exist(_tablet_manager->txn_log_location(10086, kTxnId)));
    ASSERT_FALSE(fs::path_exist(_tablet_manager->txn_log_location(10087, kTxnId)));
    ASSERT_FALSE(fs::path_exist(_tablet_manager->txn_log_location(10088, kTxnId)));
}

TEST_F(LakeTabletsChannelTest, test_empty_tablet) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 12;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);

    // Only tablet 10086 has data
    for (int i = 0; i < kChunkSize; i++) {
        add_chunk_request.add_tablet_ids(10086);
        add_chunk_request.add_partition_ids(10);
    }

    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    bool close_channel;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
    ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
    ASSERT_EQ(4, finish_response.tablet_vec_size());
    ASSERT_TRUE(close_channel);

    std::vector<int64_t> finished_tablets;
    for (auto& info : finish_response.tablet_vec()) {
        ASSERT_TRUE(info.has_schema_hash());
        finished_tablets.emplace_back(info.tablet_id());
    }
    std::sort(finished_tablets.begin(), finished_tablets.end());
    ASSERT_EQ(10086, finished_tablets[0]);
    ASSERT_EQ(10087, finished_tablets[1]);
    ASSERT_EQ(10088, finished_tablets[2]);
    ASSERT_EQ(10089, finished_tablets[3]);

    for (auto tablet_id : finished_tablets) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(tablet_id));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        if (tablet_id == 10086) {
            ASSERT_EQ(1, txnlog->op_write().rowset().segments().size());
            auto tmp_chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
            ASSERT_EQ(kChunkSize, tmp_chunk->num_rows());
        } else {
            ASSERT_EQ(0, txnlog->op_write().rowset().segments().size());
        }
    }
}

TEST_F(LakeTabletsChannelTest, test_finish_failed) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 12;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(false);
    add_chunk_request.set_packet_seq(0);

    // Only tablet 10086 has data
    for (int i = 0; i < kChunkSize; i++) {
        add_chunk_request.add_tablet_ids(10086);
        add_chunk_request.add_partition_ids(10);
    }

    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    bool close_channel;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_FALSE(close_channel);

    // Remove txn log directory before finish
    fs::remove_all(_location_provider->txn_log_root_location(10087));

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
    ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
    ASSERT_TRUE(close_channel);
}

TEST_F(LakeTabletsChannelTest, test_finish_after_abort) {
    auto open_request = _open_request;
    open_request.set_num_senders(2);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    {
        constexpr int kChunkSize = 128;
        constexpr int kChunkSizePerTablet = kChunkSize / 4;
        auto chunk = generate_data(kChunkSize);

        PTabletWriterAddChunkRequest add_chunk_request;
        PTabletWriterAddBatchResult add_chunk_response;
        add_chunk_request.set_index_id(kIndexId);
        add_chunk_request.set_sender_id(0);
        add_chunk_request.set_eos(true);
        add_chunk_request.set_packet_seq(0);

        for (int i = 0; i < kChunkSize; i++) {
            int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
            add_chunk_request.add_tablet_ids(tablet_id);
            add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
        }

        ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
        add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

        bool close_channel;
        _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
        ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
        ASSERT_FALSE(close_channel);

        _tablets_channel->abort();

        _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response, &close_channel);
        ASSERT_EQ(TStatusCode::DUPLICATE_RPC_INVOCATION, add_chunk_response.status().status_code());
        ASSERT_FALSE(close_channel);
    }
    {
        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(1);
        finish_request.set_eos(true);
        finish_request.set_packet_seq(0);

        bool close_channel;
        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
        ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
        ASSERT_GE(finish_response.status().error_msgs_size(), 1);
        const auto& message = finish_response.status().error_msgs(0);
        ASSERT_TRUE(message.find("AsyncDeltaWriter has been closed") != std::string::npos) << message;
        ASSERT_TRUE(close_channel);

        PTabletWriterAddBatchResult finish_response2;
        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response2, &close_channel);
        ASSERT_EQ(TStatusCode::DUPLICATE_RPC_INVOCATION, finish_response2.status().status_code());
        ASSERT_FALSE(close_channel);
    }
}

TEST_F(LakeTabletsChannelTest, test_profile) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, &_open_response, _schema_param, false));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    add_chunk_request.set_index_id(kIndexId);
    add_chunk_request.set_sender_id(0);
    add_chunk_request.set_eos(true);
    add_chunk_request.set_packet_seq(0);

    for (int i = 0; i < kChunkSize; i++) {
        int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
        add_chunk_request.add_tablet_ids(tablet_id);
        add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
    }

    ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
    add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

    bool close_channel;
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    ASSERT_TRUE(close_channel);
    _tablets_channel->update_profile();

    auto* profile = _root_profile->get_child(fmt::format("Index (id={})", kIndexId));
    ASSERT_NE(nullptr, profile);
    ASSERT_EQ(1, profile->get_counter("OpenRpcCount")->value());
    ASSERT_TRUE(profile->get_counter("OpenRpcTime")->value() > 0);
    ASSERT_EQ(1, profile->get_counter("AddChunkRpcCount")->value());
    ASSERT_TRUE(profile->get_counter("AddChunkRpcTime")->value() > 0);
    ASSERT_EQ(chunk.num_rows(), profile->get_counter("AddRowNum")->value());
    auto* replicas_profile = profile->get_child("PeerReplicas");
    ASSERT_NE(nullptr, replicas_profile);
    ASSERT_EQ(4, replicas_profile->get_counter("TabletsNum")->value());
}

struct Param {
    int num_sender;
    int fail_tablet;
};

class LakeTabletsChannelMultiSenderTest : public LakeTabletsChannelTestBase,
                                          public ::testing::WithParamInterface<Param> {
public:
    LakeTabletsChannelMultiSenderTest() : LakeTabletsChannelTestBase("lake_tablets_channel_multi_sender_test") {}
};

TEST_P(LakeTabletsChannelMultiSenderTest, test_dont_write_txn_log) {
    auto num_sender = GetParam().num_sender;
    auto fail_tablet = GetParam().fail_tablet;

    constexpr int kChunkSize = 1024;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);

    SyncPoint::GetInstance()->SetCallBack("AsyncDeltaWriter:enter_finish", [&](void* arg) {
        auto w = static_cast<lake::AsyncDeltaWriter*>(arg);
        LOG(INFO) << "tablet_id=" << w->tablet_id() << " fail_tablet=" << fail_tablet;
        if (w->tablet_id() == fail_tablet) {
            w->close();
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("AsyncDeltaWriter:enter_finish");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto open_request = _open_request;
    open_request.set_sender_id(0);
    open_request.set_num_senders(num_sender);
    open_request.mutable_lake_tablet_params()->set_write_txn_log(false);

    auto open_response = PTabletWriterOpenResult{};

    ASSERT_OK(_tablets_channel->open(open_request, &open_response, _schema_param, false));
    ASSERT_EQ(0, open_response.status().status_code());
    std::atomic<int> close_channel_count{0};
    auto sender_task = [&](int sender_id) {
        PTabletWriterAddChunkRequest add_chunk_request;
        PTabletWriterAddBatchResult add_chunk_response;
        add_chunk_request.set_index_id(kIndexId);
        add_chunk_request.set_sender_id(sender_id);
        add_chunk_request.set_eos(false);
        add_chunk_request.set_packet_seq(0);
        add_chunk_request.set_timeout_ms(30 * 1000);

        for (int i = 0; i < kChunkSize; i++) {
            int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
            add_chunk_request.add_tablet_ids(tablet_id);
            add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
        }

        ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
        add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

        bool close_channel;
        _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response, &close_channel);
        ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
        ASSERT_FALSE(add_chunk_response.has_lake_tablet_data());
        ASSERT_FALSE(close_channel);

        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(sender_id);
        finish_request.set_eos(true);
        finish_request.set_packet_seq(1);
        finish_request.add_partition_ids(10);
        finish_request.add_partition_ids(11);
        finish_request.set_timeout_ms(30 * 1000);

        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response, &close_channel);
        if (close_channel) {
            close_channel_count.fetch_add(1);
        }

        LOG(INFO) << "sender_id=" << sender_id << " #finished_tablets=" << finish_response.tablet_vec_size();

        if (sender_id == 0 && fail_tablet == 0) {
            ASSERT_EQ(TStatusCode::OK, finish_response.status().status_code())
                    << finish_response.status().error_msgs(0);
            ASSERT_TRUE(finish_response.has_lake_tablet_data());
            ASSERT_EQ(4, finish_response.lake_tablet_data().txn_logs_size());
            auto metacache = _tablet_manager->metacache();
            for (auto tablet_id : {10086, 10087, 10088, 10089}) {
                auto txn_log_path = _tablet_manager->txn_log_location(tablet_id, kTxnId);
                ASSERT_TRUE(metacache->lookup_txn_log(txn_log_path));
                ASSERT_TRUE(FileSystem::Default()->path_exists(txn_log_path).is_not_found());
            }
        } else if (sender_id == 0) {
            ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
            ASSERT_EQ(0, finish_response.lake_tablet_data().txn_logs_size());
        } else {
            ASSERT_EQ(0, finish_response.lake_tablet_data().txn_logs_size());
        }
    };

    auto bids = std::vector<bthread_t>{};
    for (int i = 0; i < num_sender; i++) {
        ASSIGN_OR_ABORT(auto bid, bthreads::start_bthread([&, id = i]() { sender_task(id); }));
        bids.push_back(bid);
    }
    for (auto bid : bids) {
        (void)bthread_join(bid, nullptr);
    }
    ASSERT_EQ(1, close_channel_count.load());
}
// clang-format off
INSTANTIATE_TEST_SUITE_P(LakeTabletsChannelMultiSenderTest, LakeTabletsChannelMultiSenderTest,
                         ::testing::Values(Param{1, 0},
                                           Param{2, 0},
                                           Param{8, 0},
                                           Param{1, 10086},
                                           Param{4, 10087}));
// clang-format on
} // namespace starrocks
