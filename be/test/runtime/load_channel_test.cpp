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

#include "runtime/load_channel.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/lake_tablets_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/local_tablets_channel.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/uid_util.h"

namespace starrocks {

// 2 senders, 1 index, each index has 2 partitions, each partition has 2 tablets, each tablet has 2 columns
// partition id: 10, 11
// tablet id: 10086, 10087, 10088, 10089
class LoadChannelTestForLakeTablet : public testing::Test {
public:
    LoadChannelTestForLakeTablet() {
        _schema_id = next_id();
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _location_provider = std::make_shared<lake::FixedLocationProvider>(kTestGroupPath);
        _update_manager = std::make_unique<lake::UpdateManager>(_location_provider);
        _tablet_manager = std::make_unique<lake::TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);

        _load_channel_mgr = std::make_unique<LoadChannelMgr>();

        auto metadata = new_tablet_metadata(10086);
        _tablet_schema = TabletSchema::create(metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        // init _open_request
        _open_request.set_is_lake_tablet(true);
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

    std::unique_ptr<lake::TabletMetadata> new_tablet_metadata(int64_t tablet_id) {
        auto metadata = std::make_unique<lake::TabletMetadata>();
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
        Chunk chunk({c0, c1}, _schema);
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
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));

        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10086)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10087)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10088)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10089)));

        auto load_mem_tracker = std::make_unique<MemTracker>(-1, "", _mem_tracker.get());
        _load_channel =
                std::make_shared<LoadChannel>(_load_channel_mgr.get(), _tablet_manager.get(), UniqueId::gen_uid(),
                                              next_id(), string(), 1000, std::move(load_mem_tracker));
    }

    void TearDown() override {
        _load_channel.reset();
        (void)fs::remove_all(kTestGroupPath);
        _tablet_manager->prune_metacache();
    }

    std::shared_ptr<Chunk> read_segment(int64_t tablet_id, const std::string& filename) {
        // Check segment file
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
        auto path = _location_provider->segment_location(tablet_id, filename);

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

    static constexpr int kIndexId = 10;
    static constexpr int64_t kTxnId = 56789;
    static constexpr const char* const kTestGroupPath = "load_channel_test_for_lake_tablet";

    int64_t _schema_id;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<lake::FixedLocationProvider> _location_provider;
    std::unique_ptr<lake::UpdateManager> _update_manager;
    std::unique_ptr<lake::TabletManager> _tablet_manager;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
    PTabletWriterOpenRequest _open_request;

    std::shared_ptr<LoadChannel> _load_channel;
};

TEST_F(LoadChannelTestForLakeTablet, test_simple_write) {
    PTabletWriterOpenRequest open_request = _open_request;
    PTabletWriterOpenResult open_response;
    open_request.set_num_senders(1);
    _load_channel->open(nullptr, open_request, &open_response, nullptr);
    ASSERT_EQ(TStatusCode::OK, open_response.status().status_code());

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);

    PTabletWriterAddChunkRequest add_chunk_request;
    PTabletWriterAddBatchResult add_chunk_response;
    {
        add_chunk_request.set_index_id(kIndexId);
        add_chunk_request.set_sender_id(0);
        add_chunk_request.set_eos(false);
        add_chunk_request.set_packet_seq(0);

        ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
        add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

        for (int i = 0; i < kChunkSize; i++) {
            int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
            add_chunk_request.add_tablet_ids(tablet_id);
            add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
        }
    }

    auto clear_response = [](PTabletWriterAddBatchResult* response) {
        PTabletWriterAddBatchResult tmp;
        response->Swap(&tmp);
    };

    _load_channel->add_chunk(add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
    clear_response(&add_chunk_response);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _load_channel->add_chunk(finish_request, &finish_response);
    ASSERT_EQ(TStatusCode::OK, finish_response.status().status_code());
    ASSERT_EQ(4, finish_response.tablet_vec_size());

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
        auto chunk1 = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSizePerTablet, chunk1->num_rows());
    }
}

TEST_F(LoadChannelTestForLakeTablet, test_write_concurrently) {
    PTabletWriterOpenRequest open_request = _open_request;
    PTabletWriterOpenResult open_response;
    _load_channel->open(nullptr, open_request, &open_response, nullptr);
    ASSERT_EQ(TStatusCode::OK, open_response.status().status_code());

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    constexpr int kNumSenders = 2;
    constexpr int kLookCount = 10;
    constexpr int kSegmentRows = kChunkSizePerTablet * kNumSenders * kLookCount;
    auto chunk = generate_data(kChunkSize);

    std::atomic<bool> started{false};

    auto do_write = [&](int sender_id) {
        while (!started) {
        }
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

            _load_channel->add_chunk(add_chunk_request, &add_chunk_response);
            ASSERT_EQ(TStatusCode::OK, add_chunk_response.status().status_code())
                    << add_chunk_response.status().error_msgs()[0];
        }

        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(sender_id);
        finish_request.set_eos(true);
        finish_request.set_wait_all_sender_close(true);
        finish_request.set_packet_seq(kLookCount);
        finish_request.add_partition_ids(10);
        finish_request.add_partition_ids(11);

        _load_channel->add_chunk(finish_request, &finish_response);
        ASSERT_EQ(TStatusCode::OK, finish_response.status().status_code()) << finish_response.status().error_msgs()[0];
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
        auto chunk1 = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kSegmentRows, chunk1->num_rows());
    }
}

TEST_F(LoadChannelTestForLakeTablet, test_abort) {
    PTabletWriterOpenRequest open_request = _open_request;
    PTabletWriterOpenResult open_response;
    open_request.set_num_senders(1);
    _load_channel->open(nullptr, open_request, &open_response, nullptr);
    ASSERT_EQ(TStatusCode::OK, open_response.status().status_code());

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);
    std::atomic<int> write_count{0};
    std::atomic<bool> stopped{false};
    auto t0 = std::thread([&]() {
        int64_t packet_seq = 0;
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

            _load_channel->add_chunk(add_chunk_request, &add_chunk_response);
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
        _load_channel->add_chunk(finish_request, &finish_response);
        ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
        stopped.store(true);
    });

    while (write_count.load() < 5 && !stopped.load()) {
        std::this_thread::yield();
    }
    ASSERT_FALSE(stopped.load());
    ASSERT_GT(write_count.load(), 0);
    _load_channel->abort();

    t0.join();

    ASSERT_TRUE(_tablet_manager->get_tablet(10086)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10087)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10088)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10089)->get_txn_log(kTxnId).status().is_not_found());
}

TEST_F(LoadChannelTestForLakeTablet, test_incremental_open) {
    std::shared_ptr<TabletsChannel> ch;
    {
        PTabletWriterOpenRequest open_request = _open_request;
        PTabletWriterOpenResult open_response;
        open_request.set_num_senders(1);
        open_request.set_is_incremental(true);
        _load_channel->open(nullptr, open_request, &open_response, nullptr);
        ASSERT_EQ(TStatusCode::OK, open_response.status().status_code()) << open_response.status().error_msgs(0);
        ch = _load_channel->get_tablets_channel(kIndexId);
        ASSERT_NE(nullptr, ch.get());
        // not a local tablet channel
        ASSERT_EQ(nullptr, dynamic_cast<LocalTabletsChannel*>(ch.get()));
    }

    {
        PTabletWriterOpenRequest open_request = _open_request;
        PTabletWriterOpenResult open_response;
        open_request.set_is_incremental(true);
        // open again with incremental info.
        _load_channel->open(nullptr, open_request, &open_response, nullptr);
        EXPECT_EQ(TStatusCode::OK, open_response.status().status_code()) << open_response.status().error_msgs(0);

        auto ch2 = _load_channel->get_tablets_channel(kIndexId);
        ASSERT_NE(nullptr, ch2.get());
        // the channels are the same
        ASSERT_EQ(ch, ch2);
    }
}

TEST_F(LoadChannelTestForLakeTablet, test_add_segment) {
    {
        PTabletWriterOpenRequest open_request = _open_request;
        PTabletWriterOpenResult open_response;
        open_request.set_num_senders(1);
        _load_channel->open(nullptr, open_request, &open_response, nullptr);
        ASSERT_EQ(TStatusCode::OK, open_response.status().status_code());
    }

    {
        PTabletWriterAddSegmentRequest request;
        PTabletWriterAddSegmentResult response;
        request.mutable_id()->CopyFrom(_open_request.id());
        request.set_index_id(kIndexId);
        request.set_tablet_id(10086);
        request.set_txn_id(kTxnId);
        request.set_eos(false);
        request.set_num_segments(0);

        _load_channel->add_segment(nullptr, &request, &response, nullptr);
        EXPECT_EQ(TStatusCode::INTERNAL_ERROR, response.status().status_code());
        EXPECT_EQ("channel is not local tablets channel.", response.status().error_msgs()[0]);
    }
}

} // namespace starrocks
