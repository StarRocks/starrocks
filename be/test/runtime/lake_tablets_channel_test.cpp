// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "runtime/lake_tablets_channel.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "gen_cpp/descriptors.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "util/uid_util.h"

namespace starrocks {

using VSchema = starrocks::vectorized::Schema;
using VChunk = starrocks::vectorized::Chunk;
using Int32Column = starrocks::vectorized::Int32Column;

// 2 senders, 1 index, each index has 2 partitions, each partition has 2 tablets, each tablet has 2 columns
// partition id: 10, 11
// tablet id: 10086, 10087, 10088, 10089
class LakeTabletsChannelTest : public testing::Test {
public:
    LakeTabletsChannelTest() {
        _schema_id = next_id();
        _mem_tracker = std::make_unique<MemTracker>(-1);
        _load_channel_mgr = std::make_unique<LoadChannelMgr>();

        _tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
        _tablet_manager->prune_metacache();

        _location_provider = std::make_unique<lake::FixedLocationProvider>(kTestGroupPath);
        _backup_location_provider = _tablet_manager->TEST_set_location_provider(_location_provider.get());

        auto metadata = new_tablet_metadata(10086);
        _tablet_schema = TabletSchema::create(metadata->schema());
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(*_tablet_schema));

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
        schema->set_compress_kind(COMPRESS_LZ4);
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

    VChunk generate_data(int64_t chunk_size) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        VChunk chunk({c0, c1}, _schema);
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
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_location_provider.get());
        (void)fs::remove_all(kTestGroupPath);
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestGroupPath, lake::kTxnLogDirectoryName)));

        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10086)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10087)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10088)));
        CHECK_OK(_tablet_manager->put_tablet_metadata(*new_tablet_metadata(10089)));

        auto load_mem_tracker = std::make_unique<MemTracker>(-1, "", _mem_tracker.get());
        _load_channel = std::make_shared<LoadChannel>(_load_channel_mgr.get(), UniqueId::gen_uid(), string(), 1000,
                                                      std::move(load_mem_tracker));
        TabletsChannelKey key{UniqueId::gen_uid().to_proto(), 99999};
        _tablets_channel = new_lake_tablets_channel(_load_channel.get(), key, _load_channel->mem_tracker());
    }

    void TearDown() override {
        _tablets_channel.reset();
        _load_channel.reset();
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(10086));
        tablet.delete_txn_log(kTxnId);
        ASSIGN_OR_ABORT(tablet, _tablet_manager->get_tablet(10087));
        tablet.delete_txn_log(kTxnId);
        ASSIGN_OR_ABORT(tablet, _tablet_manager->get_tablet(10088));
        tablet.delete_txn_log(kTxnId);
        ASSIGN_OR_ABORT(tablet, _tablet_manager->get_tablet(10089));
        tablet.delete_txn_log(kTxnId);
        (void)ExecEnv::GetInstance()->lake_tablet_manager()->TEST_set_location_provider(_backup_location_provider);
        (void)fs::remove_all(kTestGroupPath);
        _tablet_manager->prune_metacache();
    }

    std::shared_ptr<VChunk> read_segment(int64_t tablet_id, const std::string& filename) {
        // Check segment file
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(kTestGroupPath));
        auto path = _location_provider->segment_location(tablet_id, filename);
        std::cerr << path << '\n';

        ASSIGN_OR_ABORT(auto seg, Segment::open(_mem_tracker.get(), fs, path, 0, _tablet_schema.get()));

        OlapReaderStatistics statistics;
        vectorized::SegmentReadOptions opts;
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
    static constexpr const char* const kTestGroupPath = "test_lake_tablets_channel";

    int64_t _schema_id;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    lake::TabletManager* _tablet_manager;
    std::unique_ptr<lake::FixedLocationProvider> _location_provider;
    lake::LocationProvider* _backup_location_provider;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
    PTabletWriterOpenRequest _open_request;

    std::shared_ptr<LoadChannel> _load_channel;
    std::shared_ptr<TabletsChannel> _tablets_channel;
};

TEST_F(LakeTabletsChannelTest, test_simple_write) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

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

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);

    // Duplicated request, should be ignored.
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);

    // Out-of-order request, should return error.
    add_chunk_request.set_packet_seq(10);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    // No packet_seq
    add_chunk_request.clear_packet_seq();
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    // No sender_id
    add_chunk_request.set_packet_seq(1);
    add_chunk_request.clear_sender_id();
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    // sender_id < 0
    add_chunk_request.set_sender_id(-1);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    // sender_id too large
    add_chunk_request.set_sender_id(1);
    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    // no chunk and no eos
    add_chunk_request.set_sender_id(0);
    add_chunk_request.clear_chunk();
    _tablets_channel->add_chunk(nullptr, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() != TStatusCode::OK);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
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
        auto chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSizePerTablet, chunk->num_rows());
    }

    // Duplicated eos request should return error
    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
    ASSERT_EQ(TStatusCode::DUPLICATE_RPC_INVOCATION, finish_response.status().status_code());
}

TEST_F(LakeTabletsChannelTest, test_write_partial_partition) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

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

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    // Does not contain partition 11
    finish_request.add_partition_ids(10);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
    ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
    ASSERT_EQ(2, finish_response.tablet_vec_size());

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
        auto chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSizePerTablet, chunk->num_rows());
    }
}

TEST_F(LakeTabletsChannelTest, test_write_concurrently) {
    ASSERT_OK(_tablets_channel->open(_open_request, _schema_param));

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

            _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
            ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);
        }

        PTabletWriterAddChunkRequest finish_request;
        PTabletWriterAddBatchResult finish_response;
        finish_request.set_index_id(kIndexId);
        finish_request.set_sender_id(sender_id);
        finish_request.set_eos(true);
        finish_request.set_packet_seq(kLookCount);
        finish_request.add_partition_ids(10);
        finish_request.add_partition_ids(11);

        _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
        ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
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
        auto chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kSegmentRows, chunk->num_rows());
    }
}

TEST_F(LakeTabletsChannelTest, test_cancel) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

    constexpr int kChunkSize = 128;
    constexpr int kChunkSizePerTablet = kChunkSize / 4;
    auto chunk = generate_data(kChunkSize);
    std::atomic<bool> started{false};
    auto t0 = std::thread([&]() {
        PTabletWriterAddChunkRequest add_chunk_request;
        PTabletWriterAddBatchResult add_chunk_response;
        add_chunk_request.set_index_id(kIndexId);
        add_chunk_request.set_sender_id(0);
        add_chunk_request.set_eos(false);
        int64_t packet_seq = 0;
        while (true) {
            add_chunk_request.set_packet_seq(packet_seq++);

            for (int i = 0; i < kChunkSize; i++) {
                int64_t tablet_id = 10086 + (i / kChunkSizePerTablet);
                add_chunk_request.add_tablet_ids(tablet_id);
                add_chunk_request.add_partition_ids(tablet_id < 10088 ? 10 : 11);
            }

            ASSIGN_OR_ABORT(auto chunk_pb, serde::ProtobufChunkSerde::serialize(chunk));
            add_chunk_request.mutable_chunk()->Swap(&chunk_pb);

            _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
            if (add_chunk_response.status().status_code() != TStatusCode::OK) {
                break;
            }
            started = true;
        }
    });

    while (!started) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    _tablets_channel->cancel();

    t0.join();

    ASSERT_TRUE(_tablet_manager->get_tablet(10086)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10087)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10088)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10089)->get_txn_log(kTxnId).status().is_not_found());
}

TEST_F(LakeTabletsChannelTest, test_write_failed) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

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

    _tablet_manager->delete_tablet(10089);

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_NE(TStatusCode::OK, add_chunk_response.status().status_code());

    _tablets_channel->cancel();

    ASSERT_TRUE(_tablet_manager->get_tablet(10086)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10087)->get_txn_log(kTxnId).status().is_not_found());
    ASSERT_TRUE(_tablet_manager->get_tablet(10088)->get_txn_log(kTxnId).status().is_not_found());
}

TEST_F(LakeTabletsChannelTest, test_empty_tablet) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

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

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
    ASSERT_TRUE(finish_response.status().status_code() == TStatusCode::OK);
    ASSERT_EQ(4, finish_response.tablet_vec_size());

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
            auto chunk = read_segment(tablet_id, txnlog->op_write().rowset().segments(0));
            ASSERT_EQ(kChunkSize, chunk->num_rows());
        } else {
            ASSERT_EQ(0, txnlog->op_write().rowset().segments().size());
        }
    }
}

TEST_F(LakeTabletsChannelTest, test_finish_failed) {
    auto open_request = _open_request;
    open_request.set_num_senders(1);

    ASSERT_OK(_tablets_channel->open(open_request, _schema_param));

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

    _tablets_channel->add_chunk(&chunk, add_chunk_request, &add_chunk_response);
    ASSERT_TRUE(add_chunk_response.status().status_code() == TStatusCode::OK);

    // Drop tablet 10087 and 10088 before finish
    _tablet_manager->delete_tablet(10087);
    _tablet_manager->delete_tablet(10088);

    PTabletWriterAddChunkRequest finish_request;
    PTabletWriterAddBatchResult finish_response;
    finish_request.set_index_id(kIndexId);
    finish_request.set_sender_id(0);
    finish_request.set_eos(true);
    finish_request.set_packet_seq(1);
    finish_request.add_partition_ids(10);
    finish_request.add_partition_ids(11);

    _tablets_channel->add_chunk(nullptr, finish_request, &finish_response);
    ASSERT_NE(TStatusCode::OK, finish_response.status().status_code());
    ASSERT_EQ(2, finish_response.tablet_vec_size());

    std::vector<int64_t> finished_tablets;
    for (auto& info : finish_response.tablet_vec()) {
        ASSERT_TRUE(info.has_schema_hash());
        finished_tablets.emplace_back(info.tablet_id());
    }
    std::sort(finished_tablets.begin(), finished_tablets.end());
    ASSERT_EQ(10086, finished_tablets[0]);
    ASSERT_EQ(10089, finished_tablets[1]);
    // tablet 10086
    {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(10086));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        ASSERT_EQ(1, txnlog->op_write().rowset().segments().size());
        auto chunk = read_segment(10086, txnlog->op_write().rowset().segments(0));
        ASSERT_EQ(kChunkSize, chunk->num_rows());
    }
    // tablet 10089
    {
        ASSIGN_OR_ABORT(auto tablet, _tablet_manager->get_tablet(10089));
        ASSIGN_OR_ABORT(auto txnlog, tablet.get_txn_log(kTxnId));
        ASSERT_EQ(0, txnlog->op_write().rowset().segments().size());
    }
}

} // namespace starrocks
