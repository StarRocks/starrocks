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

// Issue #78224: on a shared-data primary-key tablet with a CLOUD_NATIVE persistent index, an upsert
// of an existing key can fail to leave a durable delete-vector mark on the superseded row *as the
// persistent-index rebuild sees it*. The next rebuild (owner CN restart / tablet re-placement) then
// finds two live rows for one key, PersistentIndexMemtable::insert() returns AlreadyExist, and every
// publish retry for the table fails forever.
//
// These tests drive the exact publish path the report is on: the load spills, so the PK tablet
// writer builds a per-segment PK sstable eagerly and op_write carries `ssts`. Publish then takes the
// READ-ONLY branch of UpdateManager::_do_update -- index.parallel_get() locates the superseded rows
// without upserting, and the new keys enter the index later via LakePersistentIndex::ingest_sst().
// The primary index is dropped from the cache between transactions, so every publish rebuilds the
// index from segments + delete vectors, which is where a lost mark surfaces.
//
// The key shape is not incidental. pk_index_eager_build_supported() (tablet_writer.cpp) refuses the
// eager SST for a V1-encoded single non-VARCHAR key column, so a one-INT-key tablet never reaches
// this path at all. The reporter's table is PRIMARY KEY(BIGINT, DATE) -- two key columns, 12 encoded
// bytes, matching the `PersistentIndexMemtable<12>` in their error -- so this fixture mirrors it with
// (BIGINT, INT) keys.

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <map>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/config_ingest_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_env.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class LakePkDelvecLoss78224Test : public TestBase {
public:
    LakePkDelvecLoss78224Test() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_two_key_column_metadata();
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    // PRIMARY KEY(k0 BIGINT, k1 INT), value v INT -- the reporter's two-column, 12-byte-encoded key.
    // A single INT key would encode with V1 and pk_index_eager_build_supported() would decline the
    // eager SST, silently taking the load off the path under test.
    static std::shared_ptr<TabletMetadataPB> generate_two_key_column_metadata() {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(next_id());
        metadata->set_version(1);
        metadata->set_cumulative_point(0);
        metadata->set_next_rowset_id(1);
        auto schema = metadata->mutable_schema();
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(65535);
        auto* k0 = schema->add_column();
        k0->set_unique_id(next_id());
        k0->set_name("k0");
        k0->set_type("BIGINT");
        k0->set_is_nullable(false);
        k0->set_is_key(true);
        auto* k1 = schema->add_column();
        k1->set_unique_id(next_id());
        k1->set_name("k1");
        k1->set_type("INT");
        k1->set_is_nullable(false);
        k1->set_is_key(true);
        auto* v = schema->add_column();
        v->set_unique_id(next_id());
        v->set_name("v");
        v->set_type("INT");
        v->set_is_nullable(false);
        v->set_is_key(false);
        v->set_aggregation("REPLACE");
        return metadata;
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        StorageEnv::GetInstance()->parallel_compact_mgr()->TEST_set_tablet_mgr(_tablet_mgr.get());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }
    void TearDown() override { remove_test_dir_ignore_error(); }

    // One chunk of `count` consecutive primary keys starting at `start`; `val_bias` makes a re-upsert
    // of the same keys carry a distinct value so the read-back can tell which row survived.
    Chunk generate_data(int64_t count, int64_t start, int32_t val_bias) {
        auto k0 = Int64Column::create();
        auto k1 = Int32Column::create();
        auto v = Int32Column::create();
        for (int64_t i = 0; i < count; i++) {
            k0->append(start + i);
            k1->append(static_cast<int32_t>((start + i) % 7)); // second key column, derived from the first
            v->append(static_cast<int32_t>((start + i) * 3) + val_bias);
        }
        return Chunk({std::move(k0), std::move(k1), std::move(v)}, _schema);
    }

    // Load `chunks` in one transaction and publish it. With write_buffer_size == 1 each write() flushes,
    // so the load spills and the merge emits several segments plus their eager PK sstables.
    Status load_and_publish(int64_t tablet_id, std::vector<Chunk>& chunks, int64_t new_version, int64_t* ssts_out) {
        ASSIGN_OR_RETURN(auto txn_id, write_txn(tablet_id, chunks));
        if (ssts_out != nullptr) {
            ASSIGN_OR_RETURN(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
            *ssts_out = txn_log->op_write().ssts_size();
        }
        RETURN_IF_ERROR(publish_single_version(tablet_id, new_version, txn_id).status());
        return Status::OK();
    }

    // Write `chunks` as one uncommitted transaction and return its txn id, so a test can inspect or
    // adjust the txn log before publishing it.
    StatusOr<int64_t> write_txn(int64_t tablet_id, std::vector<Chunk>& chunks) {
        const int64_t txn_id = next_id();
        ASSIGN_OR_RETURN(auto delta_writer, DeltaWriterBuilder()
                                                    .set_tablet_manager(_tablet_mgr.get())
                                                    .set_tablet_id(tablet_id)
                                                    .set_txn_id(txn_id)
                                                    .set_partition_id(_partition_id)
                                                    .set_mem_tracker(_mem_tracker.get())
                                                    .set_schema_id(_tablet_schema->id())
                                                    .set_profile(&_dummy_runtime_profile)
                                                    .build());
        RETURN_IF_ERROR(delta_writer->open());
        for (auto& chunk : chunks) {
            std::vector<uint32_t> indexes(chunk.num_rows());
            for (uint32_t i = 0, n = chunk.num_rows(); i < n; i++) {
                indexes[i] = i;
            }
            RETURN_IF_ERROR(delta_writer->write(chunk, indexes.data(), indexes.size()));
        }
        RETURN_IF_ERROR(delta_writer->finish_with_txnlog());
        delta_writer->close();
        return txn_id;
    }

    // PK -> value for the whole tablet at `version`. A duplicated key would inflate the row count
    // above the map size, so both are reported separately.
    std::map<int64_t, int32_t> read_key_values(int64_t tablet_id, int64_t version, int64_t* rows_out) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        std::map<int64_t, int32_t> out;
        int64_t rows = 0;
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 128);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            auto cols = tmp->columns();
            for (size_t i = 0; i < tmp->num_rows(); i++) {
                out[cols[0]->get(i).get_int64()] = cols[2]->get(i).get_int32();
            }
            rows += tmp->num_rows();
        }
        if (rows_out != nullptr) {
            *rows_out = rows;
        }
        return out;
    }

    constexpr static const char* const kTestDirectory = "test_lake_pk_delvec_loss_78224";
    // Three chunks per transaction, so a transaction writes several segments (and several eager SSTs).
    constexpr static int64_t kKeysPerChunk = 100;
    constexpr static int64_t kNumKeys = 3 * kKeysPerChunk;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = next_id();
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

// The reporter's workload: the same key set is re-upserted transaction after transaction while the
// owner CN keeps losing its resident primary index, so every publish rebuilds the index from
// segments + delete vectors. Every publish must succeed, and the tablet must still hold exactly one
// row per key carrying the newest value.
TEST_F(LakePkDelvecLoss78224Test, rebuild_after_repeated_upsert_of_same_keys) {
    ConfigResetGuard<bool> spill_guard(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> buffer_guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> eager_guard(&config::pk_index_eager_build_threshold_bytes, 1);

    const int64_t tablet_id = _tablet_metadata->id();
    constexpr int kRounds = 6;
    int64_t version = 1;
    int32_t last_bias = 0;

    for (int round = 0; round < kRounds; round++) {
        const int32_t val_bias = (round + 1) * 1000000;
        std::vector<Chunk> chunks;
        for (int64_t c = 0; c < 3; c++) {
            chunks.emplace_back(generate_data(kKeysPerChunk, c * kKeysPerChunk, val_bias));
        }
        int64_t ssts = 0;
        auto publish_st = load_and_publish(tablet_id, chunks, ++version, &ssts);
        ASSERT_TRUE(publish_st.ok()) << "round " << round << ": " << publish_st;
        // Guard the premise: the load really did take the eager-SST / read-only publish path.
        ASSERT_GT(ssts, 0) << "round " << round
                           << ": op_write carries no ssts, so publish did not take "
                              "the read-only parallel_get + ingest_sst path";
        last_bias = val_bias;
        // The owner CN loses its resident primary index (restart or tablet re-placement). The next
        // publish must rebuild it from segments + delete vectors.
        _update_mgr->try_remove_primary_index_cache(static_cast<uint32_t>(tablet_id));
    }

    int64_t rows = 0;
    auto kv = read_key_values(tablet_id, version, &rows);
    EXPECT_EQ(kNumKeys, static_cast<int64_t>(kv.size()));
    EXPECT_EQ(kNumKeys, rows) << "a superseded row survived: its delete-vector mark was lost";
    for (int64_t k = 0; k < kNumKeys; k++) {
        auto it = kv.find(k);
        ASSERT_NE(it, kv.end()) << "key " << k << " disappeared";
        EXPECT_EQ(static_cast<int32_t>(k * 3) + last_bias, it->second) << "key " << k << " kept a stale value";
    }
}

// The consolidating writer of a spilled load can end up holding a segment that has no eager PK-index
// SST: SpillMemTableSink's single-flush fast path (write_single_flush) writes a plain segment and
// clears the eager-build flag, while merge_blocks_to_segments() turns eager build back on for the
// cloned merge writers, whose segments do carry SSTs. Publish reads op_write.ssts(i) /
// sst_ranges(i) by SEGMENT index, so the two must stay positionally aligned. When they are not, the
// keys of one segment are ingested under another segment's rssid while the rows that are really in
// that segment never enter the primary index at all -- so the next upsert of those keys finds nothing
// to supersede, writes no delete-vector mark, and the next index rebuild sees two live rows for one
// key (AlreadyExist). This drives the writer through exactly that sequence and pins the invariant.
TEST_F(LakePkDelvecLoss78224Test, eager_pk_ssts_stay_aligned_with_segments) {
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
    ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, next_id()));
    ASSERT_OK(writer->open());

    // The flush that took the single-flush fast path: a plain segment, eager SST skipped.
    auto single_flush_chunk = generate_data(kKeysPerChunk, 0, 0);
    ASSERT_OK(writer->write_single_flush(single_flush_chunk, nullptr, false));
    ASSERT_OK(writer->flush());
    ASSERT_EQ(size_t{1}, writer->segments().size());
    ASSERT_TRUE(writer->ssts().empty()) << "premise: the single-flush segment must carry no eager SST";

    // merge_blocks_to_segments(): eager build back on, then the merge writers' output is consolidated.
    writer->try_enable_pk_index_eager_build();
    ASSERT_TRUE(writer->enable_pk_index_eager_build())
            << "premise: (BIGINT, INT) keys must support eager PK index build";
    ASSIGN_OR_ABORT(auto merge_writer, writer->clone());
    auto merged_chunk = generate_data(kKeysPerChunk, kKeysPerChunk, 0);
    ASSERT_OK(merge_writer->write(merged_chunk));
    ASSERT_OK(merge_writer->finish());
    ASSERT_EQ(size_t{1}, merge_writer->segments().size());
    ASSERT_EQ(size_t{1}, merge_writer->ssts().size()) << "premise: the merge writer must build an eager SST";

    ASSERT_OK(writer->merge_other_writer(merge_writer.get()));
    ASSERT_OK(writer->finish());

    ASSERT_EQ(writer->segments().size(), writer->ssts().size())
            << "publish reads op_write.ssts by segment index, so there must be one entry per segment";
    ASSERT_EQ(writer->segments().size(), writer->sst_ranges().size());
    EXPECT_TRUE(writer->ssts()[0].path.empty()) << "the single-flush segment has no eager SST";
    EXPECT_FALSE(writer->ssts()[1].path.empty()) << "the merged segment's eager SST must stay at its own segment index";

    merge_writer->close();
    writer->close();
}

// The publish side of the same contract: an ssts entry may be an empty placeholder for a segment that
// has no eager SST. That segment's keys are in no ingested sstable, so publish must upsert it into the
// index instead of taking the read-only lookup and ingesting nothing -- otherwise its rows never enter
// the index and the next upsert of those keys cannot mark the rows it supersedes.
TEST_F(LakePkDelvecLoss78224Test, publish_indexes_segment_without_eager_sst) {
    ConfigResetGuard<bool> spill_guard(&config::enable_load_spill, true);
    ConfigResetGuard<int64_t> buffer_guard(&config::write_buffer_size, 1);
    ConfigResetGuard<int64_t> eager_guard(&config::pk_index_eager_build_threshold_bytes, 1);

    const int64_t tablet_id = _tablet_metadata->id();
    int64_t version = 1;

    // Round 1: a normal eager-SST load, then blank the first segment's SST entry so the txn log has
    // the shape a consolidating writer produces -- segment 0 without an eager SST, the rest with one.
    std::vector<Chunk> chunks;
    for (int64_t c = 0; c < 3; c++) {
        chunks.emplace_back(generate_data(kKeysPerChunk, c * kKeysPerChunk, 1000));
    }
    ASSIGN_OR_ABORT(auto txn_id, write_txn(tablet_id, chunks));
    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_GE(txn_log->op_write().ssts_size(), 1) << "premise: the load must take the eager-SST path";
    ASSERT_EQ(txn_log->op_write().rowset().segment_metas_size(), txn_log->op_write().ssts_size());
    auto adjusted = std::make_shared<TxnLog>(*txn_log);
    adjusted->mutable_op_write()->mutable_ssts(0)->clear_name();
    adjusted->mutable_op_write()->mutable_sst_ranges(0)->Clear();
    ASSERT_OK(_tablet_mgr->put_txn_log(adjusted));
    auto publish_st = publish_single_version(tablet_id, ++version, txn_id).status();
    ASSERT_TRUE(publish_st.ok()) << "publish must handle a segment with no eager SST: " << publish_st;

    // Round 2: re-upsert every key with the resident primary index dropped, so publish rebuilds the
    // index from segments + delete vectors. Round 1's segment 0 rows must have made it into the index,
    // otherwise they are not superseded here and survive as duplicates.
    _update_mgr->try_remove_primary_index_cache(static_cast<uint32_t>(tablet_id));
    const int32_t last_bias = 2000;
    std::vector<Chunk> chunks2;
    for (int64_t c = 0; c < 3; c++) {
        chunks2.emplace_back(generate_data(kKeysPerChunk, c * kKeysPerChunk, last_bias));
    }
    int64_t ssts = 0;
    auto st = load_and_publish(tablet_id, chunks2, ++version, &ssts);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_GT(ssts, 0) << "op_write carries no ssts, so publish did not take the read-only path";

    int64_t rows = 0;
    auto kv = read_key_values(tablet_id, version, &rows);
    EXPECT_EQ(kNumKeys, static_cast<int64_t>(kv.size()));
    EXPECT_EQ(kNumKeys, rows) << "a superseded row survived: its delete-vector mark was lost";
    for (int64_t k = 0; k < kNumKeys; k++) {
        auto it = kv.find(k);
        ASSERT_NE(it, kv.end()) << "key " << k << " disappeared";
        EXPECT_EQ(static_cast<int32_t>(k * 3) + last_bias, it->second) << "key " << k << " kept a stale value";
    }
}

} // namespace starrocks::lake
