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

#include "storage/lake/lake_persistent_index.h"

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "base/container/lru_cache.h"
#include "base/debug/trace.h"
#include "base/phmap/btree.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/raw_data_visitor.h"
#include "column/runtime_type_traits.h"
#include "column/serde/column_array_serde.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/config_starlet_fwd.h"
#include "common/thread/threadpool.h"
#include "fs/fs.h"
#include "platform/key_cache.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/lake_persistent_index_key_value_merger.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/parallel_task_runner.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/persistent_index_sstable_fileset.h"
#include "storage/lake/pk_index_pressure_test_util.h"
#include "storage/lake/rowset.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/sstable/block.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"
#include "storage_primitive/primary_key_encoder.h"
#include "test_util.h"
#include "types/datum.h"

namespace starrocks::lake {

class KeyValueMergerTestIterator final : public sstable::Iterator {
public:
    KeyValueMergerTestIterator(std::string key, int64_t version, IndexValue value, uint64_t max_rss_rowid,
                               DelVectorPtr delvec = nullptr)
            : _key(std::move(key)), _max_rss_rowid(max_rss_rowid), _delvec(std::move(delvec)) {
        IndexValuesWithVerPB value_pb;
        auto* entry = value_pb.add_values();
        entry->set_version(version);
        entry->set_rssid(value.get_rssid());
        entry->set_rowid(value.get_rowid());
        _value = value_pb.SerializeAsString();
    }

    bool Valid() const override { return true; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void Seek(const Slice& /*target*/) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { return Slice(_key); }
    Slice value() const override { return Slice(_value); }
    Status status() const override { return Status::OK(); }
    uint64_t max_rss_rowid() const override { return _max_rss_rowid; }
    DelVectorPtr delvec() const override { return _delvec; }

private:
    std::string _key;
    std::string _value;
    uint64_t _max_rss_rowid;
    DelVectorPtr _delvec;
};

class LakePersistentIndexTest : public TestBase {
public:
    void check_read_state_reclaim_progress(bool same_tablet);
    void check_read_state_reservation_demand(bool deferred_initial_load);
    void check_manager_prepare_pressure_flush(int outcome);
    PersistentIndexSstablePB make_pressure_sstable(const std::string& filename) {
        phmap::btree_map<std::string, IndexValueWithVer, std::less<>> data;
        for (uint64_t i = 0; i < 20000; ++i) {
            data.emplace(fmt::format("{:016x}-{:08d}", i * uint64_t{0x9e3779b97f4a7c15}, i),
                         std::make_pair(int64_t{1}, IndexValue(i)));
        }
        ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(_tablet_metadata->id(), filename)));
        uint64_t filesize = 0;
        CHECK_OK(PersistentIndexSstable::build_sstable(data, wf.get(), &filesize, nullptr));
        CHECK_OK(wf->close());
        PersistentIndexSstablePB pb;
        pb.set_filename(filename);
        pb.set_filesize(filesize);
        pb.set_max_rss_rowid(uint64_t{100} << 32);
        return pb;
    }
    LakePersistentIndexTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
        }

        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
        auto key_or = root_encryption_key->generate_key();
        EXPECT_TRUE(key_or.ok());
        std::unique_ptr<EncryptionKey> encryption_key = std::move(key_or.value());
        encryption_key->set_id(2);
        KeyCache::instance().add_key(root_encryption_key);
        KeyCache::instance().add_key(encryption_key);
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    constexpr static const char* const kTestDirectory = "test_lake_persistent_index";

    std::shared_ptr<TabletMetadata> _tablet_metadata;

    std::vector<std::string> sst_inventory(int64_t tablet_id) {
        std::vector<std::string> ssts;
        CHECK_OK(FileSystem::Default()->iterate_dir(_lp->segment_root_location(tablet_id), [&](std::string_view name) {
            if (name.ends_with(".sst")) {
                ssts.emplace_back(name);
            }
            return true;
        }));
        std::sort(ssts.begin(), ssts.end());
        return ssts;
    }

    std::vector<std::string> sst_inventory_delta(const std::vector<std::string>& before,
                                                 const std::vector<std::string>& after) {
        std::vector<std::string> delta;
        std::set_difference(after.begin(), after.end(), before.begin(), before.end(), std::back_inserter(delta));
        return delta;
    }

    void remove_exact_ssts(int64_t tablet_id, const std::vector<std::string>& filenames) {
        for (const auto& filename : filenames) {
            const auto path = _lp->sst_location(tablet_id, filename);
            const auto exists = FileSystem::Default()->path_exists(path);
            if (exists.ok()) {
                CHECK_OK(FileSystem::Default()->delete_file(path));
            } else {
                CHECK(exists.is_not_found()) << path << ": " << exists;
            }
        }
    }

    TabletSchemaPB create_tablet_schema_pb(const std::vector<std::pair<std::string, std::string>>& columns,
                                           int num_key_columns) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(PRIMARY_KEYS);
        schema_pb.set_num_short_key_columns(num_key_columns);
        schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        for (int i = 0; i < (int)columns.size(); ++i) {
            auto* c = schema_pb.add_column();
            c->set_name(columns[i].first);
            c->set_type(columns[i].second);
            c->set_is_key(i < num_key_columns);
            c->set_is_nullable(false);
        }
        return schema_pb;
    }

    VariantPB make_int_variant_pb(int32_t v) {
        VariantPB var;
        TypeDescriptor type_desc(TYPE_INT);
        var.mutable_type()->CopyFrom(type_desc.to_protobuf());
        var.set_value(std::to_string(v));
        return var;
    }

    VariantPB make_string_variant_pb(const std::string& v) {
        VariantPB var;
        TypeDescriptor type_desc(TYPE_VARCHAR);
        var.mutable_type()->CopyFrom(type_desc.to_protobuf());
        var.set_value(v);
        return var;
    }

    VariantPB make_null_variant_pb(LogicalType ltype) {
        VariantPB var;
        TypeDescriptor type_desc(ltype);
        var.mutable_type()->CopyFrom(type_desc.to_protobuf());
        var.set_variant_type(VariantTypePB::NULL_VALUE);
        return var;
    }

    // Build a tablet whose PK is a single VARCHAR column. A single non-encoded varchar key keeps the
    // rebuild scan on the binary key path (ColumnHelper::build_slices) -- the branch reworked by the
    // parallel-scan refactor -- and lets get() be issued with the raw string keys directly.
    std::shared_ptr<TabletMetadata> make_varchar_pk_metadata() {
        auto md = std::make_shared<TabletMetadata>();
        md->set_id(next_id());
        md->set_version(1);
        md->set_enable_persistent_index(true);
        md->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
        auto schema = md->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("VARCHAR");
        c0->set_length(128);
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto c1 = schema->add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        return md;
    }

    // Write rows with keys ["k%08d" % start .. start+n) as ONE rowset (one segment) and append it to
    // `md` (no SSTs, so load_from_lake_tablet does a full cold rebuild). `rowset_id` becomes the rssid
    // base; keep it >=1 so no row lands on value 0 (which the rebuild treats as <= rebuild point). The
    // written keys are appended to `out_keys`.
    void append_cold_rowset(TabletMetadata* md, int start, int n, uint32_t rowset_id,
                            std::vector<std::string>* out_keys, size_t key_padding = 0) {
        auto tablet_schema = std::make_shared<TabletSchema>(md->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(md->id()));
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto writer, tablet.new_writer(kHorizontal, txn_id));
        ASSERT_OK(writer->open());

        std::vector<std::string> keys;
        keys.reserve(n);
        for (int i = 0; i < n; i++) {
            char buf[16];
            snprintf(buf, sizeof(buf), "k%08d", start + i);
            keys.emplace_back(std::string(buf) + std::string(key_padding, 'p'));
        }
        auto kc = BinaryColumn::create();
        auto vc = Int32Column::create();
        for (int i = 0; i < n; i++) {
            kc->append(Slice(keys[i]));
            vc->append(start + i);
        }
        Chunk chunk({std::move(kc), std::move(vc)}, schema);
        ASSERT_OK(writer->write(chunk));
        ASSERT_OK(writer->finish());

        auto* rs = md->add_rowsets();
        rs->set_id(rowset_id);
        rs->set_overlapped(false);
        rs->set_num_rows(writer->num_rows());
        rs->set_data_size(writer->data_size());
        for (const auto& f : writer->segments()) {
            auto* segment = rs->add_segment_metas();
            segment->set_filename(f.path);
            if (!f.encryption_meta.empty()) {
                segment->set_encryption_meta(f.encryption_meta);
            }
        }
        writer->close();

        for (auto& k : keys) {
            out_keys->emplace_back(std::move(k));
        }
    }

    // --- Helpers for load_dels tests. ---
    // A single-VARCHAR-key PK schema is used so the keys round-trip through a BinaryColumn and
    // extract_keys() takes the binary branch (no _key_size init, which only load_from_lake_tablet does).

    std::shared_ptr<TabletSchema> make_varchar_pk_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(PRIMARY_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
        auto* c0 = pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("pk");
        c0->set_type("VARCHAR");
        c0->set_length(128);
        c0->set_index_length(128);
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto* c1 = pb.add_column();
        c1->set_unique_id(1);
        c1->set_name("v");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        return std::make_shared<TabletSchema>(pb);
    }

    // Serialize `keys` as a BinaryColumn and write it as a del file at del_location(tablet_id, name),
    // matching what load_dels' read_one() expects to deserialize.
    void write_binary_del_file(int64_t tablet_id, const std::string& name, const std::vector<std::string>& keys) {
        auto col = BinaryColumn::create();
        for (const auto& k : keys) {
            col->append(Slice(k));
        }
        const int64_t max_size = serde::ColumnArraySerde::max_serialized_size(*col);
        std::vector<uint8_t> buffer(max_size);
        ASSIGN_OR_ABORT(auto* end, serde::ColumnArraySerde::serialize(*col, buffer.data()));
        const size_t used = end - buffer.data();
        auto path = _tablet_mgr->del_location(tablet_id, name);
        WritableFileOptions opts;
        opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE; // tests rewrite the same del path across runs
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(opts, path));
        ASSERT_OK(wf->append(Slice(reinterpret_cast<const char*>(buffer.data()), used)));
        ASSERT_OK(wf->close());
    }

    void ensure_kek_in_key_cache() {
        if (KeyCache::instance().get_key("0000000000000000") != nullptr) {
            return;
        }
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        std::unique_ptr<EncryptionKey> root_key = EncryptionKey::create_from_pb(pb).value();
        auto kek = root_key->generate_key().value();
        kek->set_id(2);
        KeyCache::instance().add_key(root_key);
        KeyCache::instance().add_key(kek);
    }

    PersistentIndexSstablePB* append_all_key_sstable(
            TabletMetadata* metadata, const std::vector<std::tuple<std::string, int64_t, IndexValue>>& entries,
            uint64_t max_rss_rowid) {
        KeyValueMerger merger("", 0, /*merge_base_level=*/false, _tablet_mgr.get(), metadata->id(),
                              /*enable_multiple_output_files=*/false);
        for (const auto& [key, version, value] : entries) {
            KeyValueMergerTestIterator iter(key, version, value, max_rss_rowid);
            EXPECT_OK(merger.merge(&iter));
        }
        auto outputs = merger.finish();
        EXPECT_OK(outputs.status());
        if (!outputs.ok() || outputs->size() != 1) return nullptr;

        const auto& output = outputs->front();
        auto* sstable = metadata->mutable_sstable_meta()->add_sstables();
        sstable->set_filename(output.filename);
        sstable->set_filesize(output.filesize);
        sstable->set_encryption_meta(output.encryption_meta);
        sstable->set_max_rss_rowid(max_rss_rowid);
        sstable->mutable_range()->set_start_key(output.start_key);
        sstable->mutable_range()->set_end_key(output.end_key);
        sstable->mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
        return sstable;
    }

    // Every sstable of a tablet as its own candidate fileset, the shape
    // LakePersistentIndexSizeTieredCompactionStrategy produces for standalone sstables and the
    // shape the parallel compaction manager expects. More than one candidate also keeps the
    // manager on its merge path instead of the single-fileset "move" shortcut.
    static std::vector<std::vector<PersistentIndexSstablePB>> sstables_as_candidate_filesets(
            const PersistentIndexSstableMetaPB& sstable_meta) {
        std::vector<std::vector<PersistentIndexSstablePB>> candidates;
        for (const auto& sstable : sstable_meta.sstables()) {
            candidates.push_back({sstable});
        }
        return candidates;
    }

    // Run |candidates| through the parallel index compaction manager and record the inputs and
    // outputs into |txn_log| exactly as LakePersistentIndex::parallel_major_compact does, but with
    // the candidate set and the base/cumulative decision chosen by the caller instead of by the
    // size-tiered strategy.
    Status parallel_compact_into_txn_log(const TabletMetadataPtr& metadata,
                                         const std::vector<std::vector<PersistentIndexSstablePB>>& candidates,
                                         bool merge_base_level, TxnLogPB* txn_log) {
        LakePersistentIndexParallelCompactMgr compact_mgr(_tablet_mgr.get());
        RETURN_IF_ERROR(compact_mgr.init());
        DeferOp shutdown([&] { compact_mgr.shutdown(); });
        std::vector<PersistentIndexSstablePB> output_sstables;
        RETURN_IF_ERROR(compact_mgr.compact(candidates, metadata, merge_base_level, &output_sstables));

        auto* op_compaction = txn_log->mutable_op_compaction();
        for (const auto& candidate : candidates) {
            for (const auto& sstable_pb : candidate) {
                op_compaction->add_input_sstables()->CopyFrom(sstable_pb);
            }
        }
        if (op_compaction->input_sstables().empty()) {
            return Status::InternalError("no input sstables to compact");
        }
        const uint64_t max_rss_rowid =
                op_compaction->input_sstables(op_compaction->input_sstables_size() - 1).max_rss_rowid();
        for (const auto& sstable_pb : output_sstables) {
            auto* output_sstable = op_compaction->add_output_sstables();
            output_sstable->CopyFrom(sstable_pb);
            output_sstable->set_max_rss_rowid(max_rss_rowid);
        }
        return Status::OK();
    }

    StatusOr<std::vector<std::pair<std::string, IndexValueWithVer>>> read_persistent_index_sstable(
            int64_t tablet_id, const PersistentIndexSstablePB& sstable) {
        std::vector<std::pair<std::string, IndexValueWithVer>> entries;
        RandomAccessFileOptions options;
        if (!sstable.encryption_meta().empty()) {
            ASSIGN_OR_ABORT(options.encryption_info,
                            KeyCache::instance().unwrap_encryption_meta(sstable.encryption_meta()));
        }
        ASSIGN_OR_ABORT(auto file,
                        fs::new_random_access_file(options, _tablet_mgr->sst_location(tablet_id, sstable.filename())));
        sstable::Options table_options;
        std::unique_ptr<sstable::Table> table;
        RETURN_IF_ERROR(sstable::Table::Open(table_options, file.get(), sstable.filesize(), table));
        sstable::ReadOptions read_options;
        std::unique_ptr<sstable::Iterator> iterator(table->NewIterator(read_options));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            IndexValuesWithVerPB values;
            if (!values.ParseFromArray(iterator->value().data, iterator->value().size) || values.values_size() != 1) {
                return Status::Corruption("invalid generic compaction test SST value");
            }
            const auto& value = values.values(0);
            entries.emplace_back(
                    iterator->key().to_string(),
                    IndexValueWithVer{value.version(),
                                      IndexValue((static_cast<uint64_t>(value.rssid()) << 32) | value.rowid())});
        }
        RETURN_IF_ERROR(iterator->status());
        return entries;
    }

    void wait_for_one_async_memtable_flush(LakePersistentIndex* index) {
        std::promise<Status> flushed;
        auto future = flushed.get_future();
        std::atomic<int> callback_count = 0;
        SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::run:after_flush", [&](void* arg) {
            if (callback_count.fetch_add(1) == 0) {
                flushed.set_value(*static_cast<Status*>(arg));
            }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp clear_callback([]() {
            SyncPoint::GetInstance()->ClearCallBack("PersistentIndexMemtable::run:after_flush");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(10)));
        ASSERT_OK(future.get());
        EXPECT_EQ(1, callback_count.load());
    }

    std::vector<IndexValue> cold_reload_values(const TabletMetadataPtr& metadata, int64_t version,
                                               const std::vector<std::string>& keys) {
        Tablet tablet(_tablet_mgr.get(), metadata->id());
        auto reloaded_metadata = std::make_shared<TabletMetadata>();
        reloaded_metadata->CopyFrom(*metadata);
        MetaFileBuilder builder(tablet, reloaded_metadata);
        auto reloaded = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
        CHECK_OK(reloaded->init(metadata));
        CHECK_OK(reloaded->load_from_lake_tablet(_tablet_mgr.get(), metadata, version, &builder));
        std::vector<Slice> slices;
        slices.reserve(keys.size());
        for (const auto& key : keys) {
            slices.emplace_back(key);
        }
        std::vector<IndexValue> values(keys.size());
        CHECK_OK(reloaded->get(keys.size(), slices.data(), values.data()));
        return values;
    }

    void run_complete_checkpoint_cache_only_tail(bool parallel) {
        constexpr uint32_t kEarlierHighRssid = 100;
        constexpr uint32_t kLaterLowRssid = 50;
        constexpr uint32_t kPostFrontierRssid1 = 101;
        constexpr uint32_t kPostFrontierRssid2 = 102;
        constexpr int64_t kFirstCommitVersion = 3;
        constexpr int64_t kLaterCommitVersion = 5;

        auto md = make_varchar_pk_metadata();
        md->set_version(kFirstCommitVersion);
        md->set_next_rowset_id(kPostFrontierRssid2 + 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

        std::vector<std::string> high_keys;
        std::vector<std::string> low_keys;
        std::vector<std::string> post_keys1;
        std::vector<std::string> post_keys2;
        append_cold_rowset(md.get(), 0, 1, kEarlierHighRssid, &high_keys);
        append_cold_rowset(md.get(), 100, 1, kLaterLowRssid, &low_keys);
        md->mutable_rowsets(0)->set_version(2);
        md->mutable_rowsets(1)->set_version(3);

        ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
        ASSERT_OK(index->init(md));

        const Slice high_key(high_keys[0]);
        const Slice low_key(low_keys[0]);
        const IndexValue high_value(uint64_t{kEarlierHighRssid} << 32);
        const IndexValue low_value(uint64_t{kLaterLowRssid} << 32);
        ASSERT_OK(index->insert(1, &high_key, &high_value, 2));
        ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
        ASSERT_OK(index->insert(1, &low_key, &low_value, 3));

        Tablet tablet(_tablet_mgr.get(), md->id());
        auto checkpointed = std::make_shared<TabletMetadata>(*md);
        MetaFileBuilder first_builder(tablet, checkpointed);
        ASSERT_OK(index->commit(&first_builder));
        ASSERT_GT(checkpointed->sstable_meta().sstables_size(), 0);

        append_cold_rowset(checkpointed.get(), 200, 1, kPostFrontierRssid1, &post_keys1);
        append_cold_rowset(checkpointed.get(), 300, 1, kPostFrontierRssid2, &post_keys2);
        checkpointed->mutable_rowsets(2)->set_version(4);
        checkpointed->mutable_rowsets(3)->set_version(5);
        checkpointed->set_version(kLaterCommitVersion);
        checkpointed->set_next_rowset_id(kPostFrontierRssid2 + 1);

        std::array<Slice, 2> post_slices = {Slice(post_keys1[0]), Slice(post_keys2[0])};
        std::array<IndexValue, 2> post_values = {IndexValue(uint64_t{kPostFrontierRssid1} << 32),
                                                 IndexValue(uint64_t{kPostFrontierRssid2} << 32)};
        ASSERT_OK(index->insert(post_slices.size(), post_slices.data(), post_values.data(), kLaterCommitVersion));

        const auto first_sst_meta = checkpointed->sstable_meta().SerializeAsString();
        const auto first_inventory = sst_inventory(md->id());
        auto cache_only = std::make_shared<TabletMetadata>(*checkpointed);
        MetaFileBuilder later_builder(tablet, cache_only);
        ASSERT_OK(index->commit(&later_builder));
        EXPECT_EQ(first_sst_meta, cache_only->sstable_meta().SerializeAsString());
        EXPECT_EQ(first_inventory, sst_inventory(md->id()));
        index.reset();

        // The serial leg drives the cold rebuild onto its single-pass fallback; the parallel leg is
        // the default, with the memory gate opened so it cannot be taken by surprise.
        std::optional<RebuildMemPressureGuard> serial_guard;
        std::optional<ConfigResetGuard<int32_t>> ratio_guard;
        if (parallel) {
            ratio_guard.emplace(&config::pk_index_parallel_rebuild_mem_ratio, 100);
            ASSERT_FALSE(RuntimeEnv::GetInstance()->update_mem_tracker()->limit_exceeded_by_ratio(
                    config::pk_index_parallel_rebuild_mem_ratio));
        } else {
            serial_guard.emplace();
        }
        std::atomic<int> parallel_callbacks = 0;
        std::atomic<int> serial_callbacks = 0;
        SyncPoint::GetInstance()->SetCallBack("LakePersistentIndex::load_from_lake_tablet:parallel",
                                              [&](void*) { ++parallel_callbacks; });
        SyncPoint::GetInstance()->SetCallBack("LakePersistentIndex::load_from_lake_tablet:serial",
                                              [&](void*) { ++serial_callbacks; });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp clear_callbacks([&]() {
            SyncPoint::GetInstance()->ClearCallBack("LakePersistentIndex::load_from_lake_tablet:parallel");
            SyncPoint::GetInstance()->ClearCallBack("LakePersistentIndex::load_from_lake_tablet:serial");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        const std::vector<std::string> keys = {high_keys[0], low_keys[0], post_keys1[0], post_keys2[0]};
        auto values = cold_reload_values(cache_only, kLaterCommitVersion, keys);
        EXPECT_EQ(parallel ? 1 : 0, parallel_callbacks.load());
        EXPECT_EQ(parallel ? 0 : 1, serial_callbacks.load());
        EXPECT_EQ(high_value, values[0]);
        EXPECT_EQ(low_value, values[1]);
        EXPECT_EQ(post_values[0], values[2]);
        EXPECT_EQ(post_values[1], values[3]);
    }
};

TEST_F(LakePersistentIndexTest, test_tombstone_survives_cumulative_then_base_compaction_absorbs_it) {
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->set_version(10);
    constexpr uint64_t old_live_value = (static_cast<uint64_t>(3) << 32) | 7;
    ASSERT_NE(nullptr, append_all_key_sstable(metadata.get(), {{"gone", 10, IndexValue(old_live_value)}}, 100));
    const std::string old_base_filename = metadata->sstable_meta().sstables(0).filename();
    ASSERT_NE(nullptr, append_all_key_sstable(metadata.get(), {{"gone", 20, IndexValue(NullIndexValue)}}, 200));
    ASSERT_NE(nullptr, append_all_key_sstable(metadata.get(), {{"gone", 30, IndexValue(NullIndexValue)}}, 300));

    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(index->init(metadata));
    Slice gone("gone");
    IndexValue value;
    ASSERT_OK(index->get(1, &gone, &value));
    ASSERT_EQ(NullIndexValue, value.get_value());

    // The old live base stays out of a cumulative compaction, so its output
    // must retain the tombstone that prevents resurrection.
    TxnLogPB cumulative_log;
    ASSERT_OK(parallel_compact_into_txn_log(
            metadata, {{metadata->sstable_meta().sstables(1)}, {metadata->sstable_meta().sstables(2)}},
            /*merge_base_level=*/false, &cumulative_log));
    ASSERT_EQ(2, cumulative_log.op_compaction().input_sstables_size());
    for (const auto& input : cumulative_log.op_compaction().input_sstables()) {
        EXPECT_NE(old_base_filename, input.filename());
    }
    ASSERT_EQ(1, cumulative_log.op_compaction().output_sstables_size());
    ASSIGN_OR_ABORT(auto cumulative_entries,
                    read_persistent_index_sstable(metadata->id(), cumulative_log.op_compaction().output_sstables(0)));
    ASSERT_EQ(1, cumulative_entries.size());
    EXPECT_EQ(NullIndexValue, cumulative_entries[0].second.second.get_value());
    ASSERT_OK(index->apply_opcompaction(metadata, cumulative_log.op_compaction()));
    auto cumulative_metadata = std::make_shared<TabletMetadata>(*metadata);
    cumulative_metadata->set_version(11);
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder cumulative_builder(tablet, cumulative_metadata);
    ASSERT_OK(index->commit(&cumulative_builder));
    ASSERT_EQ(2, cumulative_metadata->sstable_meta().sstables_size());
    EXPECT_EQ(old_base_filename, cumulative_metadata->sstable_meta().sstables(0).filename());

    auto cumulative_reopened = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(cumulative_reopened->init(cumulative_metadata));
    ASSERT_OK(cumulative_reopened->get(1, &gone, &value));
    EXPECT_EQ(NullIndexValue, value.get_value());

    // A complete base compaction owns the old live input too, so it may absorb
    // both the live value and its tombstone without publishing another SST.
    TxnLogPB base_log;
    ASSERT_OK(parallel_compact_into_txn_log(cumulative_metadata,
                                            sstables_as_candidate_filesets(cumulative_metadata->sstable_meta()),
                                            /*merge_base_level=*/true, &base_log));
    ASSERT_EQ(2, base_log.op_compaction().input_sstables_size());
    EXPECT_TRUE(base_log.op_compaction().output_sstables().empty());
    ASSERT_OK(cumulative_reopened->apply_opcompaction(cumulative_metadata, base_log.op_compaction()));
    auto final_metadata = std::make_shared<TabletMetadata>(*cumulative_metadata);
    final_metadata->set_version(12);
    MetaFileBuilder base_builder(tablet, final_metadata);
    ASSERT_OK(cumulative_reopened->commit(&base_builder));
    EXPECT_TRUE(final_metadata->sstable_meta().sstables().empty());

    auto final_reopened = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(final_reopened->init(final_metadata));
    ASSERT_OK(final_reopened->get(1, &gone, &value));
    EXPECT_EQ(NullIndexValue, value.get_value());
}

TEST_F(LakePersistentIndexTest, test_basic_api) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int N = 1000;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<size_t> idxes;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        values.emplace_back(i * 2);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
    }
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
    ASSERT_TRUE(index->memory_usage() > 0);

    // test get
    vector<IndexValue> get_values(keys.size());
    ASSERT_TRUE(index->get(N, key_slices.data(), get_values.data()).ok());
    for (int i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], get_values[i]);
    }
    vector<Key> get2_keys;
    vector<Slice> get2_key_slices;
    get2_keys.reserve(N);
    get2_key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        get2_keys.emplace_back(i * 2);
        get2_key_slices.emplace_back((uint8_t*)(&get2_keys[i]), sizeof(Key));
    }
    vector<IndexValue> get2_values(keys.size());
    // should only find 0,2,..N-2, not found: N,N+2, .. N*2-2
    ASSERT_TRUE(index->get(N, get2_key_slices.data(), get2_values.data()).ok());
    for (int i = 0; i < N / 2; ++i) {
        ASSERT_EQ(values[i * 2], get2_values[i]);
    }
    for (int i = N / 2; i < N; ++i) {
        ASSERT_EQ(NullIndexValue, get2_values[i].get_value());
    }

    // test erase
    vector<Key> erase_keys;
    vector<Slice> erase_key_slices;
    erase_keys.reserve(N);
    erase_key_slices.reserve(N);
    size_t num = 0;
    for (int i = 0; i < N + 3; i += 3) {
        erase_keys.emplace_back(i);
        erase_key_slices.emplace_back((uint8_t*)(&erase_keys[num]), sizeof(Key));
        num++;
    }
    vector<IndexValue> erase_old_values(erase_keys.size());
    ASSERT_TRUE(index->erase(num, erase_key_slices.data(), erase_old_values.data(), 1).ok());

    // test upsert
    vector<Key> upsert_keys(N, 0);
    vector<Slice> upsert_key_slices;
    vector<IndexValue> upsert_values(upsert_keys.size());
    upsert_key_slices.reserve(N);
    idxes.clear();
    for (int i = 0; i < N; i++) {
        upsert_keys[i] = i * 2;
        upsert_key_slices.emplace_back((uint8_t*)(&upsert_keys[i]), sizeof(Key));
        upsert_values[i] = i * 3;
        idxes.emplace_back(i);
    }
    vector<IndexValue> upsert_old_values(upsert_keys.size());
    ASSERT_TRUE(index->upsert(N, upsert_key_slices.data(), upsert_values.data(), upsert_old_values.data()).ok());
    config::l0_max_mem_usage = l0_max_mem_usage;
}

// The parallel reverse-lookup in erase (for keys not resolved by the active memtable) must produce
// exactly the same old_values as the serial path. Build an index whose keys all live in sstables,
// then erase every key both with and without parallel execution and compare bit-for-bit.
TEST_F(LakePersistentIndexTest, test_erase_parallel_matches_serial) {
    auto saved_l0_max_mem_usage = config::l0_max_mem_usage;
    auto saved_min_rows = config::pk_index_parallel_execution_min_rows;
    // Tiny l0 budget forces every batch to flush, so all keys end up in sstables (active memtable
    // empty) and the whole delete goes through the reverse lookup.
    config::l0_max_mem_usage = 10;
    DeferOp restore([&]() {
        config::l0_max_mem_usage = saved_l0_max_mem_usage;
        config::pk_index_parallel_execution_min_rows = saved_min_rows;
    });

    using Key = uint64_t;
    const int kBatches = 13;
    const int kPerBatch = 1000;
    const int kTotal = kBatches * kPerBatch; // 13000 keys; with min-rows=1000 -> ~13 concurrent lookup subsets

    // Insert kTotal distinct keys across kBatches flushed batches, then erase all of them and return
    // the reverse-looked-up old values.
    auto build_and_erase = [&](std::vector<IndexValue>* erase_old_values) {
        std::vector<Key> keys(kTotal);
        std::vector<Slice> key_slices(kTotal);
        for (int i = 0; i < kTotal; ++i) {
            keys[i] = i;
            key_slices[i] = Slice((uint8_t*)(&keys[i]), sizeof(Key));
        }
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), _tablet_metadata->id());
        ASSERT_OK(index->init(_tablet_metadata));
        for (int b = 0; b < kBatches; ++b) {
            std::vector<Key> bk(kPerBatch);
            std::vector<Slice> bks(kPerBatch);
            std::vector<IndexValue> bv(kPerBatch);
            for (int j = 0; j < kPerBatch; ++j) {
                int gid = b * kPerBatch + j;
                bk[j] = gid;
                bks[j] = Slice((uint8_t*)(&bk[j]), sizeof(Key));
                bv[j] = IndexValue((uint64_t)(gid + 1)); // distinct, non-null
            }
            index->set_publish_version(b + 1);
            std::vector<IndexValue> old(kPerBatch);
            ASSERT_OK(index->upsert(kPerBatch, bks.data(), bv.data(), old.data()));
            ASSERT_OK(index->flush_memtable(true));
            ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10s: commit every sstable
        }
        erase_old_values->assign(kTotal, IndexValue(NullIndexValue));
        ASSERT_OK(index->erase(kTotal, key_slices.data(), erase_old_values->data(), /*del_rssid=*/12345));
    };

    // erase() only fans out when the unresolved key count exceeds the per-task size, so a min-rows
    // above kTotal keeps the whole reverse lookup on the caller thread.
    std::vector<IndexValue> serial_values;
    config::pk_index_parallel_execution_min_rows = kTotal + 1;
    build_and_erase(&serial_values);

    // A small min-rows lets the parallel path split the delete into many concurrent subsets without
    // needing a huge key count.
    std::vector<IndexValue> parallel_values;
    config::pk_index_parallel_execution_min_rows = 1000;
    build_and_erase(&parallel_values);

    ASSERT_EQ(serial_values.size(), parallel_values.size());
    int found = 0;
    for (int i = 0; i < kTotal; ++i) {
        ASSERT_EQ(serial_values[i].get_value(), parallel_values[i].get_value()) << "mismatch at index " << i;
        // Reverse lookup must have found the value inserted for this key.
        ASSERT_EQ(serial_values[i].get_value(), (uint64_t)(i + 1)) << "wrong old value at index " << i;
        if (serial_values[i].get_value() != NullIndexValue) {
            ++found;
        }
    }
    ASSERT_EQ(found, kTotal); // every key was resolved through the sstable reverse lookup
}

// bulk_erase (ingest a pre-built tombstone sstable, bypassing the memtable) must be observationally
// identical to the memtable erase(): same reverse-looked-up old_values (delvec inputs) AND the deleted
// keys must read back as tombstones while the untouched keys keep their values.
TEST_F(LakePersistentIndexTest, test_bulk_erase_matches_memtable) {
    auto saved_l0 = config::l0_max_mem_usage;
    auto saved_min_rows = config::pk_index_parallel_execution_min_rows;
    config::l0_max_mem_usage = 10;                       // force flushes -> keys live in sstables
    config::pk_index_parallel_execution_min_rows = 1000; // small -> exercise the parallel reverse-lookup
    DeferOp restore([&]() {
        config::l0_max_mem_usage = saved_l0;
        config::pk_index_parallel_execution_min_rows = saved_min_rows;
    });

    using Key = uint64_t;
    const int N = 20000; // total keys
    const int M = 16000; // keys to delete (0..M-1); keep M..N-1
    const uint32_t del_rssid = 100;

    std::vector<Key> keys(N);
    std::vector<Slice> key_slices(N);
    for (int i = 0; i < N; ++i) {
        keys[i] = i;
        key_slices[i] = Slice((uint8_t*)(&keys[i]), sizeof(Key));
    }

    // Build a fresh index with all N keys flushed into sstables (active memtable empty). Value = key+1
    // so every key has a distinct, non-null rss_rowid the reverse lookup must recover.
    auto build_index = [&]() {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), _tablet_metadata->id());
        CHECK_OK(index->init(_tablet_metadata));
        const int batches = 4, per = N / 4;
        for (int b = 0; b < batches; ++b) {
            std::vector<Slice> bks(per);
            std::vector<IndexValue> bv(per);
            for (int j = 0; j < per; ++j) {
                int gid = b * per + j;
                bks[j] = key_slices[gid];
                bv[j] = IndexValue((uint64_t)(gid + 1));
            }
            index->set_publish_version(b + 1);
            std::vector<IndexValue> old(per);
            CHECK_OK(index->upsert(per, bks.data(), bv.data(), old.data()));
            CHECK_OK(index->flush_memtable(true));
            CHECK_OK(index->sync_flush_all_memtables(10000000));
        }
        index->set_publish_version(batches + 1); // version stamped on the delete tombstones
        return index;
    };

    // Path A: memtable erase.
    auto idx_a = build_index();
    std::vector<IndexValue> ov_a(M, IndexValue(NullIndexValue));
    ASSERT_OK(idx_a->erase(M, key_slices.data(), ov_a.data(), del_rssid));

    // Path B: bulk_erase -- pre-build a tombstone sstable from the delete keys (exactly as the writer does
    // at import time), then apply the delete by ingesting it. TableBuilder requires bytewise-ascending
    // keys, so sort a copy for the sstable; the reverse-lookup takes the original key order (independent).
    std::vector<Slice> sorted_del(key_slices.begin(), key_slices.begin() + M);
    std::sort(sorted_del.begin(), sorted_del.end(), [](const Slice& a, const Slice& b) { return a.compare(b) < 0; });
    const std::string del_sst_name = "test_bulk_erase_tombstone.sst";
    ASSIGN_OR_ABORT(auto del_sst_wf,
                    fs::new_writable_file(_tablet_mgr->sst_location(_tablet_metadata->id(), del_sst_name)));
    uint64_t del_sst_filesize = 0;
    PersistentIndexSstableRangePB del_sst_range;
    ASSERT_OK(PersistentIndexSstable::build_tombstone_sstable(sorted_del.data(), M, /*version=*/0, del_sst_wf.get(),
                                                              &del_sst_filesize, &del_sst_range));
    ASSERT_OK(del_sst_wf->close());
    FileMetaPB del_sst_meta;
    del_sst_meta.set_name(del_sst_name);
    del_sst_meta.set_size(del_sst_filesize);

    auto idx_b = build_index();
    std::vector<IndexValue> ov_b(M, IndexValue(NullIndexValue));
    ASSERT_OK(idx_b->bulk_erase(M, key_slices.data(), ov_b.data(), del_rssid, del_sst_meta, del_sst_range,
                                /*version=*/5));

    // 1. Reverse-looked-up old values must match bit-for-bit and recover the inserted value (key+1).
    for (int i = 0; i < M; ++i) {
        ASSERT_EQ(ov_a[i].get_value(), ov_b[i].get_value()) << "old_value mismatch at " << i;
        ASSERT_EQ(ov_a[i].get_value(), (uint64_t)(i + 1)) << "old_value wrong at " << i;
    }

    // 2. Deleted keys must read back as tombstones on both paths.
    std::vector<IndexValue> get_a(M), get_b(M);
    ASSERT_OK(idx_a->get(M, key_slices.data(), get_a.data()));
    ASSERT_OK(idx_b->get(M, key_slices.data(), get_b.data()));
    for (int i = 0; i < M; ++i) {
        ASSERT_EQ(NullIndexValue, get_a[i].get_value()) << "memtable path: key " << i << " not deleted";
        ASSERT_EQ(NullIndexValue, get_b[i].get_value()) << "sst path: key " << i << " not deleted";
    }

    // 3. Untouched keys (M..N-1) must keep their values on both paths.
    const int R = N - M;
    std::vector<IndexValue> keep_a(R), keep_b(R);
    ASSERT_OK(idx_a->get(R, key_slices.data() + M, keep_a.data()));
    ASSERT_OK(idx_b->get(R, key_slices.data() + M, keep_b.data()));
    for (int i = 0; i < R; ++i) {
        ASSERT_EQ(keep_a[i].get_value(), (uint64_t)(M + i + 1)) << "memtable path: kept key wrong at " << i;
        ASSERT_EQ(keep_b[i].get_value(), (uint64_t)(M + i + 1)) << "sst path: kept key wrong at " << i;
    }
}

// Direct SST ingest bypasses the active memtable. A following lower-RSSID write must inherit the direct
// SST's watermark; otherwise commit rejects the resulting file order and a cold reload loses the tail.
TEST_F(LakePersistentIndexTest, test_ingest_sst_preserves_lower_tail_watermark) {
    constexpr uint32_t kDirectRssid = 100;
    constexpr uint32_t kTailRssid = 50;
    constexpr uint64_t kDirectValue = (static_cast<uint64_t>(kDirectRssid) << 32) | (UINT32_MAX - 1);
    constexpr uint64_t kTailValue = static_cast<uint64_t>(kTailRssid) << 32;
    const int64_t tablet_id = _tablet_metadata->id();
    const std::string direct_key = "direct-ingest-key";
    const std::string tail_key = "lower-rssid-tail";
    const std::string filename = "direct-ingest-watermark.sst";

    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> entries;
    entries.emplace(direct_key, std::make_pair(int64_t{1}, IndexValue(kDirectValue)));
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(tablet_id, filename)));
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range;
    ASSERT_OK(PersistentIndexSstable::build_sstable(entries, wf.get(), &filesize, &range));
    ASSERT_OK(wf->close());

    FileMetaPB direct_sst;
    direct_sst.set_name(filename);
    direct_sst.set_size(filesize);
    DelvecPagePB delvec_page;
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    ASSERT_OK(index->ingest_sst(direct_sst, range, kDirectRssid, /*version=*/1, delvec_page, nullptr));

    const Slice tail_slice(tail_key);
    const IndexValue tail_value(kTailValue);
    ASSERT_OK(index->insert(1, &tail_slice, &tail_value, /*version=*/2));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    auto committed = std::make_shared<TabletMetadata>();
    committed->CopyFrom(*_tablet_metadata);
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    ASSERT_GE(committed->sstable_meta().sstables_size(), 2);
    for (int i = 1; i < committed->sstable_meta().sstables_size(); ++i) {
        EXPECT_LE(committed->sstable_meta().sstables(i - 1).max_rss_rowid(),
                  committed->sstable_meta().sstables(i).max_rss_rowid());
    }

    auto reloaded = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(reloaded->init(committed));
    std::array<Slice, 2> keys = {Slice(direct_key), Slice(tail_key)};
    std::array<IndexValue, 2> values;
    ASSERT_OK(reloaded->get(keys.size(), keys.data(), values.data()));
    EXPECT_EQ(IndexValue(kDirectValue), values[0]);
    EXPECT_EQ(IndexValue(kTailValue), values[1]);
}

void LakePersistentIndexTest::check_read_state_reclaim_progress(bool same_tablet) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    const int old_timeout = config::lake_pk_index_publish_wait_timeout_ms;
    const auto old_limit = _mem_tracker->limit();
    config::lake_pk_index_partial_eviction = false;
    config::lake_pk_index_publish_wait_timeout_ms = 1000;
    DeferOp restore([&] {
        config::lake_pk_index_partial_eviction = old_partial;
        config::lake_pk_index_publish_wait_timeout_ms = old_timeout;
        _mem_tracker->set_limit(old_limit);
    });
    auto active = std::make_shared<TabletMetadata>(*_tablet_metadata);
    auto idle = std::make_shared<TabletMetadata>(*_tablet_metadata);
    idle->set_id(next_id());
    for (int i = 0; i < 4; ++i) {
        auto pb = make_pressure_sstable(fmt::format("reclaim-old-{}.sst", i));
        (same_tablet ? active : idle)->mutable_sstable_meta()->add_sstables()->CopyFrom(pb);
    }
    const auto fresh = make_pressure_sstable("reclaim-new.sst");
    if (!same_tablet) {
        Tablet tablet(_tablet_mgr.get(), idle->id());
        MetaFileBuilder builder(tablet, idle);
        std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
        ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(idle, &builder, 1, 2, guard));
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    }
    Tablet tablet(_tablet_mgr.get(), active->id());
    MetaFileBuilder builder(tablet, active);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(active, &builder, 1, 2, guard));
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    Slice tail("memtable-tail");
    IndexValue tail_value(12345);
    ASSERT_OK(index.insert(1, &tail, &tail_value, 2));
    const size_t memtable_bytes = index.memory_stats().memtable_bytes;
    config::lake_pk_index_partial_eviction = true;
    _primary_key_mgr->refresh_index_cache_eviction_policy();
    ASSIGN_OR_ABORT(auto cold,
                    PersistentIndexSstable::new_sstable(
                            fresh, _tablet_mgr->sst_location(active->id(), fresh.filename()), nullptr, true, nullptr,
                            nullptr, nullptr, _primary_key_mgr->sst_read_state_memory_context(), true));
    ASSERT_OK(index.merge_sstable_into_fileset(cold));
    _primary_key_mgr->update_index_cache_size(entry);
    const size_t cache_before = _primary_key_mgr->index_cache().size();
    const auto loads = StorageMetrics::instance()->lake_pk_index_load_total.value();
    // Measure the mandatory index open peak, not a disk-file ratio or just its resident bytes.
    MemTracker probe_tracker(16 * 1024 * 1024);
    auto context = std::make_shared<SstReadStateMemoryContext>(&probe_tracker);
    ASSIGN_OR_ABORT(auto probe, PersistentIndexSstable::new_sstable(
                                        fresh, _tablet_mgr->sst_location(active->id(), fresh.filename()), nullptr,
                                        false, nullptr, nullptr, nullptr, context, true));
    ASSERT_OK(probe->reopen_read_state());
    const int64_t peak = probe_tracker.peak_consumption();
    ASSERT_GT(peak, 4096);
    const int64_t limit = _mem_tracker->consumption() + 4096 + (peak - 4096) / 2;
    ASSERT_LT(peak, limit); // solvable: the mandatory unit is smaller than the entire budget
    ASSERT_LE(_mem_tracker->peak_consumption(), limit);
    _mem_tracker->set_limit(limit);
    // The active entry and tablet guard remain pinned: global idle eviction must skip it.
    const auto start = std::chrono::steady_clock::now();
    const auto key_string = fmt::format("{:016x}-{:08d}", uint64_t{3} * uint64_t{0x9e3779b97f4a7c15}, 3);
    Slice key(key_string);
    IndexValue value;
    ASSERT_OK(index.get(1, &key, &value));
    EXPECT_EQ(IndexValue(3), value);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    EXPECT_LT(_primary_key_mgr->index_cache().size(), cache_before);
    EXPECT_EQ(memtable_bytes, index.memory_stats().memtable_bytes);
    ASSERT_OK(index.get(1, &tail, &value));
    EXPECT_EQ(tail_value, value);
    EXPECT_EQ(loads, StorageMetrics::instance()->lake_pk_index_load_total.value());
    _primary_key_mgr->update_index_cache_size(entry);
    EXPECT_EQ(index.memory_usage() + index.cached_metadata_bytes(), entry->size());
    EXPECT_LE(_mem_tracker->peak_consumption(), limit);
    EXPECT_LE(_mem_tracker->consumption(), limit);
    EXPECT_TRUE(index.is_load(1));
    EXPECT_EQ(same_tablet ? 1 : 2, _primary_key_mgr->index_cache().object_size());
}

TEST_F(LakePersistentIndexTest, test_growth_reservation_uses_short_flush_wait_and_preserves_state) {
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 2);
    ConfigResetGuard<int32_t> timeout(&config::lake_pk_index_publish_wait_timeout_ms, 50);
    const bool old_partial = config::lake_pk_index_partial_eviction;
    const auto old_limit = _mem_tracker->limit();
    config::lake_pk_index_partial_eviction = false;
    DeferOp restore([&] {
        config::lake_pk_index_partial_eviction = old_partial;
        _mem_tracker->set_limit(old_limit);
    });
    Tablet tablet(_tablet_mgr.get(), _tablet_metadata->id());
    MetaFileBuilder builder(tablet, _tablet_metadata);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(_tablet_metadata, &builder, 1, 2, guard));
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    std::vector<int32_t> raw_keys(2000);
    std::iota(raw_keys.begin(), raw_keys.end(), 0);
    std::vector<Slice> keys;
    std::vector<IndexValue> values;
    for (auto& key : raw_keys) {
        keys.emplace_back(reinterpret_cast<const char*>(&key), sizeof(key));
        values.emplace_back(uint64_t{1} << 32 | uint32_t(key));
    }
    ASSERT_OK(index.insert(keys.size(), keys.data(), values.data(), 1));
    std::atomic<bool> hold{true};
    std::atomic<bool> started{false};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void*) {
        started.store(true);
        while (hold.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp drain([&] {
        hold.store(false);
        _mem_tracker->set_limit(old_limit);
        (void)index.sync_flush_all_memtables(10'000'000);
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSERT_OK(index.flush_memtable(true));
    const auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!started.load() && std::chrono::steady_clock::now() < start_deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(started.load());
    config::lake_pk_index_partial_eviction = true;
    _primary_key_mgr->update_index_cache_size(entry);
    const auto before = _mem_tracker->consumption();
    _mem_tracker->set_limit(before + 32768);
    const Slice fresh("not-inserted-before-flush");
    const IndexValue fresh_value(uint64_t{2} << 32);
    const auto begin = std::chrono::steady_clock::now();
    auto status = index.insert(1, &fresh, &fresh_value, 2);
    EXPECT_TRUE(status.is_time_out()) << status;
    EXPECT_NE(std::string::npos, status.to_string().find("PK MemTable growth flush wait"));
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(1));
    EXPECT_EQ(before, _mem_tracker->consumption()); // no mutation or leaked growth credit
    EXPECT_LE(_mem_tracker->peak_consumption(), _mem_tracker->limit());
    std::vector<IndexValue> found(keys.size());
    ASSERT_OK(index.get(keys.size(), keys.data(), found.data()));
    EXPECT_EQ(values, found);
    IndexValue absent;
    ASSERT_OK(index.get(1, &fresh, &absent));
    EXPECT_EQ(IndexValue(NullIndexValue), absent);
    hold.store(false);
    ASSERT_OK(index.sync_flush_all_memtables(10'000'000));
    _primary_key_mgr->update_index_cache_size(entry);
    ASSERT_OK(index.insert(1, &fresh, &fresh_value, 2));
    ASSERT_OK(index.get(1, &fresh, &absent));
    EXPECT_EQ(fresh_value, absent);
    EXPECT_LE(_mem_tracker->peak_consumption(), _mem_tracker->limit());
}

TEST_F(LakePersistentIndexTest, test_active_tablet_reclaims_own_unpinned_ssts_before_waiting) {
    check_read_state_reclaim_progress(true);
}

TEST_F(LakePersistentIndexTest, test_mandatory_sst_reclaims_idle_tablet_before_waiting) {
    check_read_state_reclaim_progress(false);
}

TEST_F(LakePersistentIndexTest, test_mode_transition_serializes_with_delayed_resize) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    DeferOp restore([&] { config::lake_pk_index_partial_eviction = old_partial; });
    config::lake_pk_index_partial_eviction = false;
    auto* entry = _primary_key_mgr->index_cache().get_or_create(_tablet_metadata->id());
    auto guard = entry->value().fetch_guard();
    ASSERT_OK(entry->value().lake_load(_tablet_mgr.get(), _tablet_metadata, 1, nullptr));
    Slice key("tail");
    IndexValue value(42);
    ASSERT_OK(entry->value().insert(1, &key, &value, 2));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto legacy = entry->size();
    const auto metadata = entry->value().cached_metadata_bytes();
    auto* sync = SyncPoint::GetInstance();
    for (bool mode : {true, false}) {
        std::promise<void> paused, resume;
        auto continuing = resume.get_future().share();
        sync->SetCallBack("PrimaryKeyManager::update_index_cache_size:before_accounting_lock", [&](void*) {
            paused.set_value();
            continuing.wait();
        });
        sync->EnableProcessing();
        auto resize = std::async(std::launch::async, [&] { _primary_key_mgr->update_index_cache_size(entry); });
        EXPECT_EQ(std::future_status::ready, paused.get_future().wait_for(std::chrono::seconds(2)));
        config::lake_pk_index_partial_eviction = mode;
        _primary_key_mgr->refresh_index_cache_eviction_policy();
        resume.set_value();
        resize.get();
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
        EXPECT_EQ(legacy + (mode ? metadata : 0), entry->size());
        EXPECT_EQ(entry->size(), _mem_tracker->consumption());
    }
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
}

TEST_F(LakePersistentIndexTest, test_off_transition_prevents_delayed_sweep_recharging_on_bytes) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    config::lake_pk_index_partial_eviction = false;
    DeferOp restore([&] { config::lake_pk_index_partial_eviction = old_partial; });
    auto meta = std::make_shared<TabletMetadata>(*_tablet_metadata);
    meta->mutable_sstable_meta()->add_sstables()->CopyFrom(make_pressure_sstable("mode-sweep.sst"));
    Tablet tablet(_tablet_mgr.get(), meta->id());
    MetaFileBuilder builder(tablet, meta);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(meta, &builder, 1, 2, guard));
    const size_t legacy = entry->size();
    const size_t resident = entry->value().cached_sst_bytes();
    config::lake_pk_index_partial_eviction = true;
    _primary_key_mgr->refresh_index_cache_eviction_policy();
    _primary_key_mgr->index_cache().set_capacity(entry->size(), false);
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
    std::promise<void> paused, resume;
    auto continuing = resume.get_future().share();
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("PrimaryKeyManager::evict_index_cache:before_accounting_lock", [&](void*) {
        paused.set_value();
        continuing.wait();
    });
    sync->EnableProcessing();
    auto sweep = std::async(std::launch::async,
                            [&] { _primary_key_mgr->_evict_index_cache(_primary_key_mgr->index_cache().capacity()); });
    EXPECT_EQ(std::future_status::ready, paused.get_future().wait_for(std::chrono::seconds(2)));
    config::lake_pk_index_partial_eviction = false;
    _primary_key_mgr->refresh_index_cache_eviction_policy();
    resume.set_value();
    sweep.get();
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
    EXPECT_EQ(legacy, _primary_key_mgr->get_index_memory_size(meta->id()));
    auto* actual = _primary_key_mgr->index_cache().get(meta->id());
    ASSERT_NE(nullptr, actual);
    EXPECT_EQ(resident, actual->value().cached_sst_bytes());
    EXPECT_EQ(legacy, _mem_tracker->consumption());
    _primary_key_mgr->release_primary_index_cache(actual);
}

TEST_F(LakePersistentIndexTest, test_partial_eviction_mode_change_reaccounts_untouched_memtable_only_entries) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    config::lake_pk_index_partial_eviction = false;
    DeferOp restore([&] { config::lake_pk_index_partial_eviction = old_partial; });
    std::vector<uint64_t> ids;
    std::vector<size_t> legacy;
    std::vector<size_t> metadata_bytes;
    for (int i = 0; i < 2; ++i) {
        auto meta = std::make_shared<TabletMetadata>(*_tablet_metadata);
        meta->set_id(next_id());
        ids.push_back(meta->id());
        auto* entry = _primary_key_mgr->index_cache().get_or_create(meta->id());
        auto guard = entry->value().fetch_guard();
        ASSERT_OK(entry->value().lake_load(_tablet_mgr.get(), meta, 1, nullptr));
        Slice key("tail");
        IndexValue value(i);
        ASSERT_OK(entry->value().insert(1, &key, &value, 2));
        _primary_key_mgr->update_index_cache_size(entry);
        legacy.push_back(entry->size());
        metadata_bytes.push_back(entry->value().cached_metadata_bytes());
        ASSERT_GT(metadata_bytes.back(), 0);
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    }
    const auto order_before = _primary_key_mgr->index_cache().get_entry_sizes();
    config::lake_pk_index_partial_eviction = true;
    _primary_key_mgr->refresh_index_cache_eviction_policy();
    for (size_t i = 0; i < ids.size(); ++i) {
        EXPECT_EQ(legacy[i] + metadata_bytes[i], _primary_key_mgr->get_index_memory_size(ids[i]));
    }
    // No entry freed SST bytes, and neither was accessed through prepare/lookup after the flip.
    EXPECT_EQ(2, _primary_key_mgr->index_cache().object_size());
    config::lake_pk_index_partial_eviction = false;
    _primary_key_mgr->refresh_index_cache_eviction_policy();
    EXPECT_EQ(order_before, _primary_key_mgr->index_cache().get_entry_sizes());
    for (size_t i = 0; i < ids.size(); ++i) EXPECT_EQ(legacy[i], _primary_key_mgr->get_index_memory_size(ids[i]));
}

TEST_F(LakePersistentIndexTest, bounded_merge_rotates_builders_and_preserves_winners) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, false);
    ensure_kek_in_key_cache();
    constexpr size_t kKeysPerFile = 4;
    constexpr size_t kKeys = 17;
    KeyValueMerger merger("", 0, false, _tablet_mgr.get(), _tablet_metadata->id(), true, kKeysPerFile);
    for (size_t i = 0; i < kKeys; ++i) {
        const auto key = "key_" + std::to_string(1000 + i);
        KeyValueMergerTestIterator original(key, 11, IndexValue((uint64_t{1} << 32) | i), i);
        ASSERT_OK(merger.merge(&original));
        if (i % 3 == 0) {
            KeyValueMergerTestIterator tombstone(key, 12, IndexValue(NullIndexValue), i);
            ASSERT_OK(merger.merge(&tombstone));
        }
    }
    ASSIGN_OR_ABORT(auto outputs, merger.finish());
    ASSERT_EQ(5, outputs.size());
    size_t next = 0;
    for (const auto& output : outputs) {
        PersistentIndexSstablePB pb;
        pb.set_filename(output.filename);
        pb.set_filesize(output.filesize);
        pb.set_encryption_meta(output.encryption_meta);
        RandomAccessFileOptions file_options;
        if (!pb.encryption_meta().empty()) {
            ASSIGN_OR_ABORT(file_options.encryption_info,
                            KeyCache::instance().unwrap_encryption_meta(pb.encryption_meta()));
        }
        ASSIGN_OR_ABORT(auto rf,
                        fs::new_random_access_file(file_options,
                                                   _tablet_mgr->sst_location(_tablet_metadata->id(), pb.filename())));
        PersistentIndexSstable sst;
        ASSERT_OK(sst.init(std::move(rf), pb, nullptr, false, nullptr, _tablet_metadata, _tablet_mgr.get()));
        sstable::ReadOptions options;
        options.verify_checksums = true;
        std::unique_ptr<sstable::Iterator> iter(sst.new_iterator(options));
        size_t count = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            ASSERT_LT(next, kKeys);
            ASSERT_EQ("key_" + std::to_string(1000 + next), iter->key().to_string());
            IndexValuesWithVerPB value;
            ASSERT_TRUE(value.ParseFromArray(iter->value().data, iter->value().size));
            ASSERT_EQ(1, value.values_size());
            const bool tombstone = next % 3 == 0;
            EXPECT_EQ(tombstone ? 12 : 11, value.values(0).version());
            EXPECT_EQ(tombstone ? NullIndexValue : (uint64_t{1} << 32) | next,
                      (uint64_t{value.values(0).rssid()} << 32) | value.values(0).rowid());
            ++next;
            ++count;
        }
        ASSERT_OK(iter->status());
        EXPECT_LE(count, kKeysPerFile);
    }
    EXPECT_EQ(kKeys, next);
}

void LakePersistentIndexTest::check_read_state_reservation_demand(bool deferred_initial_load) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, deferred_initial_load);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(make_pressure_sstable("one-resident.sst"));
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    DeferOp release([&] {
        guard.reset();
        if (entry) _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    const auto key_string = fmt::format("{:016x}-{:08d}", uint64_t{3} * uint64_t{0x9e3779b97f4a7c15}, 3);
    Slice sst_key(key_string);
    IndexValue sst_value;
    ASSERT_OK(index.get(1, &sst_key, &sst_value));
    ASSERT_EQ(IndexValue(3), sst_value);
    _primary_key_mgr->update_index_cache_size(entry);
    const size_t resident = index.sst_memory_demand();
    ASSERT_GT(resident, 0);
    EXPECT_EQ(deferred_initial_load ? 0 : resident, index.cached_sst_bytes());
    Slice tail("memtable-tail");
    IndexValue tail_value(12345);
    ASSERT_OK(index.insert(1, &tail, &tail_value, 2));
    const size_t memtable_bytes = index.memory_stats().memtable_bytes;
    ASSERT_EQ(deferred_initial_load ? 0 : resident, index.evict_sstable_read_state(resident));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto loads = StorageMetrics::instance()->lake_pk_index_load_total.value();
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
    entry = nullptr;

    config::lake_pk_index_partial_eviction = true;
    std::atomic<int> opens{0};
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("PersistentIndexSstable::init:table_open_error", [&](void*) { ++opens; });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    // The manager must budget the same object bytes as prepare_sstable_read_state. With table
    // bytes alone, a tablet containing just this SST can never select its only known read state.
    ASSIGN_OR_ABORT(entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    EXPECT_GT(index.sst_read_state_reservation_demand(), resident);
    EXPECT_EQ(resident, index.cached_sst_bytes());
    EXPECT_EQ(1, opens.load());
    std::array<Slice, 2> keys{sst_key, tail};
    std::array<IndexValue, 2> values;
    for (int i = 0; i < 2; ++i) {
        ASSERT_OK(index.get(keys.size(), keys.data(), values.data()));
        EXPECT_EQ(IndexValue(3), values[0]);
        EXPECT_EQ(tail_value, values[1]);
    }
    EXPECT_EQ(1, opens.load());
    EXPECT_EQ(memtable_bytes, index.memory_stats().memtable_bytes);
    EXPECT_EQ(loads, StorageMetrics::instance()->lake_pk_index_load_total.value());
    _primary_key_mgr->update_index_cache_size(entry);
    EXPECT_EQ(index.memory_usage() + index.cached_metadata_bytes(), entry->size());

    const size_t exact_budget = index.sst_read_state_reservation_demand();
    ASSERT_OK(index.prepare_sstable_read_state(exact_budget - 1, true));
    EXPECT_EQ(0, index.memory_stats().index_block_bytes);
    EXPECT_EQ(1, opens.load());
    ASSERT_OK(index.prepare_sstable_read_state(exact_budget, true));
    _primary_key_mgr->update_index_cache_size(entry);
    EXPECT_EQ(resident, index.cached_sst_bytes());
    EXPECT_EQ(2, opens.load());
    EXPECT_EQ(index.memory_usage() + index.cached_metadata_bytes(), entry->size());
}

TEST_F(LakePersistentIndexTest, bounded_concatenation_preserves_all_encoded_versions) {
    ConfigResetGuard<bool> tde(&config::enable_transparent_data_encryption, true);
    ensure_kek_in_key_cache();
    KeyValueMerger merger("", 0, false, _tablet_mgr.get(), _tablet_metadata->id(), true, 4, 512);
    std::map<std::string, std::string> expected;
    for (size_t i = 0; i < 19; ++i) {
        IndexValuesWithVerPB versions;
        for (int v : {12, 11}) {
            auto* entry = versions.add_values();
            entry->set_version(v);
            entry->set_rssid(v == 12 ? UINT32_MAX : 1);
            entry->set_rowid(v == 12 ? UINT32_MAX : i);
        }
        auto key = fmt::format("key_{:04d}", i);
        auto value = versions.SerializeAsString();
        ASSERT_OK(merger.append_encoded(key, value));
        expected.emplace(key, value);
    }
    ASSIGN_OR_ABORT(auto outputs, merger.finish());
    ASSERT_GT(outputs.size(), 1);
    size_t seen = 0;
    for (const auto& output : outputs) {
        PersistentIndexSstablePB pb;
        pb.set_filename(output.filename);
        pb.set_filesize(output.filesize);
        pb.set_encryption_meta(output.encryption_meta);
        ASSIGN_OR_ABORT(auto sst,
                        PersistentIndexSstable::new_sstable(
                                pb, _tablet_mgr->sst_location(_tablet_metadata->id(), pb.filename()), nullptr, false));
        sstable::ReadOptions options;
        options.verify_checksums = true;
        std::unique_ptr<sstable::Iterator> iter(sst->new_iterator(options));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            ASSERT_TRUE(expected.contains(iter->key().to_string()));
            EXPECT_EQ(expected.at(iter->key().to_string()), iter->value().to_string());
            ++seen;
        }
        ASSERT_OK(iter->status());
    }
    EXPECT_EQ(expected.size(), seen);
}

TEST_F(LakePersistentIndexTest, pressure_coalesce_is_bounded_cancel_safe_and_preserves_base_sst) {
    ConfigResetGuard<bool> tde(&config::enable_transparent_data_encryption, true);
    ensure_kek_in_key_cache();
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    auto* base = append_all_key_sstable(metadata.get(), {{"base", 1, IndexValue(7)}}, 7);
    ASSERT_NE(nullptr, base);
    const std::string base_filename = base->filename();
    const auto tail_fileset = UniqueId::gen_uid().to_proto();
    std::vector<std::string> keys;
    std::vector<IndexValue> expected;
    for (int i = 0; i < 32; ++i) {
        auto key = i == 0 ? std::string("base") : fmt::format("tail_{:04d}_{}", i, std::string(180, 'x'));
        auto value = i % 3 == 0 ? IndexValue(NullIndexValue) : IndexValue((uint64_t{2} << 32) | i);
        auto* pb = append_all_key_sstable(metadata.get(), {{key, 2, value}}, (uint64_t{2} << 32) | 31);
        ASSERT_NE(nullptr, pb);
        pb->mutable_fileset_id()->CopyFrom(tail_fileset);
        keys.push_back(std::move(key));
        expected.push_back(value);
    }
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(8 * 1024 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto before = tracker->consumption();
    auto refresh = [&] { _primary_key_mgr->update_index_cache_size(entry); };
    int checks = 0;
    auto cancelled = index._coalesce_pressure_filesets(1024 * 1024, refresh, [&] { return ++checks == 17; });
    ASSERT_TRUE(cancelled.is_cancelled()) << cancelled;
    EXPECT_EQ(2, index._sstable_filesets.size());
    EXPECT_EQ(before, tracker->consumption());
    std::atomic<bool> scope_cancelled{false};
    ASSERT_OK(index._coalesce_pressure_filesets(1024 * 1024, refresh, [&] { return scope_cancelled.load(); }));
    scope_cancelled.store(true); // installed outputs must use the ordinary, unscoped context

    EXPECT_LT(tracker->consumption(), before);
    std::vector<Slice> slices;
    for (const auto& key : keys) slices.emplace_back(key);
    std::vector<IndexValue> found(keys.size());
    ASSERT_OK(index.get(slices.size(), slices.data(), found.data()));
    EXPECT_EQ(expected, found);
    Slice base_key("base");
    IndexValue base_value;
    ASSERT_OK(index.get(1, &base_key, &base_value));
    EXPECT_EQ(IndexValue(NullIndexValue), base_value);
    PersistentIndexSstableMetaPB committed;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&committed);
    ASSERT_EQ(2, committed.sstables_size());
    EXPECT_EQ(base_filename, committed.sstables(0).filename());
    EXPECT_EQ((uint64_t{2} << 32) | 31, committed.sstables(1).max_rss_rowid());
    auto reload_metadata = std::make_shared<TabletMetadata>(*metadata);
    reload_metadata->mutable_sstable_meta()->CopyFrom(committed);
    reload_metadata->set_version(2);
    {
        LakePersistentIndex reloaded(_tablet_mgr.get(), metadata->id());
        ASSERT_OK(reloaded.init(reload_metadata));
        ASSERT_OK(reloaded.get(1, &base_key, &base_value));
        EXPECT_EQ(IndexValue(NullIndexValue), base_value); // base live value must not resurrect
    }
    const IndexValue replacement((uint64_t{3} << 32) | 7);
    IndexValue old;
    index.set_publish_version(3);
    ASSERT_OK(index.upsert(1, &base_key, &replacement, &old));
    EXPECT_EQ(IndexValue(NullIndexValue), old);
    ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
    reload_metadata->clear_sstable_meta();
    for (const auto& fileset : index._sstable_filesets)
        fileset->get_all_sstable_pbs(reload_metadata->mutable_sstable_meta());
    reload_metadata->set_version(3);
    LakePersistentIndex reinserted(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(reinserted.init(reload_metadata));
    ASSERT_OK(reinserted.get(1, &base_key, &base_value));
    EXPECT_EQ(replacement, base_value);
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, pressure_coalesce_no_gain_group_refunds_before_next_group) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    const auto first_id = UniqueId::gen_uid().to_proto();
    std::vector<std::string> unchanged;
    for (int file = 0; file < 8; ++file) {
        std::vector<std::tuple<std::string, int64_t, IndexValue>> entries;
        for (int i = 0; i < 1500; ++i) {
            const int row = file * 1500 + i;
            entries.emplace_back(fmt::format("a_{:06d}", row), 1, IndexValue((uint64_t{1} << 32) | row));
        }
        auto* pb = append_all_key_sstable(metadata.get(), entries, (uint64_t{1} << 32) | 11999);
        ASSERT_NE(nullptr, pb);
        pb->mutable_fileset_id()->CopyFrom(first_id);
        unchanged.push_back(pb->filename());
    }
    const auto second_id = UniqueId::gen_uid().to_proto();
    for (int i = 0; i < 32; ++i) {
        auto* pb = append_all_key_sstable(metadata.get(), {{fmt::format("b_{:04d}", i), 2, IndexValue(i)}},
                                          (uint64_t{2} << 32) | 31);
        ASSERT_NE(nullptr, pb);
        pb->mutable_fileset_id()->CopyFrom(second_id);
    }
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(8 * 1024 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto before = tracker->consumption();
    auto refresh = [&] { _primary_key_mgr->update_index_cache_size(entry); };
    const auto begin = std::chrono::steady_clock::now();
    ASSERT_OK(index._coalesce_pressure_filesets(1024 * 1024, refresh, [] { return false; }));
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(5));
    ASSERT_EQ(2, index._sstable_filesets.size());
    EXPECT_TRUE(
            index._sstable_filesets[0]->pressure_coalesce_unproductive(256 * 1024, config::pk_index_target_file_size));
    PersistentIndexSstableMetaPB after;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&after);
    ASSERT_EQ(9, after.sstables_size());
    for (size_t i = 0; i < unchanged.size(); ++i) EXPECT_EQ(unchanged[i], after.sstables(i).filename());
    EXPECT_LT(tracker->consumption(), before);
    const auto charge = tracker->consumption();
    const auto inventory = sst_inventory(metadata->id());
    ASSERT_OK(index._coalesce_pressure_filesets(1024 * 1024, refresh, [] { return false; }));
    EXPECT_EQ(inventory, sst_inventory(metadata->id())); // no repeat rewrite of the unchanged group
    EXPECT_EQ(charge, tracker->consumption());
    std::array<Slice, 2> keys{Slice("a_011999"), Slice("b_0031")};
    std::array<IndexValue, 2> values;
    ASSERT_OK(index.get(keys.size(), keys.data(), values.data()));
    EXPECT_EQ(IndexValue((uint64_t{1} << 32) | 11999), values[0]);
    EXPECT_EQ(IndexValue(31), values[1]);
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, pressure_coalesce_retains_unshared_pb_projections) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    for (int group = 0; group < 2; ++group) {
        const auto fileset_id = UniqueId::gen_uid().to_proto();
        for (int i = 0; i < 8; ++i) {
            const auto key = fmt::format("projected_{}_{:04d}", group, i);
            const auto rssid = group == 0 ? 5 : 11;
            auto* pb = append_all_key_sstable(metadata.get(), {{key, 1, IndexValue((uint64_t{1} << 32) | i)}},
                                              (static_cast<uint64_t>(rssid) << 32) | 7);
            ASSERT_NE(nullptr, pb);
            pb->mutable_fileset_id()->CopyFrom(fileset_id);
            pb->set_shared(false); // Physical sharing does not determine read-time projection.
            pb->set_generation_version(1);
            if (group == 0) {
                pb->set_shared_rssid(5);
                pb->set_shared_version(7);
            } else {
                pb->set_rssid_offset(10);
            }
        }
    }
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(8 * 1024 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto charge = tracker->consumption();
    const auto inventory = sst_inventory(metadata->id());
    ASSERT_OK(index._coalesce_pressure_filesets(
            1024 * 1024, [&] { _primary_key_mgr->update_index_cache_size(entry); }, [] { return false; }));
    EXPECT_EQ(charge, tracker->consumption());
    EXPECT_EQ(inventory, sst_inventory(metadata->id()));
    PersistentIndexSstableMetaPB unchanged;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&unchanged);
    EXPECT_EQ(metadata->sstable_meta().SerializeAsString(), unchanged.SerializeAsString());
    std::array<Slice, 2> keys{Slice("projected_0_0007"), Slice("projected_1_0007")};
    std::array<IndexValue, 2> values;
    ASSERT_OK(index.get(keys.size(), keys.data(), values.data()));
    EXPECT_EQ(IndexValue((uint64_t{5} << 32) | 7), values[0]);
    EXPECT_EQ(IndexValue((uint64_t{11} << 32) | 7), values[1]);
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, pressure_coalesce_preserves_crc_cause_on_compressed_data_budget_decline) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    ConfigResetGuard<bool> tde(&config::enable_transparent_data_encryption, false);
    ConfigResetGuard<bool> checksum(&config::lake_pk_index_sst_verify_checksum, true);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    const auto fileset_id = UniqueId::gen_uid().to_proto();
    IndexValuesWithVerPB versions;
    std::mt19937 rng(290448);
    for (int i = 60000; i > 0; --i) {
        auto* value = versions.add_values();
        value->set_version(i);
        value->set_rssid(7);
        value->set_rowid((rng() & 0xffff) << 8);
    }
    const auto encoded = versions.SerializeAsString();
    ASSERT_GT(encoded.size(), 256 * 1024);
    std::string first_file;
    for (int i = 0; i < 8; ++i) {
        KeyValueMerger writer("", 0, false, _tablet_mgr.get(), metadata->id(), false);
        const auto key = fmt::format("crc_{:04d}", i);
        ASSERT_OK(writer.append_encoded(key, encoded));
        ASSIGN_OR_ABORT(auto outputs, writer.finish());
        ASSERT_EQ(1, outputs.size());
        auto* pb = metadata->mutable_sstable_meta()->add_sstables();
        pb->set_filename(outputs[0].filename);
        pb->set_filesize(outputs[0].filesize);
        pb->set_max_rss_rowid((uint64_t{7} << 32) | UINT32_MAX);
        pb->mutable_fileset_id()->CopyFrom(fileset_id);
        pb->mutable_range()->set_start_key(key);
        pb->mutable_range()->set_end_key(key);
        if (i == 0) first_file = _tablet_mgr->sst_location(metadata->id(), pb->filename());
    }
    // Corrupt only the CRC trailer of the large, otherwise legal Snappy data block.
    // Its valid index remains readable, and budget decline must still drain/check the CRC.
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(first_file));
    char footer_bytes[sstable::Footer::kEncodedLength];
    const auto filesize = metadata->sstable_meta().sstables(0).filesize();
    ASSERT_OK(rf->read_at_fully(filesize - sizeof(footer_bytes), footer_bytes, sizeof(footer_bytes)));
    Slice footer_input(footer_bytes, sizeof(footer_bytes));
    sstable::Footer footer;
    ASSERT_OK(footer.DecodeFrom(&footer_input));
    sstable::BlockContents contents;
    ASSERT_OK(sstable::ReadBlock(rf.get(), {}, footer.index_handle(), &contents));
    sstable::Block index_block(contents);
    std::unique_ptr<sstable::Iterator> index_iter(index_block.NewIterator(sstable::BytewiseComparator()));
    index_iter->SeekToFirst();
    ASSERT_TRUE(index_iter->Valid());
    sstable::BlockHandle data_handle;
    auto handle_input = index_iter->value();
    ASSERT_OK(data_handle.DecodeFrom(&handle_input));
    sstable::BlockReadMemory memory;
    ASSERT_OK(sstable::ProbeBlockMemory(rf.get(), data_handle, &memory));
    ASSERT_EQ(sstable::kSnappyCompression, memory.compression);
    ASSERT_GT(memory.encoded, 65536);
    ASSERT_GT(memory.decoded, 256 * 1024);
    rf.reset();
    std::fstream corrupt(first_file, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(corrupt.good());
    const auto crc_offset = static_cast<std::streamoff>(data_handle.offset() + data_handle.size() + 1);
    corrupt.seekg(crc_offset);
    char byte;
    corrupt.read(&byte, 1);
    ASSERT_TRUE(corrupt.good());
    byte ^= 1;
    corrupt.seekp(crc_offset);
    corrupt.write(&byte, 1);
    corrupt.close();
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(32 * 1024 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto cached = tracker->consumption();
    MemTracker inflight(-1, "retained-inflight-work", tracker);
    inflight.consume(tracker->limit() - cached - 8 * 1024 * 1024 - 128 * 1024);
    DeferOp release_inflight([&] { inflight.release(inflight.consumption()); });
    const auto before = tracker->consumption();
    EXPECT_LT(tracker->limit() - before - 8 * 1024 * 1024, memory.decoded);
    const auto errors = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    const auto status = index._coalesce_pressure_filesets(
            8 * 1024 * 1024, [&] { _primary_key_mgr->update_index_cache_size(entry); }, [] { return false; });
    EXPECT_TRUE(status.is_corruption()) << status;
    EXPECT_EQ(errors + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    EXPECT_EQ(before, tracker->consumption());
    PersistentIndexSstableMetaPB unchanged;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&unchanged);
    EXPECT_EQ(metadata->sstable_meta().SerializeAsString(), unchanged.SerializeAsString());
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, pressure_coalesce_scratch_decline_keeps_the_original_fileset) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    const auto fileset_id = UniqueId::gen_uid().to_proto();
    IndexValuesWithVerPB versions;
    for (int i = 6000; i > 0; --i) {
        auto* value = versions.add_values();
        value->set_version(i);
        value->set_rssid(2);
        value->set_rowid(i);
    }
    const auto encoded = versions.SerializeAsString();
    ASSERT_GT(encoded.size(), 32768);
    std::vector<std::string> names;
    for (int i = 0; i < 8; ++i) {
        KeyValueMerger writer("", 0, false, _tablet_mgr.get(), metadata->id(), false);
        const auto key = fmt::format("key_{:04d}", i);
        ASSERT_OK(writer.append_encoded(key, encoded));
        ASSIGN_OR_ABORT(auto outputs, writer.finish());
        ASSERT_EQ(1, outputs.size());
        auto* pb = metadata->mutable_sstable_meta()->add_sstables();
        pb->set_filename(outputs[0].filename);
        pb->set_filesize(outputs[0].filesize);
        pb->set_encryption_meta(outputs[0].encryption_meta);
        pb->set_max_rss_rowid((uint64_t{2} << 32) | 6000);
        pb->mutable_fileset_id()->CopyFrom(fileset_id);
        pb->mutable_range()->set_start_key(key);
        pb->mutable_range()->set_end_key(key);
        names.push_back(pb->filename());
    }
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(8 * 1024 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    _primary_key_mgr->update_index_cache_size(entry);
    MemTracker inflight(-1, "retained-inflight-work", tracker);
    inflight.consume(tracker->limit() - tracker->consumption() - 1024 * 1024 - 128 * 1024);
    DeferOp release_inflight([&] { inflight.release(inflight.consumption()); });
    const auto before = tracker->consumption();
    ASSERT_OK(index._coalesce_pressure_filesets(1024 * 1024, {}, [] { return false; }));
    EXPECT_EQ(before, tracker->consumption());
    PersistentIndexSstableMetaPB unchanged;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&unchanged);
    ASSERT_EQ(names.size(), unchanged.sstables_size());
    for (size_t i = 0; i < names.size(); ++i) EXPECT_EQ(names[i], unchanged.sstables(i).filename());
    Slice key("key_0000");
    IndexValue found;
    ASSERT_OK(index.get(1, &key, &found));
    EXPECT_EQ(IndexValue((uint64_t{2} << 32) | 6000), found);
    _primary_key_mgr->block_cache()->update_memory_usage();
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, manager_prepare_refunds_tail_readers_before_quota_and_repeated_mutations) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    const std::string filename = "large-unchanged-prepare.sst";
    auto fixture = make_large_index_fixture(_tablet_mgr->sst_location(_tablet_metadata->id(), filename), filename);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(fixture.pb);
    const auto tail_id = UniqueId::gen_uid().to_proto();
    for (int i = 0; i < 48; ++i) {
        auto key = fmt::format("z_tail_{:04d}_{}", i, std::string(180, 'x'));
        auto* pb = append_all_key_sstable(metadata.get(), {{key, 1, IndexValue(i)}}, uint64_t{101} << 32);
        ASSERT_NE(nullptr, pb);
        pb->mutable_fileset_id()->CopyFrom(tail_id);
    }
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(fixture.minimum_bytes + 384 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    auto& index = entry->value();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    const auto context = _primary_key_mgr->sst_read_state_memory_context();
    for (size_t i = 1; i < index._sstable_filesets.size(); ++i) {
        Status status;
        index._sstable_filesets[i]->for_each_sstable([&](const auto& sst) {
            sst.set_read_state_cacheable(true, context);
            if (status.ok()) status = sst.reopen_read_state();
        });
        ASSERT_OK(status);
    }
    // Simulate an existing MemTable from before enabling bounded mutation callbacks.
    // It fits update, but retaining it while prepaying the merge workspace does not.
    size_t pending_count = 0;
    while (index.memory_stats().memtable_bytes < static_cast<size_t>(tracker->limit() / 2)) {
        std::vector<std::string> strings;
        std::vector<Slice> pending_keys;
        std::vector<IndexValue> pending_values;
        for (int i = 0; i < 128; ++i) {
            strings.push_back(fmt::format("p_pending_{:06d}_{}", pending_count + i, std::string(180, 'p')));
            pending_values.emplace_back((uint64_t{200} << 32) | (pending_count + i));
        }
        for (const auto& key : strings) pending_keys.emplace_back(key);
        ASSERT_OK(index.insert(pending_keys.size(), pending_keys.data(), pending_values.data(), 2));
        pending_count += pending_keys.size();
    }
    _primary_key_mgr->update_index_cache_size(entry);
    ASSERT_LE(tracker->peak_consumption(), tracker->limit());
    EXPECT_GT(index.memory_stats().overhead_bytes, 48 * 4096);
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    ASSIGN_OR_ABORT(entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    ASSERT_GT(index.publish_workspace_budget(), 32768);
    PersistentIndexSstableMetaPB after;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&after);
    ASSERT_EQ(3, after.sstables_size());
    EXPECT_EQ(filename, after.sstables(0).filename());
    const auto pending_string = fmt::format("p_pending_{:06d}_{}", 0, std::string(180, 'p'));
    Slice pending_key(pending_string);
    IndexValue pending_value;
    ASSERT_OK(index.get(1, &pending_key, &pending_value));
    EXPECT_EQ(IndexValue(uint64_t{200} << 32), pending_value);
    std::array<Slice, 8> keys;
    std::array<IndexValue, 8> expected, values, old;
    for (size_t i = 0; i < keys.size(); ++i) {
        keys[i] = fixture.keys[i];
        expected[i] = IndexValue(i);
    }
    for (int epoch = 0; epoch < 24; ++epoch) {
        index.set_publish_version(epoch + 2);
        for (size_t i = 0; i < keys.size(); ++i) values[i] = IndexValue((uint64_t{102} << 32) | (epoch * 8 + i));
        ASSERT_OK(index.upsert(keys.size(), keys.data(), values.data(), old.data()));
        EXPECT_EQ(expected, old);
        expected = values;
        if (epoch % 3 == 0) {
            ASSERT_OK(index.erase(keys.size(), keys.data(), old.data(), epoch + 2));
            EXPECT_EQ(expected, old);
            expected.fill(IndexValue(NullIndexValue));
        }
        ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
        _primary_key_mgr->update_index_cache_size(entry);
        ASSERT_OK(index.prepare_sstable_read_state(
                0, true, [&] { _primary_key_mgr->update_index_cache_size(entry); }, [] { return false; }));
        EXPECT_GT(index.publish_workspace_budget(), 4096);
        ASSERT_OK(index.get(keys.size(), keys.data(), old.data()));
        EXPECT_EQ(expected, old);
        ASSERT_LE(tracker->peak_consumption(), tracker->limit());
    }
}

TEST_F(LakePersistentIndexTest, manager_growth_reclaims_four_tablet_memtables_with_preloaded_input) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    ConfigResetGuard<int32_t> timeout(&config::lake_pk_index_publish_wait_timeout_ms, 2000);
    const std::string filename = "mid-sized-old-index.sst";
    const auto fixture =
            make_large_index_fixture(_tablet_mgr->sst_location(_tablet_metadata->id(), filename), filename);
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(fixture.minimum_bytes * 2 + 256 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    ASSERT_LT(fixture.minimum_bytes, tracker->limit() / 2);
    // Retain a real eagerly loaded PK column through the manager's update-state cache,
    // rather than treating the rowset input as free space when admitting a different owner.
    auto input_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    input_metadata->set_id(next_id());
    auto input_schema = make_varchar_pk_schema();
    input_schema->to_schema_pb(input_metadata->mutable_schema());
    input_metadata->mutable_schema()->mutable_column(0)->set_length(512);
    input_schema = std::make_shared<TabletSchema>(input_metadata->schema());
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*input_metadata));
    std::vector<std::string> input_keys;
    append_cold_rowset(input_metadata.get(), 0, 1024, 3, &input_keys, 256);
    TxnLogPB_OpWrite input_op;
    input_op.mutable_rowset()->CopyFrom(input_metadata->rowsets(0));
    Tablet input_tablet(_tablet_mgr.get(), input_metadata->id());
    RssidFileInfoContainer input_container;
    RowsetUpdateStateParams input_params{.op_write = input_op,
                                         .tablet_schema = input_schema,
                                         .metadata = input_metadata,
                                         .tablet = &input_tablet,
                                         .container = input_container};
    auto* input_entry = _primary_key_mgr->_update_state_cache.get_or_create("mid-sized-sst-preload");
    DeferOp release_input([&] { _primary_key_mgr->_update_state_cache.remove(input_entry); });
    auto& input = input_entry->value();
    input.init(input_params);
    ASSERT_OK(input.prepare(input_params));
    ASSERT_OK(input.load_segment(0, input_params, 1, false, false));
    ASSERT_NE(nullptr, input.upserts(0));
    const size_t input_bytes = input.memory_usage();
    ASSERT_GT(input_bytes, 256 * 1024);
    _primary_key_mgr->_update_state_cache.update_object_size(input_entry, input_bytes);
    // Three other cached tablets each retain a live MemTable. Their cache pins are idle,
    // allowing the pressure sweep to reclaim them, while the input above stays pinned.
    for (int tablet_no = 0; tablet_no < 3; ++tablet_no) {
        auto idle_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
        idle_metadata->set_id(next_id());
        auto* idle = _primary_key_mgr->index_cache().get_or_create(idle_metadata->id());
        auto idle_guard = idle->value().fetch_guard();
        ASSERT_OK(idle->value().lake_load(_tablet_mgr.get(), idle_metadata, 1, nullptr));
        for (int row = 0; row < 768; ++row) {
            const std::string bytes = fmt::format("idle_{:08d}_{}", row, std::string(256, 'i'));
            Slice key(bytes);
            IndexValue value(row + 1);
            ASSERT_OK(idle->value().insert(1, &key, &value, 1));
        }
        _primary_key_mgr->update_index_cache_size(idle);
        idle_guard.reset();
        _primary_key_mgr->release_primary_index_cache(idle);
    }
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(fixture.pb);
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    auto guard = entry->value().fetch_guard();
    auto& index = entry->value();
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    // Reproduce a warm owner with both inactive and active trees. A mutation alone fits;
    // the same mutation followed by opening this (less than half-budget) SST does not.
    const size_t pending_target = tracker->limit() - fixture.minimum_bytes - input_bytes - 65536;
    size_t pending_rows = 0;
    bool handed_off = false;
    while (index.memory_stats().memtable_bytes < pending_target) {
        std::vector<std::string> strings;
        std::vector<Slice> keys;
        std::vector<IndexValue> values;
        for (int i = 0; i < 128; ++i) {
            strings.push_back(fmt::format("z_pending_{:08d}_{}", pending_rows + i, std::string(256, 'p')));
            values.emplace_back((uint64_t{200} << 32) | (pending_rows + i));
        }
        for (const auto& key : strings) keys.emplace_back(key);
        ASSERT_OK(index.insert(keys.size(), keys.data(), values.data(), 2));
        pending_rows += keys.size();
        if (!handed_off && index.memory_stats().memtable_bytes >= pending_target / 2) {
            ASSERT_OK(index.flush_memtable(true));
            handed_off = true;
        }
    }
    ASSERT_TRUE(handed_off);
    ASSERT_FALSE(index._inactive_memtables.empty());
    _primary_key_mgr->update_index_cache_size(entry);
    EXPECT_EQ(4, _primary_key_mgr->index_cache().object_size());
    ASSERT_LE(tracker->peak_consumption(), tracker->limit());
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    ASSIGN_OR_ABORT(entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    EXPECT_EQ(0, index.publish_workspace_budget());
    const auto pending_before = index.memory_stats().memtable_bytes;
    auto* sync = SyncPoint::GetInstance();
    size_t owner_flushes = 0;
    sync->SetCallBack("PrimaryKeyManager::_reserve_index_growth:after_flush", [&](void*) { ++owner_flushes; });
    std::promise<void> release_sibling, sibling_ready;
    auto release_signal = release_sibling.get_future().share();
    auto ready_signal = sibling_ready.get_future();
    std::future<void> competitor;
    bool released_sibling = false;
    auto drain_sibling = [&] {
        if (!released_sibling) {
            release_sibling.set_value();
            released_sibling = true;
        }
        if (competitor.valid()) competitor.get();
    };
    DeferOp drain_competitor(drain_sibling);
    size_t credit_checks = 0;
    sync->SetCallBack("PrimaryKeyManager::_reserve_index_growth:after_read_credit_reserved", [&](void*) {
        if (++credit_checks != 1) return;
        EXPECT_NE(nullptr, index._publish_read_credit);
        competitor = std::async(std::launch::async, [&] {
            auto* sibling = _primary_key_mgr->compaction_state_mem_tracker();
            const auto headroom = tracker->limit() - tracker->consumption();
            EXPECT_GT(headroom, 0);
            const auto remaining = std::max<int64_t>(0, headroom);
            auto* denied = sibling->try_consume(remaining);
            EXPECT_EQ(nullptr, denied);
            sibling_ready.set_value();
            release_signal.wait();
            if (!denied) sibling->release(remaining);
        });
        EXPECT_EQ(std::future_status::ready, ready_signal.wait_for(std::chrono::seconds(2)));
    });
    sync->EnableProcessing();
    DeferOp clear_sync([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::array<Slice, 128> keys;
    std::array<IndexValue, 128> values, old, expected;
    for (size_t i = 0; i < keys.size(); ++i) {
        keys[i] = fixture.keys[i];
        expected[i] = IndexValue(i);
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (int epoch = 0; epoch < 8; ++epoch) {
        index.set_publish_version(epoch + 2);
        for (size_t i = 0; i < keys.size(); ++i) values[i] = IndexValue((uint64_t{201} << 32) | (epoch * 128 + i));
        ASSERT_OK(index.upsert(keys.size(), keys.data(), values.data(), old.data()));
        if (epoch == 0) drain_sibling();
        EXPECT_EQ(expected, old);
        expected = values;
        if (epoch % 3 == 0) {
            ASSERT_OK(index.erase(keys.size(), keys.data(), old.data(), epoch + 2));
            EXPECT_EQ(expected, old);
            expected.fill(IndexValue(NullIndexValue));
        }
        ASSERT_LE(tracker->peak_consumption(), tracker->limit());
        ASSERT_LT(std::chrono::steady_clock::now(), deadline);
    }
    EXPECT_GT(owner_flushes, 0);
    EXPECT_LT(owner_flushes, 8);
    EXPECT_TRUE(index._publish_skip_optional_block_cache);
    EXPECT_LT(index.memory_stats().memtable_bytes, pending_before);
    ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
    _primary_key_mgr->update_index_cache_size(entry);
    ASSERT_OK(index.get(keys.size(), keys.data(), old.data()));
    EXPECT_EQ(expected, old);
    const std::string pending_string = fmt::format("z_pending_{:08d}_{}", 0, std::string(256, 'p'));
    Slice pending_key(pending_string);
    IndexValue pending_value;
    ASSERT_OK(index.get(1, &pending_key, &pending_value));
    EXPECT_EQ(IndexValue(uint64_t{200} << 32), pending_value);
    PersistentIndexSstableMetaPB after;
    for (const auto& fileset : index._sstable_filesets) fileset->get_all_sstable_pbs(&after);
    ASSERT_GT(after.sstables_size(), 0);
    EXPECT_EQ(filename, after.sstables(0).filename());
    EXPECT_NE(nullptr, input.upserts(0));
    EXPECT_EQ(input_bytes, input.memory_usage());
    EXPECT_EQ(0, _primary_key_mgr->sst_read_state_memory_context()->active_read_credits.load());
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, manager_growth_keeps_resident_sst_when_read_headroom_fits) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    const std::string filename = "resident-growth-headroom.sst";
    const auto fixture =
            make_large_index_fixture(_tablet_mgr->sst_location(_tablet_metadata->id(), filename), filename);
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(fixture.minimum_bytes * 8);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(fixture.pb);
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    Slice key(fixture.keys[0]);
    IndexValue value, old, expected(0);
    ASSERT_OK(index.get(1, &key, &old));
    ASSERT_EQ(expected, old);
    ASSERT_OK(index.prepare_sstable_read_state(tracker->limit(), true,
                                               [&] { _primary_key_mgr->update_index_cache_size(entry); }));
    const auto resident_bytes = index.cached_sst_bytes();
    ASSERT_GT(resident_bytes, 0);
    ASSERT_EQ(0, index.publish_workspace_budget());
    const auto flushes = index._publish_sst_flush_count;
    for (int epoch = 0; epoch < 8; ++epoch) {
        value = IndexValue(10000 + epoch);
        ASSERT_OK(index.upsert(1, &key, &value, &old));
        EXPECT_EQ(expected, old);
        expected = value;
    }
    EXPECT_EQ(flushes, index._publish_sst_flush_count);
    EXPECT_FALSE(index._publish_skip_optional_block_cache);
    EXPECT_EQ(resident_bytes, index.cached_sst_bytes());
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, test_large_old_index_keeps_mutations_bounded_and_reclaims_idle_data_blocks) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    const std::string filename = "large-old-index.sst";
    auto fixture = make_large_index_fixture(_tablet_mgr->sst_location(_tablet_metadata->id(), filename), filename);
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(fixture.minimum_bytes + 256 * 1024);
    DeferOp restore([&] { tracker->set_limit(old_limit); });
    ASSERT_GT(fixture.bulk_bytes, tracker->limit());
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(fixture.pb);
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
    ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard));
    DeferOp release([&] {
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    });
    auto& index = entry->value();
    ASSERT_GT(index.publish_workspace_budget(), 0);
    EXPECT_LT(index.publish_input_chunk_rows(TabletSchema(metadata->schema())), config::vector_chunk_size);
    auto* cache = _primary_key_mgr->block_cache();
    auto handle = cache->cache()->insert("idle-pressure", new char[256 * 1024], 256 * 1024,
                                         [](const CacheKey&, void* value) { delete[] static_cast<char*>(value); });
    cache->cache()->release(handle);
    cache->update_memory_usage();
    MemTracker input(-1, "bounded-pk-input", tracker);
    input.consume(index.publish_workspace_budget());
    DeferOp release_input([&] { input.release(input.consumption()); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    for (size_t i = 0; i < 400; ++i) {
        Slice key(fixture.keys[i]);
        IndexValue value(10000 + i), old;
        ASSERT_OK(index.upsert(1, &key, &value, &old));
        ASSERT_EQ(IndexValue(i), old);
        ASSERT_LE(std::chrono::steady_clock::now(), deadline);
        ASSERT_LE(tracker->peak_consumption(), tracker->limit());
    }
    // Later input batches revisit keys written by earlier batches, then DELETE and reinsert
    // across forced flushes. Check old rowids and the selector's untouched positions each time.
    std::array<Slice, 3> repeated{Slice(fixture.keys[0]), Slice(fixture.keys[17]), Slice(fixture.keys[31])};
    std::array<IndexValue, 3> old;
    ASSERT_OK(index.erase(repeated.size(), repeated.data(), old.data(), 100));
    EXPECT_EQ((std::array<IndexValue, 3>{IndexValue(10000), IndexValue(10017), IndexValue(10031)}), old);
    ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
    std::array<IndexValue, 3> replacements{IndexValue(20000), IndexValue(20017), IndexValue(20031)};
    ASSERT_OK(index.upsert(repeated.size(), repeated.data(), replacements.data(), old.data()));
    for (const auto& value : old) EXPECT_EQ(IndexValue(NullIndexValue), value);
    std::array<IndexValue, 3> selected{IndexValue(30000), IndexValue(30017), IndexValue(30031)};
    ASSERT_OK(index.replace(repeated.size(), repeated.data(), selected.data(), std::vector<uint32_t>{0, 2}));
    ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
    ASSERT_OK(index.get(repeated.size(), repeated.data(), old.data()));
    EXPECT_EQ((std::array<IndexValue, 3>{selected[0], replacements[1], selected[2]}), old);
    Slice tail(fixture.keys.back());
    IndexValue value;
    ASSERT_OK(index.get(1, &tail, &value));
    EXPECT_EQ(IndexValue(8191), value);
    auto idle = cache->cache()->lookup("idle-pressure");
    EXPECT_EQ(nullptr, idle);
    if (idle) cache->cache()->release(idle);
    EXPECT_LE(index.memory_stats().memtable_bytes, index.publish_workspace_budget() + 4096);
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

// outcome: 0 = successful handoff; 1 = bounded wait timeout; 2 = shutdown while waiting.
void LakePersistentIndexTest::check_manager_prepare_pressure_flush(int outcome) {
    ConfigResetGuard<bool> partial(&config::lake_pk_index_partial_eviction, true);
    ConfigResetGuard<int32_t> timeout(&config::lake_pk_index_publish_wait_timeout_ms, outcome == 1 ? 50 : 2000);
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 3);
    const std::string filename = "prepare-existing-large.sst";
    auto fixture = make_large_index_fixture(_tablet_mgr->sst_location(_tablet_metadata->id(), filename), filename);
    auto metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
    metadata->mutable_sstable_meta()->add_sstables()->CopyFrom(fixture.pb);
    auto* tracker = _primary_key_mgr->mem_tracker();
    const auto old_limit = tracker->limit();
    tracker->set_limit(fixture.minimum_bytes + 512 * 1024);
    DeferOp restore_limit([&] { tracker->set_limit(old_limit); });
    auto* entry = _primary_key_mgr->index_cache().get_or_create(metadata->id());
    DeferOp release([&] { _primary_key_mgr->release_primary_index_cache(entry); });
    auto guard = entry->value().fetch_guard();
    auto& index = entry->value();
    // Load without the manager's prepare phase, representing a cached index from before the
    // mandatory old SST's working-set requirement was known. Its MemTables are still live.
    ASSERT_OK(index.lake_load(_tablet_mgr.get(), metadata, 1, nullptr));
    const auto empty_memtable_bytes = index.memory_stats().memtable_bytes;
    std::atomic<bool> hold{true}, started{false};
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void*) {
        started.store(true);
        while (hold.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    sync->EnableProcessing();
    DeferOp drain([&] {
        hold.store(false);
        (void)index.sync_flush_all_memtables(10'000'000, [] { return false; });
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::vector<Slice> keys;
    std::vector<IndexValue> values;
    for (size_t i = 0; i < 1000; ++i) {
        keys.emplace_back(fixture.keys[i]);
        values.emplace_back(10000 + i);
    }
    ASSERT_OK(index.insert(keys.size(), keys.data(), values.data(), 2));
    ASSERT_OK(index.flush_memtable(true));
    const auto start_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!started.load() && std::chrono::steady_clock::now() < start_deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(started.load());
    // A second active tree must also be handed off; both sets of old values must survive.
    Slice active_key(fixture.keys[2000]);
    IndexValue active_value(12000);
    ASSERT_OK(index.insert(1, &active_key, &active_value, 2));
    _primary_key_mgr->update_index_cache_size(entry);
    const auto old_charge = entry->size();
    ASSERT_GT(index.memory_stats().memtable_bytes, 256 * 1024);
    guard.reset();

    std::atomic<bool> cancel{false};
    sync->SetCallBack("PrimaryKeyManager::prepare_primary_index:cancelled",
                      [&](void* arg) { *static_cast<bool*>(arg) = cancel.load(); });
    int checked_charge = 0;
    sync->SetCallBack("LakePersistentIndex::prepare_sstable_read_state:before_reopen", [&](void*) {
        ++checked_charge;
        EXPECT_EQ(index.memory_usage() + index.cached_metadata_bytes(), entry->size());
        EXPECT_LT(entry->size(), old_charge);
        EXPECT_EQ(empty_memtable_bytes, index.memory_stats().memtable_bytes);
    });
    std::thread unblock([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        if (outcome == 0) hold.store(false);
        if (outcome == 2) cancel.store(true);
    });
    DeferOp join([&] { unblock.join(); });
    Tablet tablet(_tablet_mgr.get(), metadata->id());
    MetaFileBuilder builder(tablet, metadata);
    const auto begin = std::chrono::steady_clock::now();
    auto prepared = _primary_key_mgr->prepare_primary_index(metadata, &builder, 1, 2, guard);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    if (outcome != 0) {
        ASSERT_FALSE(prepared.ok());
        EXPECT_TRUE(outcome == 1 ? prepared.status().is_time_out() : prepared.status().is_cancelled())
                << prepared.status();
        EXPECT_LT(elapsed, std::chrono::milliseconds(500));
        EXPECT_EQ(0, checked_charge);
        // The retained test pin permits safe draining after manager failure removes its cache pin.
        hold.store(false);
        ASSERT_OK(index.sync_flush_all_memtables(2'000'000, [] { return false; }));
    } else {
        ASSERT_OK(prepared.status());
        EXPECT_EQ(entry, prepared.value());
        EXPECT_EQ(1, checked_charge);
        // Includes both the inactive and active handoffs appended during prepare.
        auto committed = std::make_shared<TabletMetadata>(*metadata);
        MetaFileBuilder commit_builder(tablet, committed);
        ASSERT_OK(index.commit(&commit_builder));
        EXPECT_EQ(3, committed->sstable_meta().sstables_size());
        _primary_key_mgr->release_primary_index_cache(prepared.value());
    }
    cancel.store(false);
    sync->ClearCallBack("LakePersistentIndex::prepare_sstable_read_state:before_reopen");
    for (size_t i : {size_t{0}, size_t{999}, size_t{2000}, size_t{8191}}) {
        Slice key(fixture.keys[i]);
        IndexValue found;
        ASSERT_OK(index.get(1, &key, &found));
        EXPECT_EQ(IndexValue(i < 1000 ? 10000 + i : i == 2000 ? 12000 : i), found);
    }
    EXPECT_LE(tracker->peak_consumption(), tracker->limit());
}

TEST_F(LakePersistentIndexTest, test_manager_prepare_flush_refreshes_charge_and_includes_new_ssts) {
    check_manager_prepare_pressure_flush(0);
}

TEST_F(LakePersistentIndexTest, test_manager_prepare_flush_honors_short_wait_timeout) {
    check_manager_prepare_pressure_flush(1);
}

TEST_F(LakePersistentIndexTest, test_manager_prepare_flush_cancels_pending_inactive) {
    check_manager_prepare_pressure_flush(2);
}

TEST_F(LakePersistentIndexTest, test_partial_eviction_reserves_objects_when_reopening_one_sstable) {
    check_read_state_reservation_demand(false);
}

TEST_F(LakePersistentIndexTest, test_partial_eviction_caches_one_previously_deferred_sstable) {
    check_read_state_reservation_demand(true);
}

TEST_F(LakePersistentIndexTest, test_partial_eviction_sync_resize_pressure_and_rollback) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    DeferOp restore([&] { config::lake_pk_index_partial_eviction = old_partial; });
    config::lake_pk_index_partial_eviction = false;
    std::vector<PersistentIndexSstablePB> ssts;
    for (int file = 0; file < 4; ++file) {
        const std::string filename = fmt::format("fair-share-{}.sst", file);
        phmap::btree_map<std::string, IndexValueWithVer, std::less<>> data;
        for (int i = 0; i < 10000; ++i) {
            data.emplace(fmt::format("sst-key-{:05d}", i), std::make_pair(int64_t{1}, IndexValue(i)));
        }
        ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(_tablet_metadata->id(), filename)));
        uint64_t filesize = 0;
        PersistentIndexSstableRangePB range;
        ASSERT_OK(PersistentIndexSstable::build_sstable(data, wf.get(), &filesize, &range));
        ASSERT_OK(wf->close());
        PersistentIndexSstablePB pb;
        pb.set_filename(filename);
        pb.set_filesize(filesize);
        pb.set_max_rss_rowid((uint64_t{100} + file) << 32);
        ssts.push_back(pb);
    }
    std::vector<TabletMetadataPtr> metadata;
    std::vector<size_t> memtable_bytes;
    for (int i = 0; i < 6; ++i) {
        auto meta = std::make_shared<TabletMetadata>(*_tablet_metadata);
        meta->set_id(next_id());
        for (const auto& pb : ssts) meta->mutable_sstable_meta()->add_sstables()->CopyFrom(pb);
        metadata.push_back(meta);
        auto* entry = _primary_key_mgr->index_cache().get_or_create(meta->id());
        auto guard = entry->value().fetch_guard();
        ASSERT_OK(entry->value().lake_load(_tablet_mgr.get(), meta, 1, nullptr));
        Slice key("memtable-tail");
        IndexValue value(12345);
        ASSERT_OK(entry->value().insert(1, &key, &value, 2));
        _primary_key_mgr->update_index_cache_size(entry);
        memtable_bytes.push_back(entry->value().memory_stats().memtable_bytes);
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    }
    config::lake_pk_index_partial_eviction = true;
    size_t resident = 0;
    size_t fixed = 0;
    for (const auto& meta : metadata) {
        auto* entry = _primary_key_mgr->index_cache().get(meta->id());
        auto guard = entry->value().fetch_guard();
        entry->value().record_cache_access();
        _primary_key_mgr->update_index_cache_size(entry);
        resident += entry->value().cached_sst_bytes();
        fixed += entry->size() - entry->value().cached_sst_bytes();
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    }
    auto& cache = _primary_key_mgr->index_cache();
    const size_t target = fixed + resident * 3 / 4;
    // Set the soft capacity without automatic LRU, then exercise the owner's synchronous resize.
    cache.set_capacity(target * 4 / 3, false);
    auto* pinned = cache.get(metadata[0]->id());
    auto held_guard = pinned->value().fetch_guard();
    _primary_key_mgr->update_index_cache_size(pinned);
    EXPECT_EQ(6, cache.object_size());
    EXPECT_LT(cache.size(), fixed + resident);
    EXPECT_EQ(resident / 6, pinned->value().cached_sst_bytes());
    held_guard.reset();
    _primary_key_mgr->release_primary_index_cache(pinned);
    _primary_key_mgr->evict_cache(85, 75);
    EXPECT_EQ(6, cache.object_size());
    EXPECT_LE(cache.size(), target);
    for (size_t i = 0; i < metadata.size(); ++i) {
        Tablet tablet(_tablet_mgr.get(), metadata[i]->id());
        auto mutable_meta = std::make_shared<TabletMetadata>(*metadata[i]);
        MetaFileBuilder builder(tablet, mutable_meta);
        std::unique_ptr<std::lock_guard<std::shared_timed_mutex>> guard;
        ASSIGN_OR_ABORT(auto* entry, _primary_key_mgr->prepare_primary_index(metadata[i], &builder, 1, 2, guard));
        EXPECT_EQ(memtable_bytes[i], entry->value().memory_stats().memtable_bytes);
        std::array<Slice, 2> keys{Slice("memtable-tail"), Slice("sst-key-00003")};
        std::array<IndexValue, 2> values;
        ASSERT_OK(entry->value().get(2, keys.data(), values.data()));
        EXPECT_EQ(IndexValue(12345), values[0]);
        EXPECT_EQ(IndexValue(3), values[1]);
        _primary_key_mgr->update_index_cache_size(entry);
        EXPECT_EQ(entry->value().memory_usage() + entry->value().cached_metadata_bytes(), entry->size());
        guard.reset();
        _primary_key_mgr->release_primary_index_cache(entry);
    }
    config::lake_pk_index_partial_eviction = false;
    auto* entry = cache.get(metadata[0]->id());
    auto guard = entry->value().fetch_guard();
    ASSERT_OK(entry->value().prepare_sstable_read_state(std::numeric_limits<size_t>::max(), false));
    _primary_key_mgr->update_index_cache_size(entry);
    EXPECT_EQ(entry->value().memory_usage(), entry->size());
    EXPECT_EQ(resident / 6, entry->value().cached_sst_bytes());
    guard.reset();
    _primary_key_mgr->release_primary_index_cache(entry);
}

// memory_stats() splits memory_usage() into memtables, sstable index blocks and bloom filters, and also reports
// the sstable delete vectors that memory_usage() leaves out. refresh_cache_memory_metrics() adds that split to the
// lake_pk_index_cache_* gauges, and destroying the index takes it back out.
TEST_F(LakePersistentIndexTest, test_memory_stats_add_up_to_memory_usage) {
    const int64_t tablet_id = _tablet_metadata->id();
    const std::string filename = "memory-stats.sst";
    constexpr int kNumKeys = 100;

    // An sstable to ingest, carrying a delete vector the way a compaction output does.
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> entries;
    for (int i = 0; i < kNumKeys; i++) {
        entries.emplace(fmt::format("sst-key-{:04d}", i), std::make_pair(int64_t{1}, IndexValue(i)));
    }
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(tablet_id, filename)));
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range;
    ASSERT_OK(PersistentIndexSstable::build_sstable(entries, wf.get(), &filesize, &range));
    ASSERT_OK(wf->close());
    FileMetaPB sst_meta;
    sst_meta.set_name(filename);
    sst_meta.set_size(filesize);
    // Any non-empty page makes the sstable keep the delete vector passed in with it.
    DelvecPagePB delvec_page;
    delvec_page.set_size(1);
    DelVector empty_delvec;
    empty_delvec.set_empty();
    DelVectorPtr delvec;
    empty_delvec.add_dels_as_new_version({1, 3, 5}, /*version=*/1, &delvec);

    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    ASSERT_OK(index->ingest_sst(sst_meta, range, /*rssid=*/100, /*version=*/1, delvec_page, delvec));
    // Plus keys that are still in the memtable.
    std::vector<std::string> mem_keys;
    std::vector<Slice> mem_key_slices;
    std::vector<IndexValue> mem_values;
    mem_keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; i++) {
        mem_keys.emplace_back(fmt::format("mem-key-{:04d}", i));
        mem_key_slices.emplace_back(mem_keys.back());
        mem_values.emplace_back(i);
    }
    ASSERT_OK(index->insert(kNumKeys, mem_key_slices.data(), mem_values.data(), /*version=*/2));

    const auto stats = index->memory_stats();
    EXPECT_GT(stats.memtable_bytes, 0);
    EXPECT_GT(stats.index_block_bytes, 0);
    EXPECT_GT(stats.filter_bytes, 0);
    EXPECT_GT(stats.delvec_bytes, 0);
    EXPECT_EQ(delvec->memory_usage(), stats.delvec_bytes);
    EXPECT_EQ(index->memory_usage(), stats.charged_bytes());

    auto* metrics = StorageMetrics::instance();
    auto gauges = [&]() {
        return std::array<int64_t, 4>{metrics->lake_pk_index_cache_memtable_bytes.value(),
                                      metrics->lake_pk_index_cache_index_block_bytes.value(),
                                      metrics->lake_pk_index_cache_filter_bytes.value(),
                                      metrics->lake_pk_index_cache_delvec_bytes.value()};
    };
    const auto before = gauges();
    EXPECT_EQ(index->memory_usage(), index->refresh_cache_memory_metrics());
    const auto after = gauges();
    EXPECT_EQ(before[0] + static_cast<int64_t>(stats.memtable_bytes), after[0]);
    EXPECT_EQ(before[1] + static_cast<int64_t>(stats.index_block_bytes), after[1]);
    EXPECT_EQ(before[2] + static_cast<int64_t>(stats.filter_bytes), after[2]);
    EXPECT_EQ(before[3] + static_cast<int64_t>(stats.delvec_bytes), after[3]);
    // A second refresh with nothing changed must not count the index twice.
    index->refresh_cache_memory_metrics();
    EXPECT_EQ(after, gauges());

    // Partial eviction frees only the SST read state. The tail must stay usable without a tablet
    // reload, and ingested delete vectors must still filter the SST after it reopens.
    EXPECT_EQ(0, index->evict_sstable_read_state(0));
    const auto freed = index->evict_sstable_read_state(1);
    EXPECT_EQ(stats.index_block_bytes + stats.filter_bytes, freed);
    EXPECT_EQ(0, index->evict_sstable_read_state(1));
    const auto evicted = index->memory_stats();
    EXPECT_EQ(stats.memtable_bytes, evicted.memtable_bytes);
    EXPECT_EQ(stats.delvec_bytes, evicted.delvec_bytes);
    EXPECT_EQ(0, evicted.index_block_bytes);
    EXPECT_EQ(0, evicted.filter_bytes);
    EXPECT_EQ(stats.charged_bytes() - freed, index->refresh_cache_memory_metrics());
    const auto evicted_gauges = gauges();
    EXPECT_EQ(after[0], evicted_gauges[0]);
    EXPECT_EQ(before[1], evicted_gauges[1]);
    EXPECT_EQ(before[2], evicted_gauges[2]);
    EXPECT_EQ(after[3], evicted_gauges[3]);

    // A memtable-only lookup must not reopen the SST.
    IndexValue value(NullIndexValue);
    ASSERT_OK(index->get(1, &mem_key_slices[0], &value));
    EXPECT_EQ(mem_values[0], value);
    EXPECT_EQ(0, index->memory_stats().index_block_bytes);
    std::array<Slice, 2> sst_keys{Slice("sst-key-0001"), Slice("sst-key-0002")};
    std::array<IndexValue, 2> sst_values{IndexValue(NullIndexValue), IndexValue(NullIndexValue)};
    ASSERT_OK(index->get(sst_keys.size(), sst_keys.data(), sst_values.data()));
    EXPECT_EQ(NullIndexValue, sst_values[0].get_value());
    EXPECT_EQ(IndexValue((uint64_t{100} << 32) | 2), sst_values[1]);
    EXPECT_EQ(stats.charged_bytes(), index->refresh_cache_memory_metrics());
    EXPECT_EQ(after, gauges());

    index.reset();
    EXPECT_EQ(before, gauges());
}

// bulk_erase also directly appends an SST. Its tombstone watermark must advance the empty tail memtable
// so a later lower-RSSID live key can commit and cold-reload behind the tombstone.
TEST_F(LakePersistentIndexTest, test_bulk_erase_preserves_lower_tail_watermark) {
    constexpr uint32_t kLiveRssid = 10;
    constexpr uint32_t kDeleteRssid = 100;
    constexpr uint32_t kTailRssid = 50;
    constexpr uint64_t kLiveValue = (static_cast<uint64_t>(kLiveRssid) << 32) | 7;
    constexpr uint64_t kTailValue = static_cast<uint64_t>(kTailRssid) << 32;
    const int64_t tablet_id = _tablet_metadata->id();
    const std::string deleted_key = "bulk-erase-key";
    const std::string tail_key = "bulk-erase-lower-tail";

    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    const Slice deleted_slice(deleted_key);
    const IndexValue live_value(kLiveValue);
    ASSERT_OK(index->insert(1, &deleted_slice, &live_value, /*version=*/1));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    const std::string filename = "bulk-erase-watermark.sst";
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(_tablet_mgr->sst_location(tablet_id, filename)));
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range;
    ASSERT_OK(PersistentIndexSstable::build_tombstone_sstable(&deleted_slice, 1, /*version=*/0, wf.get(), &filesize,
                                                              &range));
    ASSERT_OK(wf->close());
    FileMetaPB tombstone_sst;
    tombstone_sst.set_name(filename);
    tombstone_sst.set_size(filesize);

    std::array<IndexValue, 1> old_values;
    ASSERT_OK(index->bulk_erase(1, &deleted_slice, old_values.data(), kDeleteRssid, tombstone_sst, range,
                                /*version=*/2));
    EXPECT_EQ(IndexValue(kLiveValue), old_values[0]);

    const Slice tail_slice(tail_key);
    const IndexValue tail_value(kTailValue);
    ASSERT_OK(index->insert(1, &tail_slice, &tail_value, /*version=*/3));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    auto committed = std::make_shared<TabletMetadata>();
    committed->CopyFrom(*_tablet_metadata);
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    for (int i = 1; i < committed->sstable_meta().sstables_size(); ++i) {
        EXPECT_LE(committed->sstable_meta().sstables(i - 1).max_rss_rowid(),
                  committed->sstable_meta().sstables(i).max_rss_rowid());
    }

    auto reloaded = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(reloaded->init(committed));
    std::array<Slice, 2> keys = {Slice(deleted_key), Slice(tail_key)};
    std::array<IndexValue, 2> values;
    ASSERT_OK(reloaded->get(keys.size(), keys.data(), values.data()));
    EXPECT_EQ(IndexValue(NullIndexValue), values[0]);
    EXPECT_EQ(IndexValue(kTailValue), values[1]);
}

// A failed memtable flush must not publish metadata or leave its current output behind. The first hook is
// intentionally absent before the implementation; build/footer and table-open cover the later ownership
// points. Every retry starts with a new index because a partially drained index has no reuse contract.
TEST_F(LakePersistentIndexTest, test_memtable_flush_failure_removes_current_output) {
    constexpr uint64_t kValue = (static_cast<uint64_t>(7) << 32) | 3;
    const std::array<const char*, 3> failure_points = {
            "PersistentIndexMemtable::flush:after_create",
            "table_builder_footer_error",
            "PersistentIndexSstable::init:table_open_error",
    };

    auto retry_from_fresh_index = [&](const TabletMetadataPtr& metadata, bool tde, const std::string& key,
                                      std::vector<std::string>* committed_filenames) {
        auto retry = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
        ASSERT_OK(retry->init(metadata));
        const Slice key_slice(key);
        const IndexValue value(kValue);
        ASSERT_OK(retry->insert(1, &key_slice, &value, /*version=*/2));
        ASSERT_OK(retry->sync_flush_all_memtables(10'000'000));

        auto committed = std::make_shared<TabletMetadata>();
        committed->CopyFrom(*metadata);
        Tablet tablet(_tablet_mgr.get(), metadata->id());
        MetaFileBuilder builder(tablet, committed);
        ASSERT_OK(retry->commit(&builder));
        ASSERT_EQ(1, committed->sstable_meta().sstables_size());
        if (tde) {
            EXPECT_FALSE(committed->sstable_meta().sstables(0).encryption_meta().empty());
        }

        retry.reset();
        auto reloaded = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
        ASSERT_OK(reloaded->init(committed));
        IndexValue actual;
        ASSERT_OK(reloaded->get(1, &key_slice, &actual));
        EXPECT_EQ(value, actual);

        for (const auto& sstable : committed->sstable_meta().sstables()) {
            committed_filenames->emplace_back(sstable.filename());
        }
    };

    for (bool tde : {false, true}) {
        ConfigResetGuard<bool> tde_guard(&config::enable_transparent_data_encryption, tde);
        for (const char* failure_point : failure_points) {
            auto metadata = make_varchar_pk_metadata();
            ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
            ASSERT_TRUE(sst_inventory(metadata->id()).empty());
            const std::string key = fmt::format("flush-failure-{}-{}", tde, failure_point);
            const Slice key_slice(key);
            const IndexValue value(kValue);
            const auto before = sst_inventory(metadata->id());
            const std::string metadata_before = metadata->SerializeAsString();
            std::string created_path;

            auto failed = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
            ASSERT_OK(failed->init(metadata));
            ASSERT_OK(failed->insert(1, &key_slice, &value, /*version=*/1));
            if (std::string_view(failure_point) == "PersistentIndexMemtable::flush:after_create") {
                SyncPoint::GetInstance()->SetCallBack(failure_point, [&](void* arg) {
                    auto* result = static_cast<std::pair<const std::string*, Status*>*>(arg);
                    created_path = *result->first;
                    *result->second = Status::InternalError("injected memtable flush failure");
                });
            } else {
                SyncPoint::GetInstance()->SetCallBack(failure_point, [](void* arg) {
                    *(Status*)arg = Status::InternalError("injected memtable flush failure");
                });
            }
            SyncPoint::GetInstance()->EnableProcessing();
            DeferOp disable_injection([&]() {
                SyncPoint::GetInstance()->ClearCallBack(failure_point);
                SyncPoint::GetInstance()->DisableProcessing();
            });

            const Status st = failed->sync_flush_all_memtables(10'000'000);
            EXPECT_TRUE(st.is_internal_error()) << failure_point << ": " << st;
            EXPECT_EQ(metadata_before, metadata->SerializeAsString()) << failure_point;
            EXPECT_THAT(sst_inventory(metadata->id()), testing::ElementsAreArray(before)) << failure_point;
            if (std::string_view(failure_point) == "PersistentIndexMemtable::flush:after_create") {
                EXPECT_TRUE(FileSystem::Default()->path_exists(created_path).is_not_found());
            }

            failed.reset();
            SyncPoint::GetInstance()->ClearCallBack(failure_point);
            SyncPoint::GetInstance()->DisableProcessing();
            const auto failed_outputs = sst_inventory_delta(before, sst_inventory(metadata->id()));
            std::vector<std::string> retry_outputs;
            retry_from_fresh_index(metadata, tde, key, &retry_outputs);
            remove_exact_ssts(metadata->id(), failed_outputs);
            remove_exact_ssts(metadata->id(), retry_outputs);
            EXPECT_THAT(sst_inventory(metadata->id()), testing::ElementsAreArray(before));
        }

        // Make the first output an inactive flush, then synchronously hand it off before arming the second
        // flush's callback. This gives the callback one deterministic current output to fail: callback order
        // cannot decide which logical memtable receives the error.
        auto metadata = make_varchar_pk_metadata();
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
        ASSERT_TRUE(sst_inventory(metadata->id()).empty());
        const std::string first_key = fmt::format("first-inactive-{}", tde);
        const std::string second_key = fmt::format("second-inactive-{}", tde);
        const auto before = sst_inventory(metadata->id());
        const std::string metadata_before = metadata->SerializeAsString();
        auto failed = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
        ASSERT_OK(failed->init(metadata));
        const Slice first_slice(first_key);
        const Slice second_slice(second_key);
        const IndexValue value(kValue);
        ASSERT_OK(failed->insert(1, &first_slice, &value, /*version=*/1));
        ASSERT_OK(failed->flush_memtable(/*force=*/true));
        ASSERT_OK(failed->sync_flush_all_memtables(10'000'000));
        const auto after_first_handoff = sst_inventory(metadata->id());
        const auto first_outputs = sst_inventory_delta(before, after_first_handoff);
        ASSERT_EQ(1, first_outputs.size());

        std::string second_created_path;
        SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void* arg) {
            auto* result = static_cast<std::pair<const std::string*, Status*>*>(arg);
            second_created_path = *result->first;
            *result->second = Status::InternalError("injected memtable flush failure");
        });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp disable_multi_injection([&]() {
            SyncPoint::GetInstance()->ClearCallBack("PersistentIndexMemtable::flush:after_create");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        ASSERT_OK(failed->insert(1, &second_slice, &value, /*version=*/1));
        ASSERT_OK(failed->flush_memtable(/*force=*/true));
        const Status multi_status = failed->sync_flush_all_memtables(10'000'000);
        EXPECT_TRUE(multi_status.is_internal_error()) << multi_status;
        EXPECT_EQ(metadata_before, metadata->SerializeAsString());
        EXPECT_TRUE(FileSystem::Default()->path_exists(_lp->sst_location(metadata->id(), first_outputs[0])).ok());
        EXPECT_TRUE(FileSystem::Default()->path_exists(second_created_path).is_not_found());
        EXPECT_THAT(sst_inventory(metadata->id()), testing::ElementsAreArray(after_first_handoff));

        failed.reset();
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexMemtable::flush:after_create");
        SyncPoint::GetInstance()->DisableProcessing();
        const auto failed_outputs = sst_inventory_delta(before, sst_inventory(metadata->id()));
        std::vector<std::string> retry_outputs;
        retry_from_fresh_index(metadata, tde, second_key, &retry_outputs);
        remove_exact_ssts(metadata->id(), failed_outputs);
        remove_exact_ssts(metadata->id(), retry_outputs);
        EXPECT_THAT(sst_inventory(metadata->id()), testing::ElementsAreArray(before));
    }
}

TEST_F(LakePersistentIndexTest, test_sync_flush_times_out_without_1s_poll) {
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 2);
    ASSERT_NE(RuntimeEnv::GetInstance()->pk_index_memtable_flush_thread_pool(), nullptr);

    auto metadata = make_varchar_pk_metadata();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(index->init(metadata));

    const std::string key = "sync-flush-timeout";
    const Slice key_slice(key);
    const IndexValue value(uint64_t{1} << 32);
    ASSERT_OK(index->insert(1, &key_slice, &value, /*version=*/1));

    std::atomic<bool> hold_flush{true};
    std::atomic<bool> flush_started{false};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void*) {
        flush_started.store(true, std::memory_order_release);
        while (hold_flush.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([&]() {
        hold_flush.store(false, std::memory_order_release);
        (void)index->sync_flush_all_memtables(10'000'000);
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexMemtable::flush:after_create");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    const auto submit_start = std::chrono::steady_clock::now();
    ASSERT_OK(index->flush_memtable(/*force=*/true));
    const auto submit_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - submit_start)
                    .count();
    ASSERT_LT(submit_ms, 100) << "expected async memtable flush, submit took " << submit_ms << "ms";

    const auto started_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!flush_started.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < started_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(flush_started.load(std::memory_order_acquire));

    const auto wait_start = std::chrono::steady_clock::now();
    const Status st = index->sync_flush_all_memtables(150'000);
    const auto wait_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wait_start)
                    .count();
    EXPECT_TRUE(st.is_time_out()) << st;
    EXPECT_LT(wait_ms, 800) << "timeout should honor remaining wait budget, not usleep(1s); took " << wait_ms << "ms";
}

TEST_F(LakePersistentIndexTest, test_pressure_flush_preserves_async_io_timeout_cause) {
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 2);
    auto metadata = make_varchar_pk_metadata();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(index->init(metadata));
    const Slice key("async-io-timeout");
    const IndexValue value(uint64_t{1} << 32);
    ASSERT_OK(index->insert(1, &key, &value, 1));
    const auto original = Status::TimedOut("injected asynchronous flush IO timeout");
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void* arg) {
        *static_cast<std::pair<const std::string*, Status*>*>(arg)->second = original;
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([&] {
        (void)index->sync_flush_all_memtables(10'000'000);
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSERT_OK(index->flush_memtable(true));
    const auto result = index->sync_flush_all_memtables(1'000'000, [] { return false; });
    // RETURN_IF_ERROR adds propagation context; the IO code and cause must stay unchanged.
    EXPECT_EQ(original.code(), result.code());
    EXPECT_EQ(original.message(), result.message());
}

TEST_F(LakePersistentIndexTest, test_pressure_flush_shares_deadline_and_retires_successful_handoffs) {
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 3);
    auto metadata = make_varchar_pk_metadata();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(index->init(metadata));
    std::array<std::string, 2> strings = {"first-flush", "second-flush"};
    std::array<Slice, 2> keys = {Slice(strings[0]), Slice(strings[1])};
    std::array<IndexValue, 2> values = {IndexValue(uint64_t{1} << 32), IndexValue(uint64_t{2} << 32)};
    std::atomic<int> started{0};
    std::array<std::atomic<bool>, 2> hold = {true, true};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void*) {
        const auto id = started.fetch_add(1);
        while (id < hold.size() && hold[id].load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp drain([&] {
        for (auto& flag : hold) flag.store(false);
        (void)index->sync_flush_all_memtables(10'000'000);
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    });
    ASSERT_OK(index->insert(1, &keys[0], &values[0], 1));
    ASSERT_OK(index->flush_memtable(true));
    const auto started_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (started.load() == 0 && std::chrono::steady_clock::now() < started_deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_GT(started.load(), 0);
    ASSERT_OK(index->insert(1, &keys[1], &values[1], 1));
    ASSERT_OK(index->flush_memtable(true));
    std::thread finish_first([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(220));
        hold[0].store(false);
    });
    DeferOp join([&] {
        if (finish_first.joinable()) finish_first.join();
    });
    const auto begin = std::chrono::steady_clock::now();
    const auto status = index->sync_flush_all_memtables(300'000, [] { return false; });
    EXPECT_TRUE(status.is_time_out()) << status;
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::milliseconds(420));
    hold[1].store(false);
    // The first SST was handed off before the second timed out. It must have been retired:
    // waiting for its already-consumed _sstable again would time out on this retry.
    ASSERT_OK(index->sync_flush_all_memtables(2'000'000, [] { return false; }));
    std::array<IndexValue, 2> found;
    ASSERT_OK(index->get(keys.size(), keys.data(), found.data()));
    EXPECT_EQ(values, found);
}

TEST_F(LakePersistentIndexTest, test_sync_flush_wakes_when_async_flush_finishes) {
    ConfigResetGuard<int32_t> memtable_count(&config::pk_index_memtable_max_count, 2);
    ASSERT_NE(RuntimeEnv::GetInstance()->pk_index_memtable_flush_thread_pool(), nullptr);

    auto metadata = make_varchar_pk_metadata();
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*metadata));
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), metadata->id());
    ASSERT_OK(index->init(metadata));

    const std::string key = "sync-flush-wake";
    const Slice key_slice(key);
    const IndexValue value(uint64_t{1} << 32);
    ASSERT_OK(index->insert(1, &key_slice, &value, /*version=*/1));

    std::atomic<bool> hold_flush{true};
    std::atomic<bool> flush_started{false};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexMemtable::flush:after_create", [&](void*) {
        flush_started.store(true, std::memory_order_release);
        while (hold_flush.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([&]() {
        hold_flush.store(false, std::memory_order_release);
        (void)index->sync_flush_all_memtables(10'000'000);
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexMemtable::flush:after_create");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    const auto submit_start = std::chrono::steady_clock::now();
    ASSERT_OK(index->flush_memtable(/*force=*/true));
    const auto submit_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - submit_start)
                    .count();
    ASSERT_LT(submit_ms, 100) << "expected async memtable flush, submit took " << submit_ms << "ms";

    const auto started_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!flush_started.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < started_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(flush_started.load(std::memory_order_acquire));

    std::thread releaser([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        hold_flush.store(false, std::memory_order_release);
    });
    DeferOp join_releaser([&]() {
        hold_flush.store(false, std::memory_order_release);
        if (releaser.joinable()) {
            releaser.join();
        }
    });
    const auto wait_start = std::chrono::steady_clock::now();
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    const auto wait_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wait_start)
                    .count();
    EXPECT_LT(wait_ms, 500) << "sync wait should wake on flush completion, not a 1s poll; took " << wait_ms << "ms";
}

TEST_F(LakePersistentIndexTest, test_replace) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    vector<IndexValue> replace_values;
    const int N = 10000;
    keys.reserve(N);
    key_slices.reserve(N);
    vector<size_t> replace_idxes;
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        values.emplace_back(i * 2);
        replace_values.emplace_back(i * 3);
        replace_idxes.emplace_back(i);
    }

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), false));

    //replace
    std::vector<uint32_t> failed(keys.size());
    Status st = index->try_replace(N, key_slices.data(), replace_values.data(), N, &failed);
    ASSERT_TRUE(st.ok());
    std::vector<IndexValue> new_get_values(keys.size());
    ASSERT_TRUE(index->get(keys.size(), key_slices.data(), new_get_values.data()).ok());
    ASSERT_EQ(keys.size(), new_get_values.size());
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(replace_values[i], new_get_values[i]);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_apply_opcompaction_output_sstable_with_delvec) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    DeferOp restore_partial([&] { config::lake_pk_index_partial_eviction = old_partial; });
    config::lake_pk_index_partial_eviction = false;
    auto saved_l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10; // force a flush so the upsert produces an on-disk sstable
    DeferOp restore_config([&]() { config::l0_max_mem_usage = saved_l0_max_mem_usage; });

    using Key = uint64_t;
    const int kNumKeys = 100;
    const int64_t kVersion = 2;
    const uint32_t kSegmentId = 0;
    auto tablet_id = _tablet_metadata->id();

    // 1. Build an index with a single flushed sstable.
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    std::vector<Key> keys(kNumKeys);
    std::vector<Slice> key_slices(kNumKeys);
    std::vector<IndexValue> values(kNumKeys);
    for (int i = 0; i < kNumKeys; i++) {
        keys[i] = i;
        key_slices[i] = Slice((uint8_t*)(&keys[i]), sizeof(Key));
        values[i] = i * 2;
    }
    index->set_publish_version(1);
    std::vector<IndexValue> old_values(kNumKeys);
    ASSERT_OK(index->upsert(kNumKeys, key_slices.data(), values.data(), old_values.data()));
    ASSERT_OK(index->flush_memtable(true));
    ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000));

    // 2. Commit the sstable metadata, then append a delete vector into the same metadata and finalize.
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->CopyFrom(*_tablet_metadata);
    metadata->set_version(kVersion);
    MetaFileBuilder builder(tablet, metadata);
    ASSERT_OK(index->commit(&builder));
    DelVector delete_vector;
    delete_vector.set_empty();
    std::shared_ptr<DelVector> new_delete_vector;
    std::vector<uint32_t> deleted_row_ids = {2, 6, 10};
    delete_vector.add_dels_as_new_version(deleted_row_ids, kVersion, &new_delete_vector);
    builder.append_delvec(new_delete_vector, kSegmentId);
    ASSERT_OK(builder.finalize(next_id()));

    // 3. Read back the finalized metadata: it now has the sstable plus a delete-vector page.
    ASSIGN_OR_ABORT(auto committed_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, kVersion));
    ASSERT_GT(committed_metadata->sstable_meta().sstables_size(), 0);
    auto delete_vector_entry = committed_metadata->delvec_meta().delvecs().find(kSegmentId);
    ASSERT_TRUE(delete_vector_entry != committed_metadata->delvec_meta().delvecs().end());

    // 4. Build an op_compaction that simulates the parallel-compaction passthrough: the output
    //    sstable is the input sstable carried over together with its embedded delete vector.
    const auto& base_sstable = committed_metadata->sstable_meta().sstables(0);
    TxnLogPB txn_log;
    auto* op_compaction = txn_log.mutable_op_compaction();
    op_compaction->add_input_sstables()->CopyFrom(base_sstable);
    auto* output_sstable = op_compaction->add_output_sstables();
    output_sstable->CopyFrom(base_sstable);
    output_sstable->mutable_delvec()->CopyFrom(delete_vector_entry->second);

    // 5. A fresh index loaded from the committed metadata owns the input fileset.
    auto reloaded_index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(reloaded_index->init(committed_metadata));

    // Before the fix this returned InvalidArgument:
    //   "metadata is null when loading delvec from file".
    config::lake_pk_index_partial_eviction = true;
    EXPECT_GT(reloaded_index->evict_sstable_read_state(std::numeric_limits<size_t>::max()), 0);
    ASSERT_OK(reloaded_index->apply_opcompaction(committed_metadata, txn_log.op_compaction()));
    ASSERT_OK(reloaded_index->prepare_sstable_read_state(0, true));
    std::vector<IndexValue> after(kNumKeys, IndexValue(NullIndexValue));
    ASSERT_OK(reloaded_index->get(kNumKeys, key_slices.data(), after.data()));
    for (int i = 0; i < kNumKeys; ++i) {
        EXPECT_EQ(i == 1 || i == 3 || i == 5 ? IndexValue(NullIndexValue) : values[i], after[i]);
    }
    EXPECT_GT(reloaded_index->memory_stats().delvec_bytes, 0);
    EXPECT_EQ(0, reloaded_index->memory_stats().index_block_bytes);
    EXPECT_EQ(0, _primary_key_mgr->sst_read_state_memory_context()->tracker.consumption());
}

TEST_F(LakePersistentIndexTest, test_range_single_int_pk_end_to_end) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    constexpr int N = 30;

    auto* schema_pb = _tablet_metadata->mutable_schema();
    schema_pb->clear_sort_key_idxes();
    schema_pb->add_sort_key_idxes(0);
    schema_pb->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

    auto tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    auto encode_key = [&](int32_t v) {
        auto chunk = std::make_unique<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col->append_datum(Datum(v));
        chunk->append_column(std::move(col), (SlotId)0);

        MutableColumnPtr pk_column;
        EXPECT_OK(
                PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
    };

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    for (int batch = 0; batch < 3; ++batch) {
        std::vector<std::string> keys;
        std::vector<Slice> key_slices;
        std::vector<IndexValue> values;
        std::vector<IndexValue> upsert_old_values(N);
        keys.reserve(N);
        key_slices.reserve(N);
        values.reserve(N);
        for (int j = 0; j < N; ++j) {
            const int key = 100 + batch * N + j;
            keys.emplace_back(encode_key(key));
            key_slices.emplace_back(keys.back());
            values.emplace_back(key * 10);
        }
        index->set_publish_version(batch);
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10 seconds timeout
    }

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);

    TabletRangePB* range_pb = tablet_metadata_ptr->mutable_range();
    range_pb->Clear();
    range_pb->mutable_lower_bound()->add_values()->CopyFrom(make_int_variant_pb(100));
    range_pb->set_lower_bound_included(true);
    range_pb->mutable_upper_bound()->add_values()->CopyFrom(make_int_variant_pb(200));
    range_pb->set_upper_bound_included(false);

    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    ASSERT_OK(index->commit(&builder));

    auto* sstable_meta = tablet_metadata_ptr->mutable_sstable_meta();
    for (auto& sst_pb : *sstable_meta->mutable_sstables()) {
        sst_pb.set_shared(true);
    }

    auto txn_log = std::make_shared<TxnLogPB>();
    ASSERT_OK(parallel_compact_into_txn_log(tablet_metadata_ptr,
                                            sstables_as_candidate_filesets(tablet_metadata_ptr->sstable_meta()),
                                            /*merge_base_level=*/true, txn_log.get()));
    ASSERT_EQ(1, txn_log->op_compaction().output_sstables_size());

    const auto& out_sst = txn_log->op_compaction().output_sstables(0);
    ASSERT_EQ(encode_key(100), out_sst.range().start_key());
    ASSERT_EQ(encode_key(100 + 3 * N - 1), out_sst.range().end_key());

    ASSERT_OK(index->apply_opcompaction(tablet_metadata_ptr, txn_log->op_compaction()));

    std::vector<std::string> probe_keys = {encode_key(99), encode_key(100), encode_key(150), encode_key(189),
                                           encode_key(199)};
    std::vector<Slice> probe_key_slices;
    probe_key_slices.reserve(probe_keys.size());
    for (const auto& k : probe_keys) {
        probe_key_slices.emplace_back(k);
    }

    std::vector<IndexValue> get_values(probe_keys.size());
    ASSERT_OK(index->get(probe_key_slices.size(), probe_key_slices.data(), get_values.data()));
    ASSERT_EQ(NullIndexValue, get_values[0].get_value());
    ASSERT_EQ(IndexValue(1000), get_values[1]);
    ASSERT_EQ(IndexValue(1500), get_values[2]);
    ASSERT_EQ(IndexValue(1890), get_values[3]);
    ASSERT_EQ(NullIndexValue, get_values[4].get_value());

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_insert_delete) {
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    const int N = 10000;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        values.emplace_back(i * 2);
    }

    // 1. insert
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));
    for (int i = 0; i < N; i++) {
        values[i] = i * 3;
    }
    // 2. upsert
    vector<IndexValue> old_values(N, IndexValue(NullIndexValue));
    ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), old_values.data()));

    // 3. insert delete
    vector<bool> filter(N, false);
    for (int i = 0; i < N; i++) {
        if (i % 2 == 0) {
            filter[i] = true;
        }
    }
    ASSERT_OK(index->replay_erase(N, key_slices.data(), filter, 0, 0));
    // 4. check result
    std::vector<IndexValue> new_get_values(keys.size());
    ASSERT_TRUE(index->get(N, key_slices.data(), new_get_values.data()).ok());
    ASSERT_EQ(N, new_get_values.size());
    for (int i = 0; i < new_get_values.size(); i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(IndexValue(i * 3), new_get_values[i]);
        } else {
            ASSERT_EQ(IndexValue(NullIndexValue), new_get_values[i]);
        }
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_memtable_full) {
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    size_t old_l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 1073741824;
    using Key = uint64_t;
    vector<Key> keys;
    vector<Slice> key_slices;
    vector<IndexValue> values;
    const int N = 10000;
    keys.reserve(N);
    key_slices.reserve(N);
    for (int i = 0; i < N; i++) {
        keys.emplace_back(i);
        key_slices.emplace_back((uint8_t*)(&keys[i]), sizeof(Key));
        values.emplace_back(i * 2);
    }
    ASSERT_OK(index->insert(N, key_slices.data(), values.data(), 0));

    ASSERT_FALSE(index->is_memtable_full());
    config::l0_max_mem_usage = index->memory_usage() + 1;
    ASSERT_FALSE(index->is_memtable_full());
    config::l0_max_mem_usage = index->memory_usage();
    ASSERT_TRUE(index->is_memtable_full());
    config::l0_max_mem_usage = old_l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_tablet_range_single_column_pk) {
    auto schema_pb = create_tablet_schema_pb({{"pk", "INT"}, {"v1", "INT"}}, 1);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);

    TabletRangePB range_pb;
    range_pb.mutable_lower_bound()->add_values()->CopyFrom(make_int_variant_pb(100));
    range_pb.set_lower_bound_included(true);
    range_pb.mutable_upper_bound()->add_values()->CopyFrom(make_int_variant_pb(200));
    range_pb.set_upper_bound_included(false);

    ASSIGN_OR_ABORT(auto sst_seek_range, TabletRangeHelper::create_sst_seek_range_from(range_pb, schema));

    // Encode expected keys
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);

    auto encode_key = [&](int32_t v) {
        auto chunk = std::make_unique<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col->append_datum(Datum(v));
        chunk->append_column(std::move(col), (SlotId)0);

        MutableColumnPtr pk_column;
        EXPECT_OK(
                PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            RawDataVisitor visitor;
            EXPECT_OK(pk_column->accept(&visitor));
            return std::string(reinterpret_cast<const char*>(visitor.result()), pk_column->type_size());
        }
    };

    ASSERT_EQ(sst_seek_range.seek_key, encode_key(100));
    ASSERT_EQ(sst_seek_range.stop_key, encode_key(200));
}

TEST_F(LakePersistentIndexTest, test_tablet_range_multi_column_pk) {
    auto schema_pb = create_tablet_schema_pb({{"pk1", "INT"}, {"pk2", "VARCHAR"}, {"v1", "INT"}}, 2);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);

    TabletRangePB range_pb;
    auto* lower = range_pb.mutable_lower_bound();
    lower->add_values()->CopyFrom(make_int_variant_pb(100));
    lower->add_values()->CopyFrom(make_string_variant_pb("abc"));
    range_pb.set_lower_bound_included(true);

    auto* upper = range_pb.mutable_upper_bound();
    upper->add_values()->CopyFrom(make_int_variant_pb(200));
    upper->add_values()->CopyFrom(make_string_variant_pb("def"));
    range_pb.set_upper_bound_included(false);

    ASSIGN_OR_ABORT(auto sst_seek_range, TabletRangeHelper::create_sst_seek_range_from(range_pb, schema));

    // Encode expected keys
    std::vector<ColumnId> pk_columns = {0, 1};
    auto pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);

    auto encode_key = [&](int32_t v1, const std::string& v2) {
        auto chunk = std::make_unique<Chunk>();
        auto col1 = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col1->append_datum(Datum(v1));
        chunk->append_column(std::move(col1), (SlotId)0);

        auto col2 = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false);
        col2->append_datum(Datum(Slice(v2)));
        chunk->append_column(std::move(col2), (SlotId)1);

        MutableColumnPtr pk_column;
        EXPECT_OK(
                PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            RawDataVisitor visitor;
            EXPECT_OK(pk_column->accept(&visitor));
            return std::string(reinterpret_cast<const char*>(visitor.result()), pk_column->type_size());
        }
    };

    ASSERT_EQ(sst_seek_range.seek_key, encode_key(100, "abc"));
    ASSERT_EQ(sst_seek_range.stop_key, encode_key(200, "def"));
}

TEST_F(LakePersistentIndexTest, test_tablet_range_infinite_bounds) {
    auto schema_pb = create_tablet_schema_pb({{"pk", "INT"}}, 1);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);

    TabletRangePB range_pb;
    // No lower or upper bound

    ASSIGN_OR_ABORT(auto sst_seek_range, TabletRangeHelper::create_sst_seek_range_from(range_pb, schema));

    ASSERT_TRUE(sst_seek_range.seek_key.empty());
    ASSERT_TRUE(sst_seek_range.stop_key.empty());
}

// NULL on non-nullable PK columns is treated as type-minimum (MIN sentinel from FE).
TEST_F(LakePersistentIndexTest, test_tablet_range_null_as_min_on_non_nullable_pk) {
    auto schema_pb = create_tablet_schema_pb({{"pk1", "INT"}, {"pk2", "INT"}, {"v1", "INT"}}, 2);
    auto schema = std::make_shared<const TabletSchema>(schema_pb);

    TabletRangePB range_pb;
    auto* lower = range_pb.mutable_lower_bound();
    lower->add_values()->CopyFrom(make_int_variant_pb(100));
    lower->add_values()->CopyFrom(make_null_variant_pb(TYPE_INT));
    range_pb.set_lower_bound_included(true);

    auto* upper = range_pb.mutable_upper_bound();
    upper->add_values()->CopyFrom(make_int_variant_pb(200));
    upper->add_values()->CopyFrom(make_null_variant_pb(TYPE_INT));
    range_pb.set_upper_bound_included(false);

    ASSIGN_OR_ABORT(auto sst_seek_range, TabletRangeHelper::create_sst_seek_range_from(range_pb, schema));

    // Encode expected: (100, INT_MIN) and (200, INT_MIN).
    std::vector<ColumnId> pk_columns = {0, 1};
    auto pkey_schema = ChunkHelper::convert_schema(schema, pk_columns);

    auto encode_key = [&](int32_t v1, int32_t v2) {
        auto chunk = std::make_unique<Chunk>();
        auto col1 = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col1->append_datum(Datum(v1));
        chunk->append_column(std::move(col1), (SlotId)0);
        auto col2 = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        col2->append_datum(Datum(v2));
        chunk->append_column(std::move(col2), (SlotId)1);

        MutableColumnPtr pk_column;
        EXPECT_OK(
                PrimaryKeyEncoder::create_column(pkey_schema, &pk_column, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get(),
                                  PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            RawDataVisitor visitor;
            EXPECT_OK(pk_column->accept(&visitor));
            return std::string(reinterpret_cast<const char*>(visitor.result()), pk_column->type_size());
        }
    };

    ASSERT_EQ(sst_seek_range.seek_key, encode_key(100, std::numeric_limits<int32_t>::lowest()));
    ASSERT_EQ(sst_seek_range.stop_key, encode_key(200, std::numeric_limits<int32_t>::lowest()));
}

// Helper: build a RowsetMetadataPB with given id, per-segment row counts (segment_metas populated),
// and an optional del file count.
static RowsetMetadataPB make_rowset(uint32_t id, const std::vector<int64_t>& seg_rows, int del_file_cnt = 0,
                                    int64_t del_rows_per_file = 0) {
    RowsetMetadataPB rowset;
    rowset.set_id(id);
    int64_t total_rows = 0;
    for (int64_t r : seg_rows) {
        auto* meta = rowset.add_segment_metas();
        meta->set_filename("seg.dat");
        meta->set_num_rows(r);
        total_rows += r;
    }
    rowset.set_num_rows(total_rows);
    for (int i = 0; i < del_file_cnt; ++i) {
        auto* del = rowset.add_del_files();
        // del_rows_per_file < 0 leaves num_rows unset, simulating del files written before the
        // per-del-file row count existed.
        if (del_rows_per_file >= 0) {
            del->set_num_rows(del_rows_per_file);
        }
    }
    return rowset;
}

// Helper: build a RowsetMetadataPB without segment_metas (proportional fallback path).
static RowsetMetadataPB make_rowset_no_meta(uint32_t id, int seg_cnt, int64_t total_rows) {
    RowsetMetadataPB rowset;
    rowset.set_id(id);
    for (int i = 0; i < seg_cnt; ++i) {
        rowset.add_segment_metas()->set_filename("seg.dat");
    }
    rowset.set_num_rows(total_rows);
    return rowset;
}

TEST_F(LakePersistentIndexTest, test_need_rebuild_counts) {
    TabletMetadataPB metadata;
    PersistentIndexSstableMetaPB sstable_meta;

    // Case 1: no rowsets, no SSTs → {0, 0}
    {
        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 0);
        EXPECT_EQ(row_cnt, 0);
    }

    // Case 2: three rowsets, no SSTs → all segments need rebuild, exact row count via segment_metas.
    // Rowset layout: id=0 (100 rows), id=1 (200 rows), id=2 (150 rows), each with 1 segment.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset(0, {100});
        *metadata.add_rowsets() = make_rowset(1, {200});
        *metadata.add_rowsets() = make_rowset(2, {150});

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 3u);
        EXPECT_EQ(row_cnt, 450);
    }

    // Case 3: SST covers rowsets 0 and 1 (max_rss_rowid has rss_id=2 in high 32 bits).
    // Only rowset 2's segment (rssid=2) needs rebuild.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset(0, {100});
        *metadata.add_rowsets() = make_rowset(1, {200});
        *metadata.add_rowsets() = make_rowset(2, {150});
        auto* sst = sstable_meta.add_sstables();
        sst->set_max_rss_rowid(static_cast<int64_t>(2LL << 32)); // rebuild_rss_id = 2

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 1u);
        EXPECT_EQ(row_cnt, 150);
    }

    // Case 4: proportional fallback when segment_metas is absent.
    // Rowset id=0 has 2 segments and 200 total rows; SST covers segment 0 (rssid=0),
    // segment 1 (rssid=1) needs rebuild → proportional estimate: 200 * 1 / 2 = 100.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset_no_meta(0, 2, 200);
        auto* sst = sstable_meta.add_sstables();
        sst->set_max_rss_rowid(static_cast<int64_t>(1LL << 32)); // rebuild_rss_id = 1

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 1u);
        EXPECT_EQ(row_cnt, 100);
    }

    // Case 5: del files with num_rows absent (pre-upgrade metadata, has_num_rows()==false) are counted
    // in file_cnt but contribute 0 to row_cnt (backward-compatible fallback). Passing a negative to the
    // helper leaves num_rows unset, simulating a del file written before the field existed.
    // Rowset id=0: 1 segment (100 rows) + 2 del files, num_rows unset.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset(0, {100}, /*del_file_cnt=*/2, /*del_rows_per_file=*/-1);

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 3u); // 1 segment + 2 del files
        EXPECT_EQ(row_cnt, 100);
    }

    // Case 6: del files with a recorded num_rows contribute their tombstone count to row_cnt, so a
    // tombstone-heavy workload can cross the rebuild-rows threshold even with few segment rows.
    // Rowset id=0: 1 segment (100 rows) + 3 del files each recording 40 tombstones.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset(0, {100}, /*del_file_cnt=*/3, /*del_rows_per_file=*/40);

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 4u);          // 1 segment + 3 del files
        EXPECT_EQ(row_cnt, 100 + 3 * 40); // 100 segment rows + 120 tombstone rows
    }
}

// Verify that ingest_sst skips SST files that already exist in _sstable_filesets
// (loaded by init from metadata). This prevents duplicate ingest during tablet split
// where the SST was already loaded from shared metadata with correct shared/fileset_id.
TEST_F(LakePersistentIndexTest, test_ingest_sst_skip_duplicate) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    DeferOp restore_partial([&] { config::lake_pk_index_partial_eviction = old_partial; });
    config::lake_pk_index_partial_eviction = false;
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 3;
    const int N = 100;

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    // Write data in multiple batches to create SST files
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        vector<Slice> key_slices;
        vector<IndexValue> values;
        keys.reserve(N);
        key_slices.reserve(N);
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            values.emplace_back(j * 2);
        }
        index->set_publish_version(i);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(10000000));
    }

    // Commit to get sstable_meta
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    ASSERT_OK(index->commit(&builder));

    // Verify we have SST files in metadata
    ASSERT_GT(tablet_metadata_ptr->sstable_meta().sstables_size(), 0);

    // Mark them as shared (simulating tablet split)
    auto* sstable_meta = tablet_metadata_ptr->mutable_sstable_meta();
    for (auto& sst_pb : *sstable_meta->mutable_sstables()) {
        sst_pb.set_shared(true);
    }

    // Get the filename of the first SST
    const std::string sst_filename = tablet_metadata_ptr->sstable_meta().sstables(0).filename();
    int64_t sst_filesize = tablet_metadata_ptr->sstable_meta().sstables(0).filesize();

    // Re-init the index with the updated metadata (shared SSTs)
    auto index2 = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index2->init(tablet_metadata_ptr));

    config::lake_pk_index_partial_eviction = true;
    EXPECT_GT(index2->evict_sstable_read_state(std::numeric_limits<size_t>::max()), 0);

    // Now try to ingest the same SST again
    FileMetaPB sst_meta;
    sst_meta.set_name(sst_filename);
    sst_meta.set_size(sst_filesize);
    sst_meta.set_shared(false); // wrong shared value

    PersistentIndexSstableRangePB sst_range;
    DelvecPagePB delvec_page;
    // ingest_sst should skip because the SST already exists in _sstable_filesets
    ASSERT_OK(index2->ingest_sst(sst_meta, sst_range, 0, 1, delvec_page, nullptr));
    ASSERT_OK(index2->prepare_sstable_read_state(0, true));
    Key lookup_key = 3;
    Slice lookup_slice(reinterpret_cast<const uint8_t*>(&lookup_key), sizeof(Key));
    IndexValue lookup_value(NullIndexValue);
    ASSERT_OK(index2->get(1, &lookup_slice, &lookup_value));
    EXPECT_EQ(IndexValue(6), lookup_value);
    EXPECT_EQ(0, index2->memory_stats().index_block_bytes);
    EXPECT_EQ(0, _primary_key_mgr->sst_read_state_memory_context()->tracker.consumption());

    // Commit and verify: the sstable_meta should have exactly the same SSTs as before,
    // with shared=true preserved (not overwritten by the duplicate ingest)
    auto tablet_metadata_ptr2 = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr2->CopyFrom(*tablet_metadata_ptr);
    MetaFileBuilder builder2(tablet, tablet_metadata_ptr2);
    ASSERT_OK(index2->commit(&builder2));

    // Count SSTs with matching filename - should be exactly 1 (no duplicate)
    int match_count = 0;
    for (const auto& sst : tablet_metadata_ptr2->sstable_meta().sstables()) {
        if (sst.filename() == sst_filename) {
            match_count++;
            EXPECT_TRUE(sst.shared()) << "shared flag should be preserved from init metadata";
        }
    }
    EXPECT_EQ(match_count, 1) << "SST should not be duplicated after ingest_sst";

    config::l0_max_mem_usage = l0_max_mem_usage;
}

// Verify that ingest_sst preserves the sst_meta.shared flag when committing a brand-new
// SST (not already present in _sstable_filesets). This is the cross-publish scenario
// where set_all_data_files_shared has marked sst_meta.shared=true on the txn log and
// the downstream tablet must persist it into sstable_meta so that remove_compacted_sst
// and vacuum continue to see the file as shared.
TEST_F(LakePersistentIndexTest, test_ingest_sst_preserves_shared_flag_for_new_sst) {
    const bool old_partial = config::lake_pk_index_partial_eviction;
    DeferOp restore_partial([&] { config::lake_pk_index_partial_eviction = old_partial; });
    config::lake_pk_index_partial_eviction = false;
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 3;
    const int N = 100;

    auto tablet_id = _tablet_metadata->id();

    // Phase 1: create an SST on disk by writing and flushing on a temporary index.
    std::string sst_filename;
    int64_t sst_filesize = 0;
    PersistentIndexSstableRangePB sst_range;
    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        ASSERT_OK(index->init(_tablet_metadata));
        for (int i = 0; i < M; ++i) {
            vector<Key> keys;
            vector<Slice> key_slices;
            vector<IndexValue> values;
            keys.reserve(N);
            key_slices.reserve(N);
            values.reserve(N);
            for (int j = 0; j < N; j++) {
                keys.emplace_back(j);
                key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
                values.emplace_back(j * 2);
            }
            index->set_publish_version(i);
            vector<IndexValue> upsert_old_values(keys.size());
            ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
            ASSERT_OK(index->flush_memtable(true));
            ASSERT_OK(index->sync_flush_all_memtables(10000000));
        }
        Tablet tablet(_tablet_mgr.get(), tablet_id);
        auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
        tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
        MetaFileBuilder builder(tablet, tablet_metadata_ptr);
        ASSERT_OK(index->commit(&builder));
        ASSERT_GT(tablet_metadata_ptr->sstable_meta().sstables_size(), 0);
        sst_filename = tablet_metadata_ptr->sstable_meta().sstables(0).filename();
        sst_filesize = tablet_metadata_ptr->sstable_meta().sstables(0).filesize();
        sst_range.CopyFrom(tablet_metadata_ptr->sstable_meta().sstables(0).range());
    }

    config::lake_pk_index_partial_eviction = true;

    // Phase 2: build a fresh index backed by the empty _tablet_metadata (no SSTs loaded).
    // Now ingest the SST from phase 1 with shared=true (as cross-publish would do after
    // set_all_data_files_shared has marked op_write.ssts shared).
    auto index2 = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index2->init(_tablet_metadata));

    FileMetaPB sst_meta;
    sst_meta.set_name(sst_filename);
    sst_meta.set_size(sst_filesize);
    sst_meta.set_shared(true); // cross-publish marks this true

    DelvecPagePB delvec_page;
    ASSERT_OK(index2->ingest_sst(sst_meta, sst_range, 0, 1, delvec_page, nullptr));
    ASSERT_OK(index2->prepare_sstable_read_state(0, true));
    Key lookup_key = 3;
    Slice lookup_slice(reinterpret_cast<const uint8_t*>(&lookup_key), sizeof(Key));
    IndexValue lookup_value(NullIndexValue);
    ASSERT_OK(index2->get(1, &lookup_slice, &lookup_value));
    EXPECT_EQ(IndexValue(6), lookup_value);
    EXPECT_EQ(0, index2->memory_stats().index_block_bytes);
    EXPECT_EQ(0, _primary_key_mgr->sst_read_state_memory_context()->tracker.consumption());

    // Commit and verify the shared flag made it into sstable_meta.
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    ASSERT_OK(index2->commit(&builder));

    int match_count = 0;
    for (const auto& sst : tablet_metadata_ptr->sstable_meta().sstables()) {
        if (sst.filename() == sst_filename) {
            match_count++;
            EXPECT_TRUE(sst.shared()) << "sst_meta.shared=true must be preserved in committed sstable_meta";
            EXPECT_EQ(1, sst.generation_version()) << "ingest_sst stamps the generation version (the ingest version)";
        }
    }
    EXPECT_EQ(match_count, 1);

    config::l0_max_mem_usage = l0_max_mem_usage;
}

// Failure-injection guard for the parallel path in load_dels (see
// be/src/storage/lake/lake_persistent_index.cpp). With N=3 del files where every
// read fails (files do not exist on disk), the parallel phase must propagate the
// error via shared_status and return cleanly instead of crashing or proceeding
// to Phase 2 with default-constructed pkc slots. This is the regression surface
// added by the parallel refactor that existing publish-version tests do not
// cover, and is the targeted guard requested before broader backports.
TEST_F(LakePersistentIndexTest, test_load_dels_parallel_propagates_io_error) {
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    RowsetMetadataPB rowset_meta;
    rowset_meta.set_id(42);
    for (int i = 0; i < 3; ++i) {
        auto* d = rowset_meta.add_del_files();
        d->set_name(fmt::format("nonexistent_del_{}", i));
        d->set_origin_rowset_id(42);
        d->set_op_offset(0);
    }
    auto tablet_schema = std::make_shared<TabletSchema>(_tablet_metadata->schema());
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, /*index=*/0, tablet_schema);

    std::vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
    for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    auto st = index->load_dels(rowset, pkey_schema, /*rowset_version=*/1);
    EXPECT_FALSE(st.ok()) << "expected error from missing del files; got OK";
}

// Equivalence guard for the parallel cold rebuild scan in load_from_lake_tablet (see
// be/src/storage/lake/lake_persistent_index.cpp). The refactor splits the scan into a parallel
// Phase B (per-(rowset,segment) workers each owning their chunk/encoder/InsertBatch) and a serial
// Phase C that inserts the batches in version+segment order. With multiple single-segment rowsets
// and no SSTs, every rowset is rebuilt and scan_units > 1, so the parallel path engages. The index
// it produces MUST be identical to the serial fallback; we assert that by rebuilding twice (flag on
// vs off) over the SAME metadata and comparing get() results key-by-key.
TEST_F(LakePersistentIndexTest, test_load_from_lake_tablet_parallel_matches_serial) {
    const int kRowsetCount = 6;
    const int kRowsPerRowset = 50;
    const int64_t kBaseVersion = 2;

    auto build_metadata = [&](std::vector<std::string>* keys) {
        auto md = make_varchar_pk_metadata();
        md->set_version(kBaseVersion);
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*md));
        for (int r = 0; r < kRowsetCount; r++) {
            // Disjoint key ranges across rowsets so each key maps to exactly one value (no overwrite),
            // and rowset_id starts at 1 so no row lands on rssid 0 / value 0.
            append_cold_rowset(md.get(), /*start=*/r * kRowsPerRowset, kRowsPerRowset,
                               /*rowset_id=*/r + 1, keys);
        }
        return md;
    };

    // Run a cold rebuild with the given flag value and return the per-key values.
    auto rebuild_and_dump = [&](bool parallel, const std::shared_ptr<TabletMetadata>& md,
                                const std::vector<std::string>& keys) {
        std::optional<RebuildMemPressureGuard> serial_guard;
        if (!parallel) {
            serial_guard.emplace();
        }
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
        CHECK_OK(index->init(md));
        Tablet tablet(_tablet_mgr.get(), md->id());
        auto md_copy = std::make_shared<TabletMetadata>();
        md_copy->CopyFrom(*md);
        MetaFileBuilder builder(tablet, md_copy);
        CHECK_OK(index->load_from_lake_tablet(_tablet_mgr.get(), md, kBaseVersion, &builder));

        std::vector<Slice> slices;
        slices.reserve(keys.size());
        for (const auto& k : keys) {
            slices.emplace_back(k);
        }
        std::vector<IndexValue> values(keys.size());
        CHECK_OK(index->get(keys.size(), slices.data(), values.data()));
        return values;
    };

    std::vector<std::string> keys;
    auto md = build_metadata(&keys);
    ASSERT_EQ(kRowsetCount * kRowsPerRowset, static_cast<int>(keys.size()));

    auto parallel_values = rebuild_and_dump(true, md, keys);
    auto serial_values = rebuild_and_dump(false, md, keys);

    ASSERT_EQ(keys.size(), parallel_values.size());
    ASSERT_EQ(keys.size(), serial_values.size());
    for (size_t i = 0; i < keys.size(); i++) {
        EXPECT_NE(NullIndexValue, parallel_values[i].get_value())
                << "key " << keys[i] << " missing after parallel rebuild";
        EXPECT_EQ(serial_values[i].get_value(), parallel_values[i].get_value())
                << "parallel vs serial mismatch for key " << keys[i];
    }
}

// A scalar max-RSSID rebuild watermark is only safe when rowset RSSIDs increase with rowset version.
// MERGE can produce the opposite order: an earlier rowset may have a larger RSSID than a later rowset.
// Reproduce the resulting checkpoint gap without cross-publish or concurrent resharding. The first
// rowset is persisted in an SST while the later, lower-RSSID rowset remains in the active memtable;
// after commit and a cold reload, both real rowsets must still be represented by the rebuilt index.
TEST_F(LakePersistentIndexTest, test_cold_reload_preserves_non_monotonic_rssid_tail_after_partial_checkpoint) {
    constexpr uint32_t kEarlierHighRssid = 100;
    constexpr uint32_t kLaterLowRssid = 50;
    constexpr int64_t kEarlierVersion = 2;
    constexpr int64_t kLaterVersion = 3;

    auto md = make_varchar_pk_metadata();
    md->set_version(kLaterVersion);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    std::vector<std::string> high_keys;
    std::vector<std::string> low_keys;
    append_cold_rowset(md.get(), /*start=*/0, /*n=*/1, kEarlierHighRssid, &high_keys);
    append_cold_rowset(md.get(), /*start=*/100, /*n=*/1, kLaterLowRssid, &low_keys);
    ASSERT_EQ(2, md->rowsets_size());
    md->mutable_rowsets(0)->set_version(kEarlierVersion);
    md->mutable_rowsets(1)->set_version(kLaterVersion);
    ASSERT_GT(md->rowsets(0).id(), md->rowsets(1).id());

    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    Tablet tablet(_tablet_mgr.get(), md->id());
    auto checkpointed = std::make_shared<TabletMetadata>();
    checkpointed->CopyFrom(*md);

    {
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
        ASSERT_OK(index->init(md));

        const Slice high_key(high_keys[0]);
        const IndexValue high_value((static_cast<uint64_t>(kEarlierHighRssid) << 32));
        ASSERT_OK(index->insert(1, &high_key, &high_value, kEarlierVersion));
        ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

        const Slice low_key(low_keys[0]);
        const IndexValue low_value((static_cast<uint64_t>(kLaterLowRssid) << 32));
        ASSERT_OK(index->insert(1, &low_key, &low_value, kLaterVersion));

        std::array<Slice, 2> keys = {high_key, low_key};
        std::array<IndexValue, 2> before_reload;
        ASSERT_OK(index->get(keys.size(), keys.data(), before_reload.data()));
        EXPECT_EQ(high_value, before_reload[0]);
        EXPECT_EQ(low_value, before_reload[1]);

        MetaFileBuilder builder(tablet, checkpointed);
        ASSERT_OK(index->commit(&builder));
    }

    ASSERT_EQ(2, checkpointed->sstable_meta().sstables_size());
    EXPECT_EQ(static_cast<uint64_t>(kEarlierHighRssid) << 32, checkpointed->sstable_meta().sstables(0).max_rss_rowid());
    EXPECT_EQ(static_cast<uint64_t>(kEarlierHighRssid) << 32, checkpointed->sstable_meta().sstables(1).max_rss_rowid());

    auto reloaded = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(reloaded->init(checkpointed));
    auto reloaded_metadata = std::make_shared<TabletMetadata>();
    reloaded_metadata->CopyFrom(*checkpointed);
    MetaFileBuilder reload_builder(tablet, reloaded_metadata);
    ASSERT_OK(reloaded->load_from_lake_tablet(_tablet_mgr.get(), checkpointed, kLaterVersion, &reload_builder));

    std::array<Slice, 2> keys = {Slice(high_keys[0]), Slice(low_keys[0])};
    std::array<IndexValue, 2> after_reload;
    ASSERT_OK(reloaded->get(keys.size(), keys.data(), after_reload.data()));
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(kEarlierHighRssid) << 32), after_reload[0]);
    EXPECT_EQ(IndexValue(static_cast<uint64_t>(kLaterLowRssid) << 32), after_reload[1]);
}

TEST_F(LakePersistentIndexTest, test_complete_checkpoint_parallel_reload_with_cache_only_post_frontier_rowsets) {
    run_complete_checkpoint_cache_only_tail(true);
}

TEST_F(LakePersistentIndexTest, test_complete_checkpoint_serial_reload_with_cache_only_post_frontier_rowsets) {
    run_complete_checkpoint_cache_only_tail(false);
}

TEST_F(LakePersistentIndexTest, test_complete_checkpoint_drains_inactive_and_active_lower_tails) {
    auto md = make_varchar_pk_metadata();
    md->set_version(4);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> high_keys;
    std::vector<std::string> inactive_keys;
    std::vector<std::string> active_keys;
    append_cold_rowset(md.get(), 0, 1, 100, &high_keys);
    append_cold_rowset(md.get(), 100, 1, 50, &inactive_keys);
    append_cold_rowset(md.get(), 200, 1, 51, &active_keys);
    for (int i = 0; i < md->rowsets_size(); ++i) {
        md->mutable_rowsets(i)->set_version(i + 2);
    }
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    RebuildMemPressureGuard serial_rebuild_guard;
    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice high_key(high_keys[0]);
    const Slice inactive_key(inactive_keys[0]);
    const Slice active_key(active_keys[0]);
    const IndexValue high_value(uint64_t{100} << 32);
    const IndexValue inactive_value(uint64_t{50} << 32);
    const IndexValue active_value(uint64_t{51} << 32);
    ASSERT_OK(index->insert(1, &high_key, &high_value, 2));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    ASSERT_OK(index->insert(1, &inactive_key, &inactive_value, 3));
    wait_for_one_async_memtable_flush(index.get());
    ASSERT_OK(index->insert(1, &active_key, &active_value, 4));

    Tablet tablet(_tablet_mgr.get(), md->id());
    auto committed = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    index.reset();

    auto values = cold_reload_values(committed, 4, {high_keys[0], inactive_keys[0], active_keys[0]});
    EXPECT_EQ(high_value, values[0]);
    EXPECT_EQ(inactive_value, values[1]);
    EXPECT_EQ(active_value, values[2]);
}

TEST_F(LakePersistentIndexTest, test_complete_checkpoint_drains_completed_inactive_tail_with_empty_active) {
    auto md = make_varchar_pk_metadata();
    md->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> high_keys;
    std::vector<std::string> inactive_keys;
    append_cold_rowset(md.get(), 0, 1, 100, &high_keys);
    append_cold_rowset(md.get(), 100, 1, 50, &inactive_keys);
    md->mutable_rowsets(0)->set_version(2);
    md->mutable_rowsets(1)->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    RebuildMemPressureGuard serial_rebuild_guard;
    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice high_key(high_keys[0]);
    const Slice inactive_key(inactive_keys[0]);
    const IndexValue high_value(uint64_t{100} << 32);
    const IndexValue inactive_value(uint64_t{50} << 32);
    ASSERT_OK(index->insert(1, &high_key, &high_value, 2));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    ASSERT_OK(index->insert(1, &inactive_key, &inactive_value, 3));
    wait_for_one_async_memtable_flush(index.get());

    Tablet tablet(_tablet_mgr.get(), md->id());
    auto committed = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    index.reset();

    auto values = cold_reload_values(committed, 3, {high_keys[0], inactive_keys[0]});
    EXPECT_EQ(high_value, values[0]);
    EXPECT_EQ(inactive_value, values[1]);
}

TEST_F(LakePersistentIndexTest, test_complete_checkpoint_gate_runs_after_rebuild_threshold_flush) {
    auto md = make_varchar_pk_metadata();
    md->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> high_keys;
    std::vector<std::string> low_keys;
    append_cold_rowset(md.get(), 0, 1, 100, &high_keys);
    append_cold_rowset(md.get(), 100, 1, 50, &low_keys);
    md->mutable_rowsets(0)->set_version(2);
    md->mutable_rowsets(1)->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    RebuildMemPressureGuard serial_rebuild_guard;
    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    ConfigResetGuard<int32_t> files_guard(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_guard(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice high_key(high_keys[0]);
    const Slice low_key(low_keys[0]);
    const IndexValue high_value(uint64_t{100} << 32);
    const IndexValue low_value(uint64_t{50} << 32);
    ASSERT_OK(index->insert(1, &high_key, &high_value, 2));
    wait_for_one_async_memtable_flush(index.get());

    Tablet tablet(_tablet_mgr.get(), md->id());
    auto preloaded = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder preload_builder(tablet, preloaded);
    ASSERT_OK(index->commit(&preload_builder));
    ASSERT_EQ(0, preloaded->sstable_meta().sstables_size());

    config::cloud_native_pk_index_rebuild_files_threshold = 1;
    ASSERT_OK(index->insert(1, &low_key, &low_value, 3));
    auto committed = std::make_shared<TabletMetadata>(*preloaded);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    ASSERT_GT(committed->sstable_meta().sstables_size(), 0);
    // The pre-fix threshold path leaves the low tail in an asynchronous inactive memtable. Drain the
    // test object's runtime state only after snapshotting |committed| so teardown cannot race its file.
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    index.reset();

    auto values = cold_reload_values(committed, 3, {high_keys[0], low_keys[0]});
    EXPECT_EQ(high_value, values[0]);
    EXPECT_EQ(low_value, values[1]);
}

TEST_F(LakePersistentIndexTest, test_empty_sst_metadata_and_filesets_keep_native_cache_only_path) {
    auto md = make_varchar_pk_metadata();
    md->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> keys;
    append_cold_rowset(md.get(), 0, 1, 10, &keys);
    md->mutable_rowsets(0)->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    const auto before_meta = md->sstable_meta().SerializeAsString();
    const auto before_inventory = sst_inventory(md->id());

    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    ConfigResetGuard<int32_t> files_guard(&config::cloud_native_pk_index_rebuild_files_threshold, 0);
    ConfigResetGuard<int64_t> rows_guard(&config::cloud_native_pk_index_rebuild_rows_threshold, 0);
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice key(keys[0]);
    const IndexValue value(uint64_t{10} << 32);
    ASSERT_OK(index->insert(1, &key, &value, 2));
    Tablet tablet(_tablet_mgr.get(), md->id());
    auto committed = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));

    EXPECT_EQ(0, committed->sstable_meta().sstables_size());
    EXPECT_EQ(before_meta, committed->sstable_meta().SerializeAsString());
    EXPECT_EQ(before_inventory, sst_inventory(md->id()));
}

TEST_F(LakePersistentIndexTest, test_existing_sst_does_not_force_safe_later_active_entry) {
    auto md = make_varchar_pk_metadata();
    md->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> high_keys;
    append_cold_rowset(md.get(), 0, 1, 100, &high_keys);
    md->mutable_rowsets(0)->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    RebuildMemPressureGuard serial_rebuild_guard;
    ConfigResetGuard<int64_t> l0_guard(&config::l0_max_mem_usage, std::numeric_limits<int64_t>::max());
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice high_key(high_keys[0]);
    const IndexValue high_value(uint64_t{100} << 32);
    ASSERT_OK(index->insert(1, &high_key, &high_value, 2));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    Tablet tablet(_tablet_mgr.get(), md->id());
    auto first = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder first_builder(tablet, first);
    ASSERT_OK(index->commit(&first_builder));
    const auto first_meta = first->sstable_meta().SerializeAsString();
    const auto first_inventory = sst_inventory(md->id());

    std::vector<std::string> later_keys;
    append_cold_rowset(first.get(), 100, 1, 101, &later_keys);
    first->mutable_rowsets(1)->set_version(3);
    first->set_version(3);
    const Slice later_key(later_keys[0]);
    const IndexValue later_value(uint64_t{101} << 32);
    ASSERT_OK(index->insert(1, &later_key, &later_value, 3));
    auto second = std::make_shared<TabletMetadata>(*first);
    MetaFileBuilder second_builder(tablet, second);
    ASSERT_OK(index->commit(&second_builder));
    EXPECT_EQ(first_meta, second->sstable_meta().SerializeAsString());
    EXPECT_EQ(first_inventory, sst_inventory(md->id()));
    index.reset();

    auto values = cold_reload_values(second, 3, {high_keys[0], later_keys[0]});
    EXPECT_EQ(high_value, values[0]);
    EXPECT_EQ(later_value, values[1]);
}

TEST_F(LakePersistentIndexTest, test_first_fileset_with_empty_memtables_creates_no_redundant_sst) {
    auto md = make_varchar_pk_metadata();
    md->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> keys;
    append_cold_rowset(md.get(), 0, 1, 100, &keys);
    md->mutable_rowsets(0)->set_version(2);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    const Slice key(keys[0]);
    const IndexValue value(uint64_t{100} << 32);
    ASSERT_OK(index->insert(1, &key, &value, 2));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    const auto before_inventory = sst_inventory(md->id());
    ASSERT_EQ(1, before_inventory.size());

    Tablet tablet(_tablet_mgr.get(), md->id());
    auto committed = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder builder(tablet, committed);
    ASSERT_OK(index->commit(&builder));
    EXPECT_EQ(1, committed->sstable_meta().sstables_size());
    EXPECT_EQ(before_inventory, sst_inventory(md->id()));
}

TEST_F(LakePersistentIndexTest, test_cold_rebuild_rejects_unmasked_duplicate_primary_keys) {
    auto md = make_varchar_pk_metadata();
    md->set_version(3);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));
    std::vector<std::string> first_keys;
    std::vector<std::string> duplicate_keys;
    append_cold_rowset(md.get(), 0, 1, 100, &first_keys);
    append_cold_rowset(md.get(), 0, 1, 50, &duplicate_keys);
    md->mutable_rowsets(0)->set_version(2);
    md->mutable_rowsets(1)->set_version(3);
    ASSERT_EQ(first_keys[0], duplicate_keys[0]);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*md));

    ConfigResetGuard<bool> consistency_guard(&config::experimental_lake_ignore_pk_consistency_check, false);
    RebuildMemPressureGuard serial_rebuild_guard;
    Tablet tablet(_tablet_mgr.get(), md->id());
    auto reload_metadata = std::make_shared<TabletMetadata>(*md);
    MetaFileBuilder builder(tablet, reload_metadata);
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), md->id());
    ASSERT_OK(index->init(md));
    auto st = index->load_from_lake_tablet(_tablet_mgr.get(), md, 3, &builder);
    EXPECT_TRUE(st.is_already_exist()) << st;
}

// A del file that did NOT originate from the rebuilt rowset must run get()+filter and drop deletes
// that are too old for the current index entry. too_old = origin_rowset_id + op_offset: keys whose
// current rssid (value >> 32) is <= too_old get erased; keys with a newer rssid keep their value.
TEST_F(LakePersistentIndexTest, test_load_dels_non_origin_filters_stale_deletes) {
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, 2);
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    std::vector<std::string> keys = {"k0", "k1", "k2", "k3"};
    std::vector<uint32_t> rssids = {5, 5, 50, 50};
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        key_slices.emplace_back(keys[i]);
        values.emplace_back(((uint64_t)rssids[i] << 32) | i);
    }
    ASSERT_OK(index->insert(keys.size(), key_slices.data(), values.data(), /*version=*/1));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    write_binary_del_file(tablet_id, "del_nonorigin", keys);
    RowsetMetadataPB rowset_meta;
    rowset_meta.set_id(999);
    auto* d = rowset_meta.add_del_files();
    d->set_name("del_nonorigin");
    d->set_origin_rowset_id(20); // != rowset id -> non-origin; too_old = 20 + 0
    d->set_op_offset(0);

    auto tablet_schema = make_varchar_pk_schema();
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, /*index=*/0, tablet_schema);
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    scoped_refptr<Trace> trace(new Trace);
    {
        ADOPT_TRACE(trace.get());
        ASSERT_OK(index->load_dels(rowset, pkey_schema, /*rowset_version=*/10));
    }
    int64_t tasks = 0;
    for (const auto& [name, value] : trace->metrics()->Get()) {
        if (std::string(name) == "rebuild_index_del_get_cnt") {
            tasks = value;
        }
    }
    EXPECT_EQ(2, tasks); // A single del file also fans out its SST filter lookup.

    std::vector<IndexValue> got(keys.size());
    ASSERT_OK(index->get(keys.size(), key_slices.data(), got.data()));
    EXPECT_EQ(IndexValue(NullIndexValue), got[0]); // rssid 5  <= 20 -> erased
    EXPECT_EQ(IndexValue(NullIndexValue), got[1]); // rssid 5  <= 20 -> erased
    EXPECT_EQ(values[2], got[2]);                  // rssid 50 >  20 -> stale delete skipped
    EXPECT_EQ(values[3], got[3]);                  // rssid 50 >  20 -> stale delete skipped
}

// Origin-skip: a del file whose origin_rowset_id == the rebuilt rowset id skips get() entirely and
// erases every key regardless of its current rssid. Same keys/rssids as the non-origin test above,
// but here even the rssid-50 keys (which the non-origin path keeps) are erased.
TEST_F(LakePersistentIndexTest, test_load_dels_origin_erases_all_keys) {
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    std::vector<std::string> keys = {"k0", "k1", "k2", "k3"};
    std::vector<uint32_t> rssids = {5, 5, 50, 50};
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        key_slices.emplace_back(keys[i]);
        values.emplace_back(((uint64_t)rssids[i] << 32) | i);
    }
    ASSERT_OK(index->insert(keys.size(), key_slices.data(), values.data(), /*version=*/1));

    write_binary_del_file(tablet_id, "del_origin", keys);
    RowsetMetadataPB rowset_meta;
    rowset_meta.set_id(777);
    auto* d = rowset_meta.add_del_files();
    d->set_name("del_origin");
    d->set_origin_rowset_id(777); // == rowset id -> origin del file
    d->set_op_offset(0);

    auto tablet_schema = make_varchar_pk_schema();
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, /*index=*/0, tablet_schema);
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    ASSERT_OK(index->load_dels(rowset, pkey_schema, /*rowset_version=*/10));

    std::vector<IndexValue> got(keys.size());
    ASSERT_OK(index->get(keys.size(), key_slices.data(), got.data()));
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_EQ(IndexValue(NullIndexValue), got[i]) << "key " << keys[i] << " should be erased by origin del";
    }
}

// The two-phase parallel path (num_del_files > 1, parallel execution on) must produce the same index
// as the single-pass path, including when del files share keys. Files are an origin + a non-origin del
// that overlap on keys, exercising the idempotent-erase / shared-key reasoning in load_dels.
TEST_F(LakePersistentIndexTest, test_load_dels_parallel_matches_single_pass) {
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, 2);
    // BE_TEST gives the PK execution pool one worker. load_one must return after submitting its
    // lookups so that this same worker can execute them; only the caller may join the two runners.
    ASSERT_EQ(1, RuntimeEnv::GetInstance()->pk_index_execution_thread_pool()->max_threads());
    auto tablet_schema = make_varchar_pk_schema();
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);

    std::vector<std::string> keys = {"k0", "k1", "k2", "k3", "k4", "k5"};
    std::vector<uint32_t> rssids = {5, 50, 5, 50, 5, 50};

    auto run = [&](bool parallel) {
        std::optional<RebuildMemPressureGuard> serial_guard;
        if (!parallel) {
            serial_guard.emplace();
        }
        auto tablet_id = _tablet_metadata->id();
        auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
        CHECK_OK(index->init(_tablet_metadata));

        std::vector<Slice> key_slices;
        std::vector<IndexValue> values;
        for (size_t i = 0; i < keys.size(); ++i) {
            key_slices.emplace_back(keys[i]);
            values.emplace_back(((uint64_t)rssids[i] << 32) | i);
        }
        CHECK_OK(index->insert(keys.size(), key_slices.data(), values.data(), /*version=*/1));
        CHECK_OK(index->sync_flush_all_memtables(10'000'000));

        // Two del files that share keys: one non-origin (filters stale deletes), one origin (erase all).
        write_binary_del_file(tablet_id, "del_a", {"k0", "k1", "k2"});
        write_binary_del_file(tablet_id, "del_b", {"k2", "k3", "k4", "k5"});
        RowsetMetadataPB rowset_meta;
        rowset_meta.set_id(888);
        auto* da = rowset_meta.add_del_files();
        da->set_name("del_a");
        da->set_origin_rowset_id(20); // non-origin, too_old = 20
        da->set_op_offset(0);
        auto* db = rowset_meta.add_del_files();
        db->set_name("del_b");
        db->set_origin_rowset_id(888); // origin -> erase all of k2..k5
        db->set_op_offset(0);

        auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, /*index=*/0, tablet_schema);
        scoped_refptr<Trace> trace(new Trace);
        {
            ADOPT_TRACE(trace.get());
            CHECK_OK(index->load_dels(rowset, pkey_schema, /*rowset_version=*/10));
        }
        int64_t tasks = 0;
        bool saw_sst_reads = false;
        for (const auto& [name, value] : trace->metrics()->Get()) {
            if (std::string(name) == "rebuild_index_del_get_cnt") {
                tasks = value;
            }
            saw_sst_reads |= std::string(name) == "multi_get_us";
        }
        EXPECT_EQ(2, tasks);        // Only the inherited del's three keys need the filter lookup.
        EXPECT_TRUE(saw_sst_reads); // The caller's trace survives both levels of pool tasks.

        std::vector<IndexValue> got(keys.size());
        CHECK_OK(index->get(keys.size(), key_slices.data(), got.data()));
        return got;
    };

    auto serial = run(false);
    auto parallel = run(true);
    ASSERT_EQ(serial.size(), parallel.size());
    for (size_t i = 0; i < serial.size(); ++i) {
        EXPECT_EQ(serial[i], parallel[i]) << "mismatch at key " << keys[i];
        EXPECT_EQ(i == 1 ? IndexValue((uint64_t{50} << 32) | i) : IndexValue(NullIndexValue), parallel[i]);
    }
}

TEST_F(LakePersistentIndexTest, test_parallel_get_matches_get_across_index_layers) {
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, 2);
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), _tablet_metadata->id());
    ASSERT_OK(index->init(_tablet_metadata));

    std::array<uint64_t, 7> keys = {0, 1, 2, 3, 4, 5, 6};
    std::array<Slice, 7> slices;
    std::array<IndexValue, 7> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        slices[i] = Slice(reinterpret_cast<const char*>(&keys[i]), sizeof(keys[i]));
        values[i] = IndexValue((uint64_t{5} << 32) | i);
    }
    // The last key is absent. With no backing store, even a large lookup stays inline.
    ASSERT_OK(index->insert(6, slices.data(), values.data(), 1));
    scoped_refptr<Trace> memtable_trace(new Trace);
    {
        ADOPT_TRACE(memtable_trace.get());
        ParallelTaskRunner runner(nullptr);
        LakePersistentIndex::DelFilter words;
        ASSERT_OK(index->parallel_get(&runner, keys.size(), slices.data(), 0, &words));
        ASSERT_OK(runner.join());
        std::vector<bool> got(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            got[i] = (words[i / 64].load() & (uint64_t{1} << (i % 64))) != 0;
        }
        EXPECT_EQ((std::vector<bool>{true, true, true, true, true, true, false}), got);
    }
    for (const auto& [name, value] : memtable_trace->metrics()->Get()) {
        EXPECT_NE("rebuild_index_del_get_cnt", std::string(name));
    }
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    // A newer SST overrides a key and hides another behind a tombstone.
    values[2] = IndexValue((uint64_t{50} << 32) | 2);
    ASSERT_OK(index->insert(1, &slices[2], &values[2], 2));
    ASSERT_OK(index->replay_erase(1, &slices[3], {false}, 2, 50));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));
    // The active memtable overrides an SST entry and contains another tombstone.
    values[4] = IndexValue((uint64_t{60} << 32) | 4);
    ASSERT_OK(index->insert(1, &slices[4], &values[4], 3));
    ASSERT_OK(index->replay_erase(1, &slices[0], {false}, 3, 60));
    values[0] = values[3] = values[6] = IndexValue(NullIndexValue);

    // Empty/small batches exercise the inline path; 3 and 7 include a partial final task.
    for (size_t n : {0, 1, 2, 3, 7}) {
        std::vector<IndexValue> serial(n);
        ASSERT_OK(index->get(n, slices.data(), serial.data()));
        for (size_t i = 0; i < n; ++i) {
            EXPECT_EQ(values[i], serial[i]) << "key " << i;
        }
        // Compare filters at layer boundaries, including equal rssids, tombstones and missing keys.
        for (uint32_t too_old : {0, 5, 20, 50, 60}) {
            scoped_refptr<Trace> trace(new Trace);
            auto token = RuntimeEnv::GetInstance()->pk_index_execution_thread_pool()->new_token(
                    ThreadPool::ExecutionMode::CONCURRENT);
            LakePersistentIndex::DelFilter words;
            {
                ADOPT_TRACE(trace.get());
                ParallelTaskRunner runner(token.get());
                ASSERT_OK(index->parallel_get(&runner, n, slices.data(), too_old, &words));
                ASSERT_OK(runner.join());
            }
            std::vector<bool> parallel(n);
            for (size_t i = 0; i < n; ++i) {
                parallel[i] = (words[i / 64].load() & (uint64_t{1} << (i % 64))) != 0;
            }
            ASSERT_EQ(n, parallel.size());
            int64_t tasks = 0;
            for (const auto& [name, value] : trace->metrics()->Get()) {
                if (std::string(name) == "rebuild_index_del_get_cnt") {
                    tasks = value;
                }
            }
            EXPECT_EQ(n > 2 ? (n + 1) / 2 : 0, tasks);
            for (size_t i = 0; i < n; ++i) {
                EXPECT_EQ(serial[i] != IndexValue(NullIndexValue) && serial[i].get_rssid() > too_old, parallel[i])
                        << "key " << i << ", cutoff " << too_old;
            }
        }
    }
}

TEST_F(LakePersistentIndexTest, test_parallel_get_retains_compact_filters_for_many_files) {
    constexpr size_t rows_per_batch = 257; // Deliberately not aligned to packed bitmap words.
    constexpr size_t rows_per_file = 5003;
    constexpr size_t num_files = 40;
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, rows_per_batch);
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), _tablet_metadata->id());
    ASSERT_OK(index->init(_tablet_metadata));
    std::vector<uint64_t> keys(rows_per_file);
    std::vector<Slice> slices(rows_per_file);
    std::vector<IndexValue> values(rows_per_file);
    for (size_t i = 0; i < rows_per_file; ++i) {
        keys[i] = i;
        slices[i] = Slice(reinterpret_cast<const char*>(&keys[i]), sizeof(keys[i]));
        values[i] = IndexValue((uint64_t{i % 2 ? 50U : 5U} << 32) | i);
    }
    ASSERT_OK(index->insert(rows_per_file, slices.data(), values.data(), 1));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    std::atomic<size_t> max_scratch_rows{0};
    std::atomic<size_t> lookups{0};
    SyncPoint::GetInstance()->SetCallBack("LakePersistentIndex::parallel_get:values", [&](void* arg) {
        const auto& scratch = *static_cast<std::vector<IndexValue>*>(arg);
        auto old = max_scratch_rows.load();
        while (old < scratch.capacity() && !max_scratch_rows.compare_exchange_weak(old, scratch.capacity())) {
        }
        ++lookups;
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([]() {
        SyncPoint::GetInstance()->ClearCallBack("LakePersistentIndex::parallel_get:values");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    std::vector<LakePersistentIndex::DelFilter> filters(num_files);
    auto token = RuntimeEnv::GetInstance()->pk_index_execution_thread_pool()->new_token(
            ThreadPool::ExecutionMode::CONCURRENT);
    ParallelTaskRunner runner(token.get());
    // Keep all files' output alive through the same join, as load_dels does.
    for (auto& file : filters) {
        ASSERT_OK(index->parallel_get(&runner, rows_per_file, slices.data(), 20, &file));
    }
    ASSERT_OK(runner.join());
    EXPECT_LE(max_scratch_rows.load(), rows_per_batch);
    EXPECT_EQ(num_files * ((rows_per_file + rows_per_batch - 1) / rows_per_batch), lookups.load());
    size_t retained_bits = 0;
    for (const auto& file : filters) {
        ASSERT_EQ((rows_per_file + 63) / 64, file.size());
        retained_bits += file.capacity() * 64;
        for (size_t row = 0; row < rows_per_file; ++row) {
            const bool skip = (file[row / 64].load() & (uint64_t{1} << (row % 64))) != 0;
            EXPECT_EQ(row % 2 != 0, skip) << "row " << row;
        }
    }
    // Allow bitmap word rounding, but not one byte (let alone an IndexValue) per retained key.
    EXPECT_LT(retained_bits, num_files * rows_per_file * 2);
}

TEST_F(LakePersistentIndexTest, test_parallel_get_tiny_batches_share_compact_bitmap_words) {
    constexpr size_t n = 131; // Includes two word boundaries and a partial final word.
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), _tablet_metadata->id());
    ASSERT_OK(index->init(_tablet_metadata));
    std::vector<uint64_t> keys(n);
    std::vector<Slice> slices(n);
    std::vector<IndexValue> values(n);
    for (size_t i = 0; i < n; ++i) {
        keys[i] = i;
        slices[i] = Slice(reinterpret_cast<const char*>(&keys[i]), sizeof(keys[i]));
        values[i] = IndexValue((uint64_t{i % 3 ? 50U : 5U} << 32) | i);
    }
    ASSERT_OK(index->insert(n - 1, slices.data(), values.data(), 1)); // Last key is absent.
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    std::unique_ptr<ThreadPool> pool;
    ASSERT_OK(ThreadPoolBuilder("del_filter_test").set_min_threads(4).set_max_threads(4).build(&pool));
    auto token = pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    // One-row batches concurrently merge bits into the same word. Retained output must remain
    // one bitmap per file, independent of batch size, without dropping any concurrent bit writes.
    for (int repeat = 0; repeat < 10; ++repeat) {
        for (int64_t rows_per_batch : {1, 2, 3, 63, 64, 65, 257}) {
            ConfigResetGuard<int64_t> guard(&config::pk_index_parallel_execution_min_rows, rows_per_batch);
            scoped_refptr<Trace> trace(new Trace);
            LakePersistentIndex::DelFilter words;
            {
                ADOPT_TRACE(trace.get());
                ParallelTaskRunner runner(token.get());
                ASSERT_OK(index->parallel_get(&runner, n, slices.data(), 20, &words));
                ASSERT_OK(runner.join());
            }
            ASSERT_EQ((n + 63) / 64, words.size());
            EXPECT_LT(words.capacity() * sizeof(words[0]), n); // Much less than one byte per key.
            int64_t tasks = 0;
            for (const auto& [name, value] : trace->metrics()->Get()) {
                if (std::string(name) == "rebuild_index_del_get_cnt") {
                    tasks = value;
                }
            }
            EXPECT_EQ(n > rows_per_batch ? (n + rows_per_batch - 1) / rows_per_batch : 0, tasks);
            for (size_t i = 0; i < n; ++i) {
                const bool skip = (words[i / 64].load() & (uint64_t{1} << (i % 64))) != 0;
                EXPECT_EQ(i < n - 1 && i % 3 != 0, skip) << "row " << i << ", batch " << rows_per_batch;
            }
            EXPECT_EQ(0, words.back().load() >> (n % 64)); // Unused tail bits stay zero.
        }
    }
}

TEST_F(LakePersistentIndexTest, test_load_dels_parallel_get_propagates_sst_error_before_replay) {
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, 2);
    const auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    std::vector<std::string> keys = {"k0", "k1", "k2", "k3", "k4"};
    std::vector<Slice> slices;
    std::vector<IndexValue> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        slices.emplace_back(keys[i]);
        values.emplace_back((uint64_t{5} << 32) | i);
    }
    ASSERT_OK(index->insert(keys.size(), slices.data(), values.data(), 1));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    RowsetMetadataPB rowset_meta;
    rowset_meta.set_id(999);
    for (const auto* name : {"del_lookup_error_a", "del_lookup_error_b"}) {
        write_binary_del_file(tablet_id, name, keys);
        auto* del = rowset_meta.add_del_files();
        del->set_name(name);
        del->set_origin_rowset_id(20);
        del->set_op_offset(0);
    }
    auto tablet_schema = make_varchar_pk_schema();
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, 0, tablet_schema);
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>{0});

    std::atomic<int> lookups{0};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::multi_get:error", [&](void* arg) {
        ++lookups;
        *static_cast<Status*>(arg) = Status::IOError("injected del lookup failure");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([]() {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::multi_get:error");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    const auto errors_before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();
    auto st = index->load_dels(rowset, pkey_schema, 10);
    EXPECT_TRUE(st.is_io_error()) << st;
    EXPECT_NE(std::string::npos, st.to_string().find("injected del lookup failure"));
    // Both del files' three batches finish before load_dels returns the original SST error.
    EXPECT_EQ(6, lookups.load());
    EXPECT_EQ(errors_before + 6, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
    SyncPoint::GetInstance()->DisableProcessing();
    std::vector<IndexValue> got(keys.size());
    ASSERT_OK(index->get(keys.size(), slices.data(), got.data()));
    EXPECT_EQ(values, got); // Phase 2 did not replay any delete on an incomplete prefetch.
}

TEST_F(LakePersistentIndexTest, test_load_dels_parallel_load_error_drains_get_tasks) {
    ConfigResetGuard<int64_t> min_rows_guard(&config::pk_index_parallel_execution_min_rows, 2);
    ASSERT_EQ(1, RuntimeEnv::GetInstance()->pk_index_execution_thread_pool()->max_threads());
    const auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    std::vector<std::string> keys = {"k0", "k1", "k2", "k3", "k4"};
    std::vector<Slice> slices;
    std::vector<IndexValue> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        slices.emplace_back(keys[i]);
        values.emplace_back((uint64_t{5} << 32) | i);
    }
    ASSERT_OK(index->insert(keys.size(), slices.data(), values.data(), 1));
    ASSERT_OK(index->sync_flush_all_memtables(10'000'000));

    write_binary_del_file(tablet_id, "del_before_load_error", keys);
    RowsetMetadataPB rowset_meta;
    rowset_meta.set_id(999);
    // The first load queues three lookups; the second load fails while those tasks are pending.
    for (const auto* name : {"del_before_load_error", "missing_del_file"}) {
        auto* del = rowset_meta.add_del_files();
        del->set_name(name);
        del->set_origin_rowset_id(20);
        del->set_op_offset(0);
    }
    auto tablet_schema = make_varchar_pk_schema();
    auto rowset = std::make_shared<Rowset>(_tablet_mgr.get(), tablet_id, &rowset_meta, 0, tablet_schema);
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, std::vector<ColumnId>{0});

    std::atomic<int> lookups{0};
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::multi_get:error", [&](void* arg) {
        ++lookups;
        *static_cast<Status*>(arg) = Status::IOError("lookup after load error");
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp cleanup([]() {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::multi_get:error");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    auto st = index->load_dels(rowset, pkey_schema, 10);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("missing_del_file"));
    EXPECT_EQ(3, lookups.load()); // The failing load must not free keys before the queued reads finish.
    SyncPoint::GetInstance()->DisableProcessing();
    std::vector<IndexValue> got(keys.size());
    ASSERT_OK(index->get(keys.size(), slices.data(), got.data()));
    EXPECT_EQ(values, got);
}

TEST(AssignGenerationVersionsTest, stamps_new_preserves_existing_and_legacy) {
    PersistentIndexSstableMetaPB prev;
    {
        auto* s = prev.add_sstables();
        s->set_filename("existing_v3.sst");
        s->set_generation_version(3);
    }
    {
        auto* s = prev.add_sstables();
        s->set_filename("legacy_zero.sst"); // version unset == 0
    }

    PersistentIndexSstableMetaPB cur;
    {
        auto* s = cur.add_sstables();
        s->set_filename("existing_v3.sst"); // reappears, version dropped by copy
    }
    {
        auto* s = cur.add_sstables();
        s->set_filename("legacy_zero.sst"); // legacy, still unset
    }
    {
        auto* s = cur.add_sstables();
        s->set_filename("fresh_new.sst"); // brand new this publish
    }
    {
        auto* s = cur.add_sstables();
        s->set_filename("ingested.sst");
        s->set_generation_version(9); // producer already stamped, absent from prev
    }
    {
        auto* s = cur.add_sstables();
        s->set_filename("existing_v3.sst");
        s->set_generation_version(7); // same filename as prev@3 but already non-zero
    }

    LakePersistentIndex::assign_generation_versions(&cur, prev, /*generation_version=*/100);

    ASSERT_EQ(5, cur.sstables_size());
    EXPECT_EQ(3, cur.sstables(0).generation_version());   // existing, local 0 -> carried from prev (3)
    EXPECT_EQ(0, cur.sstables(1).generation_version());   // legacy 0 -> STAYS 0 (present in prev); vacuum retains
    EXPECT_EQ(100, cur.sstables(2).generation_version()); // new (absent from prev) -> new version
    EXPECT_EQ(9, cur.sstables(3).generation_version());   // producer-stamped non-zero -> preserved (absent from prev)
    EXPECT_EQ(7, cur.sstables(4).generation_version());   // non-zero -> NEVER overwritten, even though prev has @3
}

TEST_F(LakePersistentIndexTest, test_commit_stamps_version) {
    const int N = 100;
    const int64_t publish_version = 7;
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    using Key = uint64_t;
    std::vector<Key> keys(N);
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;
    key_slices.reserve(N);
    values.reserve(N);
    for (int j = 0; j < N; j++) {
        keys[j] = j;
        key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
        values.emplace_back(j * 2);
    }
    index->set_publish_version(publish_version);
    std::vector<IndexValue> old_values(N);
    ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), old_values.data()));
    ASSERT_OK(index->flush_memtable(true));
    ASSERT_OK(index->sync_flush_all_memtables(10000000));

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto meta = std::make_shared<TabletMetadata>();
    meta->CopyFrom(*_tablet_metadata);
    meta->set_version(publish_version);
    MetaFileBuilder builder(tablet, meta);
    ASSERT_OK(index->commit(&builder));

    // A freshly-flushed sstable is stamped with the publish version.
    ASSERT_GE(meta->sstable_meta().sstables_size(), 1);
    for (const auto& s : meta->sstable_meta().sstables()) {
        EXPECT_EQ(publish_version, s.generation_version()) << "freshly-flushed sstable must carry its publish version";
    }

    // Re-commit at a later version WITHOUT new data: existing sstables must keep
    // their original version (no re-stamp), because they are present in
    // the previous sstable_meta the builder carries forward.
    auto meta2 = std::make_shared<TabletMetadata>();
    meta2->CopyFrom(*meta); // carries the publish_version sstable_meta forward
    meta2->set_version(publish_version + 10);
    MetaFileBuilder builder2(tablet, meta2);
    ASSERT_OK(index->commit(&builder2));
    ASSERT_GE(meta2->sstable_meta().sstables_size(), 1);
    for (const auto& s : meta2->sstable_meta().sstables()) {
        EXPECT_EQ(publish_version, s.generation_version()) << "re-commit must not re-stamp an existing sstable";
    }
}

} // namespace starrocks::lake
