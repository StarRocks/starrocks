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

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/type_traits.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/record_predicate/column_hash_is_congruent.h"
#include "test_util.h"

namespace starrocks::lake {

class LakePersistentIndexTest : public TestBase {
public:
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
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    constexpr static const char* const kTestDirectory = "test_lake_persistent_index";

    std::shared_ptr<TabletMetadata> _tablet_metadata;

    TabletSchemaPB create_tablet_schema_pb(const std::vector<std::pair<std::string, std::string>>& columns,
                                           int num_key_columns) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(PRIMARY_KEYS);
        schema_pb.set_num_short_key_columns(num_key_columns);
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
};

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

TEST_F(LakePersistentIndexTest, test_major_compaction) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 5;
    const int N = 100;
    vector<Key> total_keys;
    vector<Slice> total_key_slices;
    vector<IndexValue> total_values;
    vector<size_t> idxes;
    total_key_slices.reserve(M * N);
    total_keys.reserve(M * N);
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    int k = 0;
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            total_keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            total_key_slices.emplace_back((uint8_t*)(&total_keys[k]), sizeof(Key));
            values.emplace_back(j * 2);
            total_values.emplace_back(j * 2);
            ++k;
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        // generate sst files.
        index->flush_memtable(true);
    }
    ASSERT_TRUE(index->memory_usage() > 0);

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    // commit sst files
    ASSERT_OK(index->commit(&builder));
    vector<IndexValue> get_values(M * N);
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));

    get_values.clear();
    get_values.reserve(M * N);
    auto txn_log = std::make_shared<TxnLogPB>();
    // try to compact sst files.
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get()));
    ASSERT_TRUE(txn_log->op_compaction().input_sstables_size() > 0);
    ASSERT_TRUE(txn_log->op_compaction().has_output_sstable());
    ASSERT_OK(index->apply_opcompaction(txn_log->op_compaction()));
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));
    for (int i = 0; i < M * N; i++) {
        ASSERT_EQ(total_values[i], get_values[i]);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_major_compaction_with_tablet_range) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    const int N = 100;

    // Use single column VARCHAR primary key
    _tablet_metadata->mutable_schema()->mutable_column(0)->set_type("VARCHAR");
    _tablet_metadata->mutable_schema()->mutable_column(0)->set_length(65535);

    auto tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    std::vector<ColumnId> pk_columns = {0};
    auto pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    auto encode_key = [&](const std::string& v) {
        auto chunk = std::make_unique<Chunk>();
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false);
        col->append_datum(Datum(Slice(v)));
        chunk->append_column(std::move(col), (SlotId)0);

        MutableColumnPtr pk_column;
        EXPECT_OK(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get());
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            return std::string(reinterpret_cast<const char*>(pk_column->raw_data()), pk_column->type_size());
        }
    };

    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));

    // Build multiple levels of sstables to trigger merge.
    std::vector<std::string> keys;
    std::vector<Slice> key_slices;
    std::vector<IndexValue> values;
    std::vector<IndexValue> upsert_old_values(N);
    for (int i = 0; i < 3; ++i) {
        keys.clear();
        key_slices.clear();
        values.clear();
        keys.reserve(N);
        key_slices.reserve(N);
        values.reserve(N);
        for (int j = 0; j < N; ++j) {
            // Use keys like "key_00", "key_01", ..., "key_99"
            char buf[16];
            snprintf(buf, sizeof(buf), "key_%02d", j);
            keys.emplace_back(encode_key(buf));
            key_slices.emplace_back(keys.back());
            values.emplace_back(j * 2 + i);
        }
        index->prepare(EditVersion(i, 0), 0);
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        ASSERT_OK(index->flush_memtable(true));
    }
    ASSERT_TRUE(index->memory_usage() > 0);

    // Build tablet metadata with a tablet range so that major_compact will honor it.
    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);

    // Ensure sort key is the primary key column so that TabletRangeHelper can build SstSeekRange.
    auto* schema_pb = tablet_metadata_ptr->mutable_schema();
    schema_pb->clear_sort_key_idxes();
    schema_pb->add_sort_key_idxes(0);

    // Configure a tablet range ["key_10", "key_30").
    TabletRangePB* range_pb = tablet_metadata_ptr->mutable_range();
    range_pb->Clear();
    auto* lower = range_pb->mutable_lower_bound();
    auto* lower_v = lower->add_values();
    TypeDescriptor type_varchar(TYPE_VARCHAR);
    lower_v->mutable_type()->CopyFrom(type_varchar.to_protobuf());
    lower_v->set_value("key_10");
    range_pb->set_lower_bound_included(true);
    auto* upper = range_pb->mutable_upper_bound();
    auto* upper_v = upper->add_values();
    upper_v->mutable_type()->CopyFrom(type_varchar.to_protobuf());
    upper_v->set_value("key_30");
    range_pb->set_upper_bound_included(false);

    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    ASSERT_OK(index->commit(&builder));

    // Mark all sstables as shared so that range pruning path is exercised.
    auto* sstable_meta = tablet_metadata_ptr->mutable_sstable_meta();
    for (auto& sst_pb : *sstable_meta->mutable_sstables()) {
        sst_pb.set_shared(true);
    }

    auto txn_log = std::make_shared<TxnLogPB>();
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get()));
    ASSERT_TRUE(txn_log->op_compaction().has_output_sstable());

    const auto& out_sst = txn_log->op_compaction().output_sstable();

    // The compacted output sstable should only cover keys in ["key_10", "key_30").
    ASSERT_EQ(encode_key("key_10"), out_sst.range().start_key());
    // end_key is inclusive, so for ["key_10", "key_30") we expect the last key to be "key_29".
    ASSERT_EQ(encode_key("key_29"), out_sst.range().end_key());

    config::l0_max_mem_usage = l0_max_mem_usage;
}

TEST_F(LakePersistentIndexTest, test_compaction_strategy) {
    PersistentIndexSstableMetaPB sstable_meta;
    std::vector<PersistentIndexSstablePB> sstables;
    bool merge_base_level = false;
    auto test_fn = [&](size_t sub_size, size_t N, bool is_base) {
        sstable_meta.Clear();
        sstables.clear();
        auto* sstable_pb = sstable_meta.add_sstables();
        sstable_pb->set_filesize(1000000);
        sstable_pb->set_filename("aaa.sst");
        sstable_pb->set_max_rss_rowid(0);
        for (int i = 0; i < N; i++) {
            sstable_pb = sstable_meta.add_sstables();
            sstable_pb->set_filesize(sub_size);
            sstable_pb->set_max_rss_rowid(i + 1);
        }
        LakePersistentIndex::pick_sstables_for_merge(sstable_meta, &sstables, &merge_base_level);
        if (is_base) {
            ASSERT_TRUE(merge_base_level);
            ASSERT_TRUE(sstables.size() == std::min(1 + N, (size_t)config::lake_pk_index_sst_max_compaction_versions));
            ASSERT_TRUE(sstables[0].filename() == "aaa.sst");
            for (int i = 1; i < N; i++) {
                ASSERT_TRUE(sstables[i].filesize() == sub_size);
            }
        } else {
            ASSERT_TRUE(!merge_base_level);
            ASSERT_TRUE(sstables.size() == std::min(N, (size_t)config::lake_pk_index_sst_max_compaction_versions));
            for (int i = 0; i < N; i++) {
                ASSERT_TRUE(sstables[i].filesize() == sub_size);
            }
        }
    };
    // 1. <1000000, 100>
    test_fn(100, 1, false);
    // 2. <1000000>
    test_fn(100, 0, false);
    // 3. <1000000, 10000, 10000, 10000, ...(9 items)>
    test_fn(10000, 9, false);
    // 4. <1000000, 10000, 10000, 10000, ...(10 items)>
    test_fn(10000, 10, true);
    // 4. <1000000, 10000, 10000, 10000, ...(11 items)>
    test_fn(10000, 11, true);
    int32_t old = config::lake_pk_index_sst_max_compaction_versions;
    config::lake_pk_index_sst_max_compaction_versions = 3;
    // 5. <1000000, 10000, 10000, 10000, ...(11 items)>
    test_fn(10000, 11, true);
    config::lake_pk_index_sst_max_compaction_versions = old;
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

TEST_F(LakePersistentIndexTest, test_compaction_strategy_same_max_rss_rowid) {
    // Test case for the fix: when base sstable's max_rss_rowid is same as cumulative sstable's max_rss_rowid,
    // we should force to do base merge instead of cumulative merge.

    PersistentIndexSstableMetaPB sstable_meta;
    std::vector<PersistentIndexSstablePB> sstables;
    bool merge_base_level = false;

    // Setup: create a scenario where cumulative merge would normally be preferred
    // but base and cumulative sstables have the same max_rss_rowid
    sstable_meta.Clear();
    sstables.clear();

    // Add base sstable (index 0) with large size
    auto* base_sstable = sstable_meta.add_sstables();
    base_sstable->set_filesize(1000000); // 1MB
    base_sstable->set_filename("base.sst");
    base_sstable->set_max_rss_rowid(100); // Same max_rss_rowid

    // Add cumulative sstables with small total size (would trigger cumulative merge normally)
    auto* cumulative_sstable = sstable_meta.add_sstables();
    cumulative_sstable->set_filesize(50000); // 50KB - much smaller than base
    cumulative_sstable->set_filename("cumulative1.sst");
    cumulative_sstable->set_max_rss_rowid(100); // Same max_rss_rowid as base

    // Without the fix, this would choose cumulative merge because:
    // base_level_bytes * ratio (1000000 * 0.1 = 100000) > cumulative_level_bytes (50000)
    // But with the fix, it should choose base merge due to same max_rss_rowid

    LakePersistentIndex::pick_sstables_for_merge(sstable_meta, &sstables, &merge_base_level);

    // Verify that base merge is chosen (merge_base_level = true)
    ASSERT_TRUE(merge_base_level) << "Should force base merge when max_rss_rowid is same";
    ASSERT_EQ(2, sstables.size()) << "Should include both base and cumulative sstables";
    ASSERT_EQ("base.sst", sstables[0].filename()) << "Base sstable should be first";
    ASSERT_EQ("cumulative1.sst", sstables[1].filename()) << "Cumulative sstable should be second";

    // Test the normal case where max_rss_rowid is different
    sstable_meta.Clear();
    sstables.clear();

    base_sstable = sstable_meta.add_sstables();
    base_sstable->set_filesize(1000000);
    base_sstable->set_filename("base2.sst");
    base_sstable->set_max_rss_rowid(100); // Different max_rss_rowid

    cumulative_sstable = sstable_meta.add_sstables();
    cumulative_sstable->set_filesize(50000);
    cumulative_sstable->set_filename("cumulative2.sst");
    cumulative_sstable->set_max_rss_rowid(200); // Different max_rss_rowid

    LakePersistentIndex::pick_sstables_for_merge(sstable_meta, &sstables, &merge_base_level);

    // This should choose cumulative merge since max_rss_rowid is different
    ASSERT_FALSE(merge_base_level) << "Should choose cumulative merge when max_rss_rowid is different";
    ASSERT_EQ(1, sstables.size()) << "Should only include cumulative sstables";
    ASSERT_EQ("cumulative2.sst", sstables[0].filename()) << "Only cumulative sstable should be included";

    // Test edge case: empty cumulative sstables
    sstable_meta.Clear();
    sstables.clear();

    base_sstable = sstable_meta.add_sstables();
    base_sstable->set_filesize(1000000);
    base_sstable->set_filename("base3.sst");
    base_sstable->set_max_rss_rowid(100);

    // No cumulative sstables added

    LakePersistentIndex::pick_sstables_for_merge(sstable_meta, &sstables, &merge_base_level);

    // Should choose base merge since there are no cumulative sstables
    ASSERT_TRUE(!merge_base_level) << "Should choose cumulative merge when no cumulative sstables exist";
    ASSERT_EQ(0, sstables.size()) << "Should be empty since no cumulative sstables exist";
}

TEST_F(LakePersistentIndexTest, test_major_compaction_with_predicate) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    auto lake_pk_index_cumulative_base_compaction_ratio = config::lake_pk_index_cumulative_base_compaction_ratio;
    SyncPoint::GetInstance()->SetCallBack("LakePersistentIndex::minor_compact:inject_predicate", [](void* arg) {
        PersistentIndexSstablePB* sstable_pb = (PersistentIndexSstablePB*)arg;
        auto sstable_predicate_pb = sstable_pb->mutable_predicate();
        auto record_predicate_pb = sstable_predicate_pb->mutable_record_predicate();

        record_predicate_pb->set_type(RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT);
        auto column_hash_is_congruent_pb = record_predicate_pb->mutable_column_hash_is_congruent();
        column_hash_is_congruent_pb->set_modulus(16);
        column_hash_is_congruent_pb->set_remainder(0);
        column_hash_is_congruent_pb->add_column_names("c0");
    });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([&]() {
        SyncPoint::GetInstance()->ClearCallBack("LakePersistentIndex::minor_compact:inject_predicate");
        SyncPoint::GetInstance()->DisableProcessing();
        config::lake_pk_index_cumulative_base_compaction_ratio = lake_pk_index_cumulative_base_compaction_ratio;
        config::l0_max_mem_usage = l0_max_mem_usage;
    });

    config::l0_max_mem_usage = 1024 * 1024 * 1024;
    using Key = int32_t;
    const int M = 5;
    const int N = 100;
    vector<Key> total_keys;
    vector<Slice> total_key_slices;
    vector<IndexValue> total_values;
    vector<size_t> idxes;
    vector<uint8_t> hits;
    total_key_slices.reserve(M * N);
    total_keys.reserve(M * N);
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    int k = 0;
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            int32_t cur_k = i * N + j;
            int32_t cur_v = j * 2;
            keys.emplace_back(cur_k);
            total_keys.emplace_back(cur_k);

            uint32_t hash = 0;
            auto key_column = Int32Column::create();
            key_column->append(keys[j]);
            key_column->crc32_hash(&(hash), 0, 1);
            hits.push_back(hash % 16 == 0);

            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            total_key_slices.emplace_back((uint8_t*)(&total_keys[k]), sizeof(Key));
            values.emplace_back(cur_v);
            total_values.emplace_back(cur_v);

            ++k;
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        // generate sst files.
        index->flush_memtable(true);
        ASSERT_OK(index->sync_flush_all_memtables(60 * 1000 * 1000)); // Wait up to 60s
    }
    ASSERT_TRUE(index->memory_usage() > 0);

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    // commit sst files
    ASSERT_OK(index->commit(&builder));

    vector<IndexValue> get_values = vector<IndexValue>(M * N, IndexValue(NullIndexValue));
    auto hit_count = SIMD::count_nonzero(hits.data(), hits.size());
    auto txn_log = std::make_shared<TxnLogPB>();
    // try to compact sst files.
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get()));
    ASSERT_TRUE(txn_log->op_compaction().input_sstables_size() == M);
    ASSERT_TRUE(txn_log->op_compaction().has_output_sstable() || hit_count == 0);
    ASSERT_OK(index->apply_opcompaction(txn_log->op_compaction()));
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));
    ASSERT_TRUE(hit_count < M * N);

    for (int i = 0; i < M * N; i++) {
        ASSERT_TRUE(!(total_values[i] == IndexValue(NullIndexValue)));
        if (hits[i]) {
            ASSERT_TRUE(!(get_values[i] == IndexValue(NullIndexValue)));
            ASSERT_EQ(total_values[i], get_values[i]);
        } else {
            ASSERT_EQ(IndexValue(NullIndexValue), get_values[i]);
        }
    }
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
        EXPECT_OK(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get());
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            return std::string(reinterpret_cast<const char*>(pk_column->raw_data()), pk_column->type_size());
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
        EXPECT_OK(PrimaryKeyEncoder::create_column(pkey_schema, &pk_column));
        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, 1, pk_column.get());
        if (pk_column->is_binary()) {
            return down_cast<BinaryColumn*>(pk_column.get())->get_slice(0).to_string();
        } else {
            return std::string(reinterpret_cast<const char*>(pk_column->raw_data()), pk_column->type_size());
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

} // namespace starrocks::lake
