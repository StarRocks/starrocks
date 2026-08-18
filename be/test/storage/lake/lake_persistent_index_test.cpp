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

<<<<<<< HEAD
#include "storage/lake/meta_file.h"
#include "storage/record_predicate/column_hash_is_congruent.h"
=======
#include <algorithm>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/raw_data_visitor.h"
#include "column/runtime_type_traits.h"
#include "column/serde/column_array_serde.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_starlet_fwd.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/sstable/block.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
#include "storage_primitive/primary_key_encoder.h"
>>>>>>> 63f7162942 ([BugFix] Drop corrupted local cache when PK index SST compaction hits corruption (#77481))
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"

namespace starrocks::lake {

class LakePersistentIndexTest : public TestBase {
public:
    LakePersistentIndexTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
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

    std::unique_ptr<TabletMetadata> _tablet_metadata;
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
        // On branch-4.0 minor_compact() flushes the memtable to an sstable synchronously and
        // registers it before returning, so there is no pending async flush to wait for (unlike
        // main, where flush_memtable(true)+sync_flush_all_memtables() are needed). Assert the
        // status so a flush failure surfaces instead of leaving the compaction below with nothing
        // to merge.
        ASSERT_OK(index->minor_compact());
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
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), *tablet_metadata_ptr, txn_log.get()));
    ASSERT_TRUE(txn_log->op_compaction().input_sstables_size() > 0);
    ASSERT_TRUE(txn_log->op_compaction().has_output_sstable());
    ASSERT_OK(index->apply_opcompaction(txn_log->op_compaction()));
    ASSERT_OK(index->get(M * N, total_key_slices.data(), get_values.data()));
    for (int i = 0; i < M * N; i++) {
        ASSERT_EQ(total_values[i], get_values[i]);
    }
    config::l0_max_mem_usage = l0_max_mem_usage;
}

<<<<<<< HEAD
=======
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
// Overwrite the 1-byte compression-type trailer of the first data block with an
// invalid value, reproducing the production "Corruption: bad block type" failure.
// The block is located through the footer -> index block, so the injection is
// deterministic no matter whether block checksum verification is enabled (with
// checksums on, the same read fails as a checksum mismatch -- still Corruption).
static void corrupt_first_data_block_type_byte(const std::string& path) {
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_ABORT(auto file_size, rf->get_size());
    ASSERT_GT(file_size, sstable::Footer::kEncodedLength);
    std::string content(file_size, '\0');
    ASSERT_OK(rf->read_at_fully(0, content.data(), file_size));

    sstable::Footer footer;
    Slice footer_input(content.data() + file_size - sstable::Footer::kEncodedLength, sstable::Footer::kEncodedLength);
    ASSERT_OK(footer.DecodeFrom(&footer_input));
    sstable::BlockContents index_contents;
    index_contents.data = Slice(content.data() + footer.index_handle().offset(), footer.index_handle().size());
    index_contents.cachable = false;
    index_contents.heap_allocated = false;
    sstable::Block index_block(index_contents);
    std::unique_ptr<sstable::Iterator> iter(index_block.NewIterator(sstable::BytewiseComparator()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    Slice handle_value = iter->value();
    sstable::BlockHandle first_block;
    ASSERT_OK(first_block.DecodeFrom(&handle_value));
    // The compression-type byte sits right after the block payload.
    size_t type_offset = first_block.offset() + first_block.size();
    ASSERT_LT(type_offset, content.size());
    content[type_offset] = 0x7f;

    WritableFileOptions wf_opts;
    wf_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(wf_opts, path));
    ASSERT_OK(wf->append(Slice(content)));
    ASSERT_OK(wf->close());
}

// Regression test for compaction failing forever with "Corruption: bad block type":
// a corrupted data block in an input sstable (usually a bad local cache copy) must
// fail the merge as Corruption AND drop the input sstables' local cache, so the next
// scheduled compaction round re-reads from remote storage instead of hitting the
// same bad blocks again.
TEST_F(LakePersistentIndexTest, test_major_compaction_drops_corrupted_cache) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 5;
    const int N = 100;
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            values.emplace_back(j * 2);
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        // generate sst files.
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10 seconds timeout
    }

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    // commit sst files
    ASSERT_OK(index->commit(&builder));

    // Corrupt the first data block of every committed sstable (the index block and
    // footer near the file tail stay intact so opening still succeeds), so whichever
    // subset the merge picks hits the corruption.
    ASSERT_GT(tablet_metadata_ptr->sstable_meta().sstables_size(), 0);
    for (const auto& sst_pb : tablet_metadata_ptr->sstable_meta().sstables()) {
        corrupt_first_data_block_type_byte(_tablet_mgr->sst_location(tablet_id, sst_pb.filename()));
    }

    bool old_cfg = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;
    int drop_cnt = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache", [&](void*) { ++drop_cnt; });
    SyncPoint::GetInstance()->EnableProcessing();

    auto txn_log = std::make_shared<TxnLogPB>();
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get());

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old_cfg;

    ASSERT_TRUE(st.is_corruption()) << st;
    // The local cache of every picked input sstable must have been dropped.
    ASSERT_GT(txn_log->op_compaction().input_sstables_size(), 0);
    ASSERT_EQ(txn_log->op_compaction().input_sstables_size(), drop_cnt);
    config::l0_max_mem_usage = l0_max_mem_usage;
}

// Overwrite the 1-byte compression-type trailer of the index block, so Table::Open
// itself fails with Corruption before any data block is read.
static void corrupt_index_block_type_byte(const std::string& path) {
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_ABORT(auto file_size, rf->get_size());
    ASSERT_GT(file_size, sstable::Footer::kEncodedLength);
    std::string content(file_size, '\0');
    ASSERT_OK(rf->read_at_fully(0, content.data(), file_size));

    sstable::Footer footer;
    Slice footer_input(content.data() + file_size - sstable::Footer::kEncodedLength, sstable::Footer::kEncodedLength);
    ASSERT_OK(footer.DecodeFrom(&footer_input));
    // The compression-type byte sits right after the block payload.
    size_t type_offset = footer.index_handle().offset() + footer.index_handle().size();
    ASSERT_LT(type_offset, content.size());
    content[type_offset] = 0x7f;

    WritableFileOptions wf_opts;
    wf_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(wf_opts, path));
    ASSERT_OK(wf->append(Slice(content)));
    ASSERT_OK(wf->close());
}

// Same corruption scenario as above, but hit while OPENING an input sstable in the
// prepare phase (Table::Open reads the index block) instead of while merging. The
// error escapes through prepare_merging_iterator before the merging iterator or the
// caller's cleanup handler exists, so the prepare phase itself must drop the local
// cache of every picked input.
TEST_F(LakePersistentIndexTest, test_major_compaction_open_corruption_drops_cache) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 5;
    const int N = 100;
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            values.emplace_back(j * 2);
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        // generate sst files.
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10 seconds timeout
    }

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    // commit sst files
    ASSERT_OK(index->commit(&builder));

    // Corrupt the index block of every committed sstable so that opening the first
    // picked input already fails with Corruption.
    ASSERT_GT(tablet_metadata_ptr->sstable_meta().sstables_size(), 0);
    for (const auto& sst_pb : tablet_metadata_ptr->sstable_meta().sstables()) {
        corrupt_index_block_type_byte(_tablet_mgr->sst_location(tablet_id, sst_pb.filename()));
    }

    bool old_cfg = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;
    int drop_cnt = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache", [&](void*) { ++drop_cnt; });
    SyncPoint::GetInstance()->EnableProcessing();

    auto txn_log = std::make_shared<TxnLogPB>();
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get());

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old_cfg;

    ASSERT_TRUE(st.is_corruption()) << st;
    // prepare_merging_iterator records the full picked input set in txn_log before
    // opening anything, and the cleanup handler must drop every one of them. On top
    // of that, PersistentIndexSstable::init drops the failing file once on its own
    // (it drops and retries before giving up); without the caller-side handling that
    // single drop would be all we see.
    ASSERT_GT(txn_log->op_compaction().input_sstables_size(), 0);
    ASSERT_EQ(txn_log->op_compaction().input_sstables_size() + 1, drop_cnt);
    config::l0_max_mem_usage = l0_max_mem_usage;
}

// Replace the sstable at `path` with a freshly built, uncompressed sstable whose
// single entry carries value bytes that cannot be parsed as IndexValuesWithVerPB
// (0x00 is an invalid protobuf tag). Block structure, checksum and the
// compression-type byte are all valid, so reading the block succeeds and the
// corruption only surfaces when KeyValueMerger::merge parses the value. The new
// file size is returned through `new_size` so the caller can patch the sstable
// meta accordingly.
static void rewrite_sstable_with_garbage_value(const std::string& path, uint64_t* new_size) {
    WritableFileOptions wf_opts;
    wf_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(wf_opts, path));
    sstable::Options options;
    options.compression = sstable::kNoCompression;
    sstable::TableBuilder builder(options, wf.get());
    ASSERT_OK(builder.Add(Slice("garbage_key"), Slice("\x00garbage", 8)));
    ASSERT_OK(builder.Finish());
    *new_size = builder.FileSize();
    ASSERT_OK(wf->close());
}

// Regression test for corrupted value bytes that survive block reading: with
// checksum verification off on the compaction read path, garbage inside a value
// only fails when KeyValueMerger::merge parses it. That parse failure must be
// classified as Corruption so the input sstables' local cache still gets dropped.
TEST_F(LakePersistentIndexTest, test_major_compaction_value_parse_corruption_drops_cache) {
    auto l0_max_mem_usage = config::l0_max_mem_usage;
    config::l0_max_mem_usage = 10;
    using Key = uint64_t;
    const int M = 5;
    const int N = 100;
    auto tablet_id = _tablet_metadata->id();
    auto index = std::make_unique<LakePersistentIndex>(_tablet_mgr.get(), tablet_id);
    ASSERT_OK(index->init(_tablet_metadata));
    for (int i = 0; i < M; ++i) {
        vector<Key> keys;
        keys.reserve(N);
        vector<Slice> key_slices;
        key_slices.reserve(N);
        vector<IndexValue> values;
        values.reserve(N);
        for (int j = 0; j < N; j++) {
            keys.emplace_back(j);
            key_slices.emplace_back((uint8_t*)(&keys[j]), sizeof(Key));
            values.emplace_back(j * 2);
        }
        index->prepare(EditVersion(i, 0), 0);
        vector<IndexValue> upsert_old_values(keys.size());
        ASSERT_OK(index->upsert(N, key_slices.data(), values.data(), upsert_old_values.data()));
        // generate sst files.
        ASSERT_OK(index->flush_memtable(true));
        ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10 seconds timeout
    }

    Tablet tablet(_tablet_mgr.get(), tablet_id);
    auto tablet_metadata_ptr = std::make_shared<TabletMetadata>();
    tablet_metadata_ptr->CopyFrom(*_tablet_metadata);
    MetaFileBuilder builder(tablet, tablet_metadata_ptr);
    // commit sst files
    ASSERT_OK(index->commit(&builder));

    // Replace every committed sstable with one whose value bytes cannot be parsed,
    // patching the recorded file sizes so opening them still succeeds. The merge then
    // hits the parse failure on its very first key no matter which inputs are picked.
    ASSERT_GT(tablet_metadata_ptr->sstable_meta().sstables_size(), 0);
    for (auto& sst_pb : *tablet_metadata_ptr->mutable_sstable_meta()->mutable_sstables()) {
        uint64_t new_size = 0;
        rewrite_sstable_with_garbage_value(_tablet_mgr->sst_location(tablet_id, sst_pb.filename()), &new_size);
        ASSERT_GT(new_size, 0);
        sst_pb.set_filesize(new_size);
    }

    bool old_cfg = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;
    int drop_cnt = 0;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache", [&](void*) { ++drop_cnt; });
    SyncPoint::GetInstance()->EnableProcessing();

    auto txn_log = std::make_shared<TxnLogPB>();
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get());

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old_cfg;

    ASSERT_TRUE(st.is_corruption()) << st;
    // Opening the inputs succeeds, so the merge-phase handler must drop the local
    // cache of every picked input sstable.
    ASSERT_GT(txn_log->op_compaction().input_sstables_size(), 0);
    ASSERT_EQ(txn_log->op_compaction().input_sstables_size(), drop_cnt);
    config::l0_max_mem_usage = l0_max_mem_usage;
}
#endif // USE_STAROS && !BUILD_FORMAT_LIB

// Regression test for: publish failing with
//   "metadata is null when loading delvec from file"
// when apply_opcompaction opens a compaction output sstable that carries an
// embedded delvec (as preserved by the parallel-compaction passthrough/move
// path). apply_opcompaction must pass the tablet metadata so the delvec can be
// loaded -- exactly like LakePersistentIndex::init() does.
TEST_F(LakePersistentIndexTest, test_apply_opcompaction_output_sstable_with_delvec) {
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
    index->prepare(EditVersion(1, 0), 0);
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
    std::vector<uint32_t> deleted_row_ids = {1, 3, 5};
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
    ASSERT_OK(reloaded_index->apply_opcompaction(committed_metadata, txn_log.op_compaction()));
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
        // Wait for async flush to complete if any
        ASSERT_OK(index->sync_flush_all_memtables(10000000)); // 10 seconds timeout
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
    schema_pb->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

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
        index->prepare(EditVersion(batch, 0), 0);
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
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), tablet_metadata_ptr, txn_log.get()));
    ASSERT_TRUE(txn_log->op_compaction().has_output_sstable());

    const auto& out_sst = txn_log->op_compaction().output_sstable();
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

>>>>>>> 63f7162942 ([BugFix] Drop corrupted local cache when PK index SST compaction hits corruption (#77481))
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));

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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));

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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
        index->flush_memtable();
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
    ASSERT_OK(LakePersistentIndex::major_compact(_tablet_mgr.get(), *tablet_metadata_ptr, txn_log.get()));
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

// Helper: build a RowsetMetadataPB with given id, per-segment row counts, and an optional del file count.
// Row count is set as total of seg_rows. Since branch-4.0 does not have segment_metas,
// the proportional estimate path is always used.
static RowsetMetadataPB make_rowset(uint32_t id, const std::vector<int64_t>& seg_rows, int del_file_cnt = 0) {
    RowsetMetadataPB rowset;
    rowset.set_id(id);
    int64_t total_rows = 0;
    for (int64_t r : seg_rows) {
        rowset.add_segments("seg.dat");
        total_rows += r;
    }
    rowset.set_num_rows(total_rows);
    for (int i = 0; i < del_file_cnt; ++i) {
        rowset.add_del_files();
    }
    return rowset;
}

// Helper: build a RowsetMetadataPB with given segment count and total rows.
static RowsetMetadataPB make_rowset_no_meta(uint32_t id, int seg_cnt, int64_t total_rows) {
    RowsetMetadataPB rowset;
    rowset.set_id(id);
    for (int i = 0; i < seg_cnt; ++i) {
        rowset.add_segments("seg.dat");
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

    // Case 2: three rowsets, no SSTs → all segments need rebuild, proportional row count estimate.
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

    // Case 4: proportional estimate with multi-segment rowset.
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

    // Case 5: del files are counted in file_cnt but not in row_cnt.
    // Rowset id=0: 1 segment (100 rows) + 2 del files.
    {
        metadata.Clear();
        sstable_meta.Clear();
        *metadata.add_rowsets() = make_rowset(0, {100}, /*del_file_cnt=*/2);

        auto [file_cnt, row_cnt] = LakePersistentIndex::need_rebuild_counts(metadata, sstable_meta);
        EXPECT_EQ(file_cnt, 3u); // 1 segment + 2 del files
        EXPECT_EQ(row_cnt, 100);
    }
}

} // namespace starrocks::lake
