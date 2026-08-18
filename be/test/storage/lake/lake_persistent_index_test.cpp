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

#include "common/config.h"
#include "fs/fs.h"
#include "storage/lake/meta_file.h"
#include "storage/sstable/block.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/format.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table_builder.h"
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
        index->minor_compact();
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
    if (!iter->Valid()) {
        // Empty sstable: on this branch upsert auto-flushes a full memtable, so the
        // test's explicit minor_compact() then flushes an empty memtable into an
        // empty sstable. There is no data block to corrupt in it; leave it intact
        // (it still participates in the compaction and in the cache-drop accounting).
        return;
    }
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
        // generate sst files (on branch-3.5 minor_compact() flushes the memtable to an
        // sstable synchronously and registers it before returning).
        ASSERT_OK(index->minor_compact());
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
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), *tablet_metadata_ptr, txn_log.get());

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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
        // generate sst files (on branch-3.5 minor_compact() flushes the memtable to an
        // sstable synchronously and registers it before returning).
        ASSERT_OK(index->minor_compact());
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
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), *tablet_metadata_ptr, txn_log.get());

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
    builder.Add(Slice("garbage_key"), Slice("\x00garbage", 8));
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
    ASSERT_OK(index->init(_tablet_metadata->sstable_meta()));
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
        // generate sst files (on branch-3.5 minor_compact() flushes the memtable to an
        // sstable synchronously and registers it before returning).
        ASSERT_OK(index->minor_compact());
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
    auto st = LakePersistentIndex::major_compact(_tablet_mgr.get(), *tablet_metadata_ptr, txn_log.get());

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

} // namespace starrocks::lake
