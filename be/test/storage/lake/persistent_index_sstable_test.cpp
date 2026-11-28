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

#include "storage/lake/persistent_index_sstable.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <ctime>
#include <map>
#include <set>
#include <string_view>

#include "base/container/lru_cache.h"
#include "base/debug/trace.h"
#include "base/debug/trace_metrics.h"
#include "base/phmap/btree.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/schema.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_starlet_fwd.h"
#include "fs/bundle_file.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "io/input_stream.h"
#include "io/seekable_input_stream.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/utils.h"
#include "storage/persistent_index.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"
#include "storage/sstable/table_builder.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/primary_key_encoder.h"
#include "types/date_value.h"
#include "types/datum.h"
#include "types/timestamp_value.h"

namespace starrocks::lake {

class PersistentIndexSstableTest : public ::testing::Test {
public:
    static void SetUpTestCase() { CHECK_OK(fs::create_directories(kTestDir)); }

    static void TearDownTestCase() { (void)fs::remove_all(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./persistent_index_sstable_test";
};

TEST_F(PersistentIndexSstableTest, test_generate_sst_scan_and_check) {
    const int N = 10000;
    sstable::Options options;
    const std::string filename = "test1.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (int i = 0; i < N; i++) {
        std::string str = fmt::format("test_key_{:016X}", i);
        IndexValue val(i);
        builder.Add(Slice(str), Slice(val.v, 8));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    // scan & check
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<sstable::Table> sstable;
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, sstable));
    sstable::ReadOptions read_options;
    int count = 0;
    sstable::Iterator* iter = sstable->NewIterator(read_options);
    for (iter->SeekToFirst(); iter->Valid() && iter->status().ok(); iter->Next()) {
        ASSERT_TRUE(iter->key().to_string() == fmt::format("test_key_{:016X}", count));
        IndexValue exp_val(count);
        IndexValue cur_val(UNALIGNED_LOAD64(iter->value().get_data()));
        ASSERT_TRUE(exp_val == cur_val);
        count++;
    }
    ASSERT_TRUE(count == N);
    delete iter;
}

TEST_F(PersistentIndexSstableTest, test_generate_sst_seek_and_check) {
    const int N = 10000;
    sstable::Options options;
    const std::string filename = "test2.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (int i = 0; i < N; i++) {
        std::string str = fmt::format("test_key_{:016X}", i);
        IndexValue val(i);
        builder.Add(Slice(str), Slice(val.v, 8));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    // seek & check
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<sstable::Table> sstable;
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, sstable));
    sstable::ReadOptions read_options;
    sstable::Iterator* iter = sstable->NewIterator(read_options);
    for (int i = 0; i < 100; i++) {
        int r = rand() % N;
        iter->Seek(fmt::format("test_key_{:016X}", r));
        ASSERT_TRUE(iter->Valid() && iter->status().ok());
        ASSERT_TRUE(iter->key().to_string() == fmt::format("test_key_{:016X}", r));
        IndexValue exp_val(r);
        IndexValue cur_val(UNALIGNED_LOAD64(iter->value().get_data()));
        ASSERT_TRUE(exp_val == cur_val);
    }
    delete iter;
}

TEST_F(PersistentIndexSstableTest, test_sample_data_keys_returns_complete_keys) {
    constexpr int kNumKeys = 10000;
    const std::string filename = "test_sample_data_keys.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> entries;
    for (int i = 0; i < kNumKeys; ++i) {
        entries.emplace(fmt::format("sample_key_{:016X}", i), std::make_pair(100, IndexValue(i)));
    }

    uint64_t file_size = 0;
    PersistentIndexSstableRangePB range;
    ASSERT_OK(PersistentIndexSstable::build_sstable(entries, file.get(), &file_size, &range));

    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(file_size);
    sstable_pb.mutable_range()->CopyFrom(range);
    auto sstable = std::make_unique<PersistentIndexSstable>();
    ASSERT_OK(sstable->init(std::move(read_file), sstable_pb, nullptr, false));

    std::vector<std::string> samples;
    // Empty bounds are unbounded on that side, i.e. sample the whole table.
    ASSERT_OK(sstable->sample_data_keys(&samples, Slice(), Slice(), 1));
    ASSERT_FALSE(samples.empty());
    for (const auto& sample : samples) {
        EXPECT_NE(entries.end(), entries.find(sample)) << "sampled an index separator rather than a data key";
    }
}

TEST_F(PersistentIndexSstableTest, test_merge) {
    std::vector<sstable::Iterator*> list;
    std::vector<std::unique_ptr<RandomAccessFile>> read_files;
    std::vector<uint64_t> fileszs;
    fileszs.resize(3);
    read_files.resize(3);
    const int N = 10000;
    for (int i = 0; i < 3; ++i) {
        sstable::Options options;
        const std::string filename = fmt::format("test_merge_{}.sst", i);
        ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
        sstable::TableBuilder builder(options, file.get());
        for (int j = 0; j < N; j++) {
            std::string str = fmt::format("test_key_{:016X}", j);
            IndexValue val(j * i);
            builder.Add(Slice(str), Slice(val.v, 8));
        }
        CHECK_OK(builder.Finish());
        uint64_t filesz = builder.FileSize();
        fileszs[i] = filesz;
        ASSIGN_OR_ABORT(read_files[i], fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    }
    sstable::Options options;
    sstable::ReadOptions read_options;
    std::vector<std::unique_ptr<sstable::Table>> sstable_ptrs(3);
    for (int i = 0; i < 3; ++i) {
        std::unique_ptr<sstable::Table> sstable;
        CHECK_OK(sstable::Table::Open(options, read_files[i].get(), fileszs[i], sstable));
        sstable::Iterator* iter = sstable->NewIterator(read_options);
        list.emplace_back(iter);
        sstable_ptrs[i] = std::move(sstable);
    }

    phmap::btree_map<std::string, std::string> map;
    {
        sstable::Options options;
        sstable::Iterator* iter = sstable::NewMergingIterator(options.comparator, &list[0], list.size());

        iter->SeekToFirst();
        while (iter->Valid()) {
            auto key = iter->key().to_string();
            auto it = map.find(key);
            if (it == map.end()) {
                map[key] = iter->value().to_string();
            } else {
                auto val = UNALIGNED_LOAD64(it->second.c_str());
                auto cur_val = UNALIGNED_LOAD64(iter->value().get_data());
                if (cur_val > val) {
                    it->second = iter->value().to_string();
                }
            }
            iter->Next();
        }
        delete iter;
    }

    ASSERT_EQ(N, map.size());
    const std::string filename = "test_merge_4.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (auto& [k, v] : map) {
        builder.Add(Slice(k), Slice(v));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<sstable::Table> sstable;
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, sstable));
    sstable::Iterator* iter = sstable->NewIterator(read_options);
    for (int i = 0; i < 100; i++) {
        int r = rand() % N;
        iter->Seek(fmt::format("test_key_{:016X}", r));
        ASSERT_TRUE(iter->Valid() && iter->status().ok());
        ASSERT_TRUE(iter->key().to_string() == fmt::format("test_key_{:016X}", r));
        auto exp_val = uint64_t(r);
        auto cur_val = UNALIGNED_LOAD64(iter->value().get_data());
        ASSERT_TRUE(2 * exp_val == cur_val);
    }
    list.clear();
    read_files.clear();
    delete iter;
    sstable_ptrs.clear();
}

TEST_F(PersistentIndexSstableTest, test_empty_iterator) {
    std::unique_ptr<sstable::Iterator> iter;
    iter.reset(sstable::NewEmptyIterator());
    ASSERT_TRUE(!iter->Valid());
    iter->Seek({});
    iter->SeekToFirst();
    iter->SeekToLast();
    CHECK_OK(iter->status());
    std::unique_ptr<sstable::Iterator> iter2;
    iter2.reset(sstable::NewErrorIterator(Status::NotFound("")));
    ASSERT_ERROR(iter2->status());
}

TEST_F(PersistentIndexSstableTest, test_persistent_index_sstable) {
    const int N = 100;
    // 1. build sstable
    const std::string filename = "test_persistent_index_sstable_1.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < N; i++) {
        map.emplace(fmt::format("test_key_{:016X}", i), std::make_pair(100, IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize, &range_pb));
    // 2. open sstable
    std::unique_ptr<PersistentIndexSstable> sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(100));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));
    // check memory usage
    ASSERT_TRUE(sst->memory_usage() > 0);

    {
        // 3. multi get with version (all keys included)
        std::vector<std::string> keys_str(N / 2);
        std::vector<Slice> keys(N / 2);
        std::vector<IndexValue> values(N / 2, IndexValue(NullIndexValue));
        std::vector<IndexValue> expected_values(N / 2);
        KeyIndexSet key_indexes_info;
        KeyIndexSet found_keys_info;
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % N;
            keys_str[i] = fmt::format("test_key_{:016X}", r);
            keys[i] = Slice(keys_str[i]);
            expected_values[i] = r;
            key_indexes_info.insert(i);
        }
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, 100, values.data(), &found_keys_info));
        ASSERT_EQ(key_indexes_info, found_keys_info);
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(expected_values[i], values[i]);
        }
    }
    {
        // 4. multi get without version (all keys included)
        std::vector<std::string> keys_str(N / 2);
        std::vector<Slice> keys(N / 2);
        std::vector<IndexValue> values(N / 2, IndexValue(NullIndexValue));
        std::vector<IndexValue> expected_values(N / 2);
        KeyIndexSet key_indexes_info;
        KeyIndexSet found_keys_info;
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % N;
            keys_str[i] = fmt::format("test_key_{:016X}", r);
            keys[i] = Slice(keys_str[i]);
            expected_values[i] = r;
            key_indexes_info.insert(i);
        }
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, -1, values.data(), &found_keys_info));
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(expected_values[i], values[i]);
        }
        ASSERT_EQ(key_indexes_info, found_keys_info);

        found_keys_info.clear();
        key_indexes_info.clear();
        for (int i = N / 4; i < N / 2; ++i) {
            key_indexes_info.insert(i);
        }
        std::vector<IndexValue> values1(N / 2, IndexValue(NullIndexValue));
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, -1, values1.data(), &found_keys_info));
        for (int i = N / 4; i < N / 2; i++) {
            ASSERT_EQ(expected_values[i], values1[i]);
        }
        ASSERT_EQ(key_indexes_info, found_keys_info);
    }
    {
        // 5. multi get with version (all keys included)
        std::vector<std::string> keys_str(N / 2);
        std::vector<Slice> keys(N / 2);
        std::vector<IndexValue> values(N / 2, IndexValue(NullIndexValue));
        std::vector<IndexValue> expected_values(N / 2);
        KeyIndexSet key_indexes_info;
        KeyIndexSet found_keys_info;
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % N;
            keys_str[i] = fmt::format("test_key_{:016X}", r);
            keys[i] = Slice(keys_str[i]);
            expected_values[i] = r;
            key_indexes_info.insert(i);
        }
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, 99, values.data(), &found_keys_info));
        ASSERT_TRUE(found_keys_info.empty());
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(NullIndexValue, values[i].get_value());
        }
    }
    {
        // 6. multi get with version (some keys included)
        std::vector<std::string> keys_str(N / 2);
        std::vector<Slice> keys(N / 2);
        std::vector<IndexValue> values(N / 2, IndexValue(NullIndexValue));
        std::vector<IndexValue> expected_values(N / 2);
        KeyIndexSet key_indexes_info;
        KeyIndexSet found_keys_info;
        int expected_found_cnt = 0;
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % (N * 2);
            keys_str[i] = fmt::format("test_key_{:016X}", r);
            keys[i] = Slice(keys_str[i]);
            if (r < N) {
                expected_values[i] = r;
                expected_found_cnt++;
            } else {
                expected_values[i] = IndexValue(NullIndexValue);
            }
            key_indexes_info.insert(i);
        }
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, 100, values.data(), &found_keys_info));
        ASSERT_EQ(expected_found_cnt, found_keys_info.size());
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(expected_values[i], values[i]);
        }
    }
    {
        // 7. multi get without version (some keys included)
        std::vector<std::string> keys_str(N / 2);
        std::vector<Slice> keys(N / 2);
        std::vector<IndexValue> values(N / 2, IndexValue(NullIndexValue));
        std::vector<IndexValue> expected_values(N / 2);
        KeyIndexSet key_indexes_info;
        KeyIndexSet found_keys_info;
        int expected_found_cnt = 0;
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % (N * 2);
            keys_str[i] = fmt::format("test_key_{:016X}", r);
            keys[i] = Slice(keys_str[i]);
            if (r < N) {
                expected_values[i] = r;
                expected_found_cnt++;
            } else {
                expected_values[i] = IndexValue(NullIndexValue);
            }
            key_indexes_info.insert(i);
        }
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes_info, -1, values.data(), &found_keys_info));
        ASSERT_EQ(expected_found_cnt, found_keys_info.size());
        for (int i = 0; i < N / 2; i++) {
            ASSERT_EQ(expected_values[i], values[i]);
        }
    }
    // 8. iterate sstable
    {
        sstable::ReadIOStat stat;
        sstable::ReadOptions options;
        options.stat = &stat;
        sstable::Iterator* iter = sst->new_iterator(options);
        iter->SeekToFirst();
        int i = 0;
        for (; iter->Valid(); iter->Next()) {
            ASSERT_EQ(iter->key().to_string(), fmt::format("test_key_{:016X}", i));
            IndexValuesWithVerPB index_value_with_ver_pb;
            ASSERT_TRUE(index_value_with_ver_pb.ParseFromArray(iter->value().data, iter->value().size));
            ASSERT_EQ(index_value_with_ver_pb.values(0).version(), 100);
            ASSERT_EQ(index_value_with_ver_pb.values(0).rowid(), i);
            i++;
        }
        ASSERT_OK(iter->status());
        delete iter;
    }
    {
        sstable::ReadIOStat stat;
        sstable::ReadOptions options;
        options.stat = &stat;
        sstable::Iterator* iter = sst->new_iterator(options);
        iter->SeekToLast();
        int i = N - 1;
        for (; iter->Valid(); iter->Prev()) {
            ASSERT_EQ(iter->key().to_string(), fmt::format("test_key_{:016X}", i));
            IndexValuesWithVerPB index_value_with_ver_pb;
            ASSERT_TRUE(index_value_with_ver_pb.ParseFromArray(iter->value().data, iter->value().size));
            ASSERT_EQ(index_value_with_ver_pb.values(0).version(), 100);
            ASSERT_EQ(index_value_with_ver_pb.values(0).rowid(), i);
            i--;
        }
        ASSERT_OK(iter->status());
        delete iter;
    }
    // 9. iterate seek test
    {
        sstable::ReadIOStat stat;
        sstable::ReadOptions options;
        options.stat = &stat;
        sstable::Iterator* iter = sst->new_iterator(options);
        for (int i = 0; i < N / 2; i++) {
            int r = rand() % (N * 2);
            iter->SeekToFirst();
            iter->Seek(fmt::format("test_key_{:016X}", r));
            if (r < N) {
                ASSERT_EQ(iter->key().to_string(), fmt::format("test_key_{:016X}", r));
                IndexValuesWithVerPB index_value_with_ver_pb;
                ASSERT_TRUE(index_value_with_ver_pb.ParseFromArray(iter->value().data, iter->value().size));
                ASSERT_EQ(index_value_with_ver_pb.values(0).version(), 100);
                ASSERT_EQ(index_value_with_ver_pb.values(0).rowid(), r);
            } else {
                ASSERT_FALSE(iter->Valid());
            }
        }
        delete iter;
    }
}

TEST_F(PersistentIndexSstableTest, test_index_value_protobuf) {
    IndexValuesWithVerPB index_value_pb;
    for (int i = 0; i < 10; i++) {
        auto* value = index_value_pb.add_values();
        value->set_version(i);
        value->set_rssid(i * 10 + i);
        value->set_rowid(i * 20 + i);
    }
    for (int i = 0; i < 10; i++) {
        const auto& value = index_value_pb.values(i);
        ASSERT_EQ(value.version(), i);
        IndexValue val = build_index_value(value);
        ASSERT_TRUE(val == IndexValue(((uint64_t)(i * 10 + i) << 32) | (i * 20 + i)));
    }
}

TEST_F(PersistentIndexSstableTest, test_ioerror_inject) {
    const int N = 10000;
    sstable::Options options;
    const std::string filename = "test_ioerror_inject.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (int i = 0; i < N; i++) {
        std::string str = fmt::format("test_key_{:016X}", i);
        IndexValue val(i);
        builder.Add(Slice(str), Slice(val.v, 8));
    }
    SyncPoint::GetInstance()->SetCallBack("table_builder_footer_error",
                                          [&](void* arg) { *(Status*)arg = Status::IOError("ut_test"); });
    SyncPoint::GetInstance()->EnableProcessing();
    auto st = builder.Finish();
    SyncPoint::GetInstance()->ClearCallBack("table_builder_footer_error");
    SyncPoint::GetInstance()->DisableProcessing();
    uint64_t filesz = builder.FileSize();
    if (st.ok()) {
        // scan & check
        ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
        std::unique_ptr<sstable::Table> sstable;
        CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, sstable));
        sstable::ReadOptions read_options;
        sstable::Iterator* iter = sstable->NewIterator(read_options);
        for (iter->SeekToFirst(); iter->Valid() && iter->status().ok(); iter->Next()) {
        }
        ASSERT_TRUE(iter->status().ok());
        delete iter;
    }
}

TEST_F(PersistentIndexSstableTest, test_persistent_index_sstable_stream_builder) {
    const int N = 1000;
    const std::string filename = "test_stream_builder.sst";
    const std::string encryption_meta = "";

    // 1. Create stream builder and add keys
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    auto builder = std::make_unique<PersistentIndexSstableStreamBuilder>(std::move(file), encryption_meta);

    // Test initial state
    ASSERT_EQ(0, builder->num_entries());
    ASSERT_FALSE(builder->file_path().empty());

    // Add keys in order
    std::vector<std::string> keys;
    for (int i = 0; i < N; i++) {
        std::string key = fmt::format("test_key_{:016X}", i);
        keys.push_back(key);
        ASSERT_OK(builder->add(Slice(key)));
    }

    // Check state after adding keys
    ASSERT_EQ(N, builder->num_entries());

    // 2. Finish building
    uint64_t file_size = 0;
    ASSERT_OK(builder->finish(&file_size));
    ASSERT_GT(file_size, 0);
    ASSERT_EQ(N, builder->num_entries());

    // Test file_info
    FileInfo info = builder->file_info();
    ASSERT_EQ(filename, info.path);
    ASSERT_EQ(file_size, info.size);
    ASSERT_EQ(encryption_meta, info.encryption_meta);

    // Test that adding after finish fails
    ASSERT_ERROR(builder->add(Slice("should_fail")));
    ASSERT_ERROR(builder->finish());

    // 3. Read back and verify the sstable
    std::unique_ptr<PersistentIndexSstable> sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(100));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(file_size);
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));

    // 4. Iterate and verify all keys
    sstable::ReadIOStat stat;
    sstable::ReadOptions options;
    options.stat = &stat;
    sstable::Iterator* iter = sst->new_iterator(options);

    iter->SeekToFirst();
    int count = 0;
    for (; iter->Valid() && iter->status().ok(); iter->Next()) {
        ASSERT_EQ(iter->key().to_string(), keys[count]);

        // Parse and verify the value
        IndexValuesWithVerPB index_value_with_ver_pb;
        ASSERT_TRUE(index_value_with_ver_pb.ParseFromArray(iter->value().data, iter->value().size));
        ASSERT_EQ(1, index_value_with_ver_pb.values_size());
        ASSERT_EQ(count, index_value_with_ver_pb.values(0).rowid());
        count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(N, count);
    delete iter;

    // 5. Test seeking to specific keys
    iter = sst->new_iterator(options);
    for (int i = 0; i < 10; i++) {
        int r = rand() % N;
        iter->Seek(keys[r]);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key().to_string(), keys[r]);

        IndexValuesWithVerPB index_value_with_ver_pb;
        ASSERT_TRUE(index_value_with_ver_pb.ParseFromArray(iter->value().data, iter->value().size));
        ASSERT_EQ(r, index_value_with_ver_pb.values(0).rowid());
    }
    delete iter;
}

TEST_F(PersistentIndexSstableTest, test_stream_builder_error_handling) {
    const std::string filename = "test_stream_builder_error.sst";
    const std::string encryption_meta = "";

    // Test with invalid file (simulate error)
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    auto builder = std::make_unique<PersistentIndexSstableStreamBuilder>(std::move(file), encryption_meta);

    // Add some keys
    ASSERT_OK(builder->add(Slice("key1")));
    ASSERT_OK(builder->add(Slice("key2")));

    // Test double finish
    uint64_t file_size = 0;
    ASSERT_OK(builder->finish(&file_size));
    ASSERT_GT(file_size, 0);

    // Second finish should fail
    ASSERT_ERROR(builder->finish(&file_size));

    // Adding after finish should fail
    ASSERT_ERROR(builder->add(Slice("key3")));
}

TEST_F(PersistentIndexSstableTest, test_table_builder_out_of_order_keys) {
    sstable::Options options;
    const std::string filename = "test_out_of_order.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());

    // Add first key
    std::string key1 = "test_key_0000000000000002";
    IndexValue val1(2);
    ASSERT_OK(builder.Add(Slice(key1), Slice(val1.v, 8)));

    // Try to add a key that is lexicographically smaller than the previous key
    // This should fail due to out-of-order constraint
    std::string key2 = "test_key_0000000000000001";
    IndexValue val2(1);
    ASSERT_ERROR(builder.Add(Slice(key2), Slice(val2.v, 8)));

    // Add another key in correct order should work
    std::string key3 = "test_key_0000000000000003";
    IndexValue val3(3);
    ASSERT_OK(builder.Add(Slice(key3), Slice(val3.v, 8)));

    // Try to add same key again - should fail
    ASSERT_ERROR(builder.Add(Slice(key3), Slice(val3.v, 8)));

    // Add key with same prefix but lexicographically smaller - should fail
    std::string key4 = "test_key_0000000000000002A";
    IndexValue val4(4);
    ASSERT_ERROR(builder.Add(Slice(key4), Slice(val4.v, 8)));

    // Builder should still be able to finish properly after error
    builder.Abandon();
}

// Helper: build a small valid SST file and return its filesize
static void build_test_sst(const std::string& path, uint64_t* filesize) {
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < 10; i++) {
        map.emplace(fmt::format("key_{:04d}", i), std::make_pair(int64_t(1), IndexValue(i)));
    }
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(path));
    PersistentIndexSstableRangePB range_pb;
    CHECK_OK(PersistentIndexSstable::build_sstable(map, wf.get(), filesize, &range_pb));
    CHECK_OK(wf->close());
}

TEST_F(PersistentIndexSstableTest, test_metric_build_sstable_write_error) {
    const std::string path = lake::join_path(kTestDir, "metric_write_error.sst");
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(path));

    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    map.emplace("key_a", std::make_pair(int64_t(1), IndexValue(1)));

    auto before = StorageMetrics::instance()->pk_index_sst_write_error_total.value();

    SyncPoint::GetInstance()->SetCallBack("table_builder_footer_error",
                                          [](void* arg) { *(Status*)arg = Status::IOError("inject_write_error"); });
    SyncPoint::GetInstance()->EnableProcessing();
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    auto st = PersistentIndexSstable::build_sstable(map, wf.get(), &filesize, &range_pb);
    SyncPoint::GetInstance()->ClearCallBack("table_builder_footer_error");
    SyncPoint::GetInstance()->DisableProcessing();

    ASSERT_ERROR(st);
    ASSERT_EQ(before + 1, StorageMetrics::instance()->pk_index_sst_write_error_total.value());
}

TEST_F(PersistentIndexSstableTest, test_metric_sst_open_read_error) {
    const std::string path = lake::join_path(kTestDir, "metric_open_error.sst");
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);

    auto before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();

    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::init:table_open_error",
                                          [](void* arg) { *(Status*)arg = Status::IOError("inject_open_error"); });
    SyncPoint::GetInstance()->EnableProcessing();

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename("metric_open_error.sst");
    sstable_pb.set_filesize(filesize);
    auto st = sst->init(std::move(rf), sstable_pb, nullptr);

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
    SyncPoint::GetInstance()->DisableProcessing();

    ASSERT_ERROR(st);
    ASSERT_EQ(before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
TEST_F(PersistentIndexSstableTest, test_sst_open_retry_after_clear_corrupted_cache) {
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDir);
    auto metadata = std::make_shared<TabletMetadata>();
    TabletManager tablet_mgr(location_provider, 1024, 1024);
    metadata->set_id(10001);
    ASSERT_OK(fs::create_directories(lake::join_path(kTestDir, lake::kSegmentDirectoryName)));

    const std::string filename = "open_retry_corrupted_cache.sst";
    const std::string path = tablet_mgr.sst_location(metadata->id(), filename);
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);

    bool old = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;

    bool injected = false;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::init:table_open_error", [&](void* arg) {
        if (!injected) {
            injected = true;
            *(Status*)arg = Status::Corruption("inject_open_corruption");
        }
    });
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    std::vector<FileInfo> reopened;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::open_sstable_file",
                                          [&](void* arg) { reopened.push_back(*static_cast<FileInfo*>(arg)); });
    SyncPoint::GetInstance()->EnableProcessing();

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    auto st = sst->init(std::move(rf), sstable_pb, nullptr, true, nullptr, metadata, &tablet_mgr);

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::open_sstable_file");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old;

    ASSERT_OK(st);
    // The file opened again after the cache drop carries the recorded size too.
    ASSERT_EQ(1, reopened.size());
    ASSERT_EQ(path, reopened[0].path);
    ASSERT_EQ(static_cast<int64_t>(filesize), reopened[0].size.value_or(-1));
}
#endif // USE_STAROS && !BUILD_FORMAT_LIB

// Every open of a PK index sstable hands the size recorded in the metadata to the filesystem. Without
// it a shared-data read looks the size up, which is a synchronous HEAD request to the object storage
// when the star cache does not hold the size; multi_get opens the file once per call.
TEST_F(PersistentIndexSstableTest, test_open_sstable_file_passes_recorded_size) {
    const std::string filename = "open_passes_recorded_size.sst";
    const std::string path = lake::join_path(kTestDir, filename);
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);
    ASSERT_GT(filesize, 0);

    std::vector<FileInfo> opened;
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::open_sstable_file",
                                          [&](void* arg) { opened.push_back(*static_cast<FileInfo*>(arg)); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::open_sstable_file");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    ASSIGN_OR_ABORT(auto sst, PersistentIndexSstable::new_sstable(sstable_pb, path, cache.get()));

    std::string key_str = fmt::format("key_{:04d}", 3);
    Slice key(key_str);
    KeyIndexSet key_indexes{0};
    std::vector<IndexValue> values(1, IndexValue(NullIndexValue));
    KeyIndexSet found;
    ASSERT_OK(sst->multi_get(&key, key_indexes, -1, values.data(), &found));
    ASSERT_EQ(1, found.size());
    ASSERT_EQ(IndexValue(3), values[0]);

    // One open by new_sstable and one by multi_get, both with the recorded size.
    ASSERT_EQ(2, opened.size());
    for (const auto& info : opened) {
        ASSERT_EQ(path, info.path);
        ASSERT_EQ(static_cast<int64_t>(filesize), info.size.value_or(-1));
    }

    // A size that is not positive is unknown: the file is opened without one and reads as before.
    ASSIGN_OR_ABORT(auto rf, PersistentIndexSstable::open_sstable_file(RandomAccessFileOptions(), path, 0));
    ASSERT_EQ(3, opened.size());
    ASSERT_FALSE(opened.back().size.has_value());
    ASSIGN_OR_ABORT(auto size, rf->get_size());
    ASSERT_EQ(static_cast<int64_t>(filesize), size);
}

TEST_F(PersistentIndexSstableTest, test_metric_sst_multiget_read_error) {
    const std::string path = lake::join_path(kTestDir, "metric_multiget_error.sst");
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename("metric_multiget_error.sst");
    sstable_pb.set_filesize(filesize);
    ASSERT_OK(sst->init(std::move(rf), sstable_pb, cache.get()));

    auto before = StorageMetrics::instance()->pk_index_sst_read_error_total.value();

    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::multi_get:error",
                                          [](void* arg) { *(Status*)arg = Status::IOError("inject_multiget_error"); });
    SyncPoint::GetInstance()->EnableProcessing();

    std::string key_str = fmt::format("key_{:04d}", 0);
    Slice key(key_str);
    KeyIndexSet key_indexes{0};
    std::vector<IndexValue> values(1, IndexValue(NullIndexValue));
    KeyIndexSet found;
    auto st = sst->multi_get(&key, key_indexes, -1, values.data(), &found);

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::multi_get:error");
    SyncPoint::GetInstance()->DisableProcessing();

    ASSERT_ERROR(st);
    ASSERT_EQ(before + 1, StorageMetrics::instance()->pk_index_sst_read_error_total.value());
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
TEST_F(PersistentIndexSstableTest, test_multiget_retry_after_clear_corrupted_cache) {
    const std::string path = lake::join_path(kTestDir, "multiget_retry_corrupted_cache.sst");
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename("multiget_retry_corrupted_cache.sst");
    sstable_pb.set_filesize(filesize);
    ASSERT_OK(sst->init(std::move(rf), sstable_pb, cache.get()));

    bool old = config::lake_clear_corrupted_cache_data;
    config::lake_clear_corrupted_cache_data = true;

    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::multi_get:error",
                                          [](void* arg) { *(Status*)arg = Status::Corruption("inject_corruption"); });
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::drop_corrupted_cache",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->EnableProcessing();

    std::string key_str = fmt::format("key_{:04d}", 0);
    Slice key(key_str);
    KeyIndexSet key_indexes{0};
    std::vector<IndexValue> values(1, IndexValue(NullIndexValue));
    KeyIndexSet found;
    auto st = sst->multi_get(&key, key_indexes, -1, values.data(), &found);

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::multi_get:error");
    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old;

    ASSERT_OK(st);
    ASSERT_EQ(1, found.size());
    ASSERT_TRUE(found.contains(0));
    ASSERT_EQ(IndexValue(0), values[0]);
}
#endif // USE_STAROS && !BUILD_FORMAT_LIB

// Flip a few bytes inside the first data block (data blocks start at file offset 0),
// keeping the file tail (filter/index blocks and footer) intact so Table::Open still
// succeeds and the corruption is only hit when the data block itself is read.
static void corrupt_sst_data_block(const std::string& path) {
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_ABORT(auto file_size, rf->get_size());
    std::string content(file_size, '\0');
    ASSERT_OK(rf->read_at_fully(0, content.data(), file_size));
    for (size_t i = 16; i < 24 && i < content.size(); i++) {
        content[i] ^= 0xff;
    }
    WritableFileOptions opts;
    opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(opts, path));
    ASSERT_OK(wf->append(Slice(content)));
    ASSERT_OK(wf->close());
}

// A data block whose bytes were tampered with must surface as Corruption via the block
// checksum, not be misparsed (the "bad block type" failure mode) or silently return
// wrong index values.
TEST_F(PersistentIndexSstableTest, test_multiget_detects_corrupted_data_block) {
    bool old_verify = config::lake_pk_index_sst_verify_checksum;
    config::lake_pk_index_sst_verify_checksum = true;
    const std::string path = lake::join_path(kTestDir, "multiget_corrupted_block.sst");
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);
    corrupt_sst_data_block(path);

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename("multiget_corrupted_block.sst");
    sstable_pb.set_filesize(filesize);
    ASSERT_OK(sst->init(std::move(rf), sstable_pb, nullptr));

    std::string key_str = fmt::format("key_{:04d}", 0);
    Slice key(key_str);
    KeyIndexSet key_indexes{0};
    std::vector<IndexValue> values(1, IndexValue(NullIndexValue));
    KeyIndexSet found;
    auto st = sst->multi_get(&key, key_indexes, -1, values.data(), &found);
    ASSERT_TRUE(st.is_corruption()) << st;
    config::lake_pk_index_sst_verify_checksum = old_verify;
}

// The tablet-split sampling path (sample_data_keys) seeks real data blocks to fetch
// the sampled keys; a tampered data block must fail as Corruption instead of feeding
// wrong keys into the split points. The first separator always maps to the first data
// block (sample_keys_in_range emits index 0 unconditionally), which is the block
// corrupt_sst_data_block tampers with, so the failure is deterministic.
TEST_F(PersistentIndexSstableTest, test_sample_data_keys_detects_corrupted_data_block) {
    bool old_verify = config::lake_pk_index_sst_verify_checksum;
    config::lake_pk_index_sst_verify_checksum = true;
    const std::string path = lake::join_path(kTestDir, "sample_corrupted_block.sst");
    uint64_t filesize = 0;
    build_test_sst(path, &filesize);
    corrupt_sst_data_block(path);

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename("sample_corrupted_block.sst");
    sstable_pb.set_filesize(filesize);
    ASSERT_OK(sst->init(std::move(rf), sstable_pb, nullptr));

    std::vector<std::string> keys;
    auto st = sst->sample_data_keys(&keys, Slice(), Slice(), /*max_samples=*/4);
    ASSERT_TRUE(st.is_corruption()) << st;
    config::lake_pk_index_sst_verify_checksum = old_verify;
}

// Tombstones are stored as (rssid=UINT32_MAX, rowid=UINT32_MAX) so that the
// 64-bit packed value equals NullIndexValue on the way out. When the owning
// sstable has a non-zero rssid_offset (child-tablet contribution after a
// tablet merge), multi_get used to unconditionally add that offset to the
// entry's rssid, wrapping UINT32_MAX to a small valid-looking rssid — the
// caller then mistook the tombstone for a live pointer, pushed
// rowid=UINT32_MAX into the publish delvec for N deleted keys, and tripped
// the `cur_old + cur_add != cur_new` consistency check (1000 adds collapse
// to 1 entry inside the Roaring bitmap).
TEST_F(PersistentIndexSstableTest, test_multi_get_preserves_tombstones_with_rssid_offset) {
    const int kNumTombstones = 16;
    const int32_t kOffset = 11;

    const std::string filename = "test_tombstones_rssid_offset.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kNumTombstones; i++) {
        // IndexValue(NullIndexValue) packs to (rssid=UINT32_MAX, rowid=UINT32_MAX).
        map.emplace(fmt::format("tomb_{:016X}", i), std::make_pair(100, IndexValue(NullIndexValue)));
    }

    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize, &range_pb));

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    sstable_pb.set_rssid_offset(kOffset);
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));

    std::vector<std::string> key_str(kNumTombstones);
    std::vector<Slice> keys(kNumTombstones);
    std::vector<IndexValue> values(kNumTombstones, IndexValue(NullIndexValue));
    KeyIndexSet key_indexes;
    KeyIndexSet found;
    for (int i = 0; i < kNumTombstones; i++) {
        key_str[i] = fmt::format("tomb_{:016X}", i);
        keys[i] = Slice(key_str[i]);
        key_indexes.insert(i);
    }
    ASSERT_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));

    // Every tombstone must come back as NullIndexValue, NOT as
    // (rssid=UINT32_MAX+kOffset wrap, rowid=UINT32_MAX) which would be a
    // small valid-looking pointer (upsert would treat the key as live).
    ASSERT_EQ(found.size(), static_cast<size_t>(kNumTombstones));
    for (int i = 0; i < kNumTombstones; i++) {
        ASSERT_EQ(NullIndexValue, values[i].get_value())
                << "tombstone at key " << key_str[i] << " leaked non-null value 0x" << std::hex << values[i].get_value()
                << " rssid=" << values[i].get_rssid() << " rowid=" << values[i].get_rowid();
    }
}

// Same invariant when shared_version / shared_rssid is used instead of
// rssid_offset. A tombstone must not have its rssid overwritten with
// shared_rssid — otherwise the caller sees a small live-looking pointer.
TEST_F(PersistentIndexSstableTest, test_multi_get_preserves_tombstones_with_shared_rssid) {
    const int kNumTombstones = 8;
    const uint32_t kSharedRssid = 5;
    const int64_t kSharedVersion = 42;

    const std::string filename = "test_tombstones_shared_rssid.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kNumTombstones; i++) {
        map.emplace(fmt::format("tomb2_{:016X}", i), std::make_pair(100, IndexValue(NullIndexValue)));
    }

    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize, &range_pb));

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    sstable_pb.set_shared_rssid(kSharedRssid);
    sstable_pb.set_shared_version(kSharedVersion);
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));

    std::vector<std::string> key_str(kNumTombstones);
    std::vector<Slice> keys(kNumTombstones);
    std::vector<IndexValue> values(kNumTombstones, IndexValue(NullIndexValue));
    KeyIndexSet key_indexes;
    KeyIndexSet found;
    for (int i = 0; i < kNumTombstones; i++) {
        key_str[i] = fmt::format("tomb2_{:016X}", i);
        keys[i] = Slice(key_str[i]);
        key_indexes.insert(i);
    }
    ASSERT_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));

    ASSERT_EQ(found.size(), static_cast<size_t>(kNumTombstones));
    for (int i = 0; i < kNumTombstones; i++) {
        ASSERT_EQ(NullIndexValue, values[i].get_value())
                << "shared_rssid tombstone at key " << key_str[i] << " leaked non-null";
    }

    // shared_version must still be projected onto tombstones so the time-travel
    // multi_get path (`version >= 0`) matches them. Without the version projection,
    // a versioned lookup walks past the tombstone and resurrects a stale live entry
    // from an older sstable — exactly the ordering bug Codex flagged. Verify by
    // querying at exactly shared_version and confirming each tombstone is found and
    // decodes to NullIndexValue.
    KeyIndexSet found_versioned;
    std::vector<IndexValue> values_versioned(kNumTombstones, IndexValue(NullIndexValue));
    ASSERT_OK(sst->multi_get(keys.data(), key_indexes, kSharedVersion, values_versioned.data(), &found_versioned));
    ASSERT_EQ(found_versioned.size(), static_cast<size_t>(kNumTombstones));
    for (int i = 0; i < kNumTombstones; i++) {
        ASSERT_EQ(NullIndexValue, values_versioned[i].get_value());
    }
}

namespace {

// Wraps an underlying SeekableInputStream and reports synthetic IO statistics so a
// shared-nothing UT can drive the local-disk vs remote breakdown that production gets
// from starlet's CacheFs. Every successful read contributes its byte count to one
// bucket, selected by the mode passed at construction.
class FakeStatsInputStream : public io::SeekableInputStreamWrapper {
public:
    enum Mode { kAllLocal, kAllRemote };

    FakeStatsInputStream(std::shared_ptr<io::SeekableInputStream> inner, Mode mode)
            : io::SeekableInputStreamWrapper(inner.get(), kDontTakeOwnership), _inner(std::move(inner)), _mode(mode) {}

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        RETURN_IF_ERROR(_inner->read_at_fully(offset, out, count));
        _bytes_read += count;
        ++_io_count;
        return Status::OK();
    }

    StatusOr<int64_t> read(void* data, int64_t count) override {
        auto res = _inner->read(data, count);
        if (res.ok()) {
            _bytes_read += *res;
            ++_io_count;
        }
        return res;
    }

    io::IoStatsSnapshot get_io_stats_snapshot() const override {
        io::IoStatsSnapshot snap;
        if (_mode == kAllRemote) {
            snap.bytes_read_remote = _bytes_read;
            snap.io_count_remote = _io_count;
        } else {
            snap.bytes_read_local_disk = _bytes_read;
            snap.io_count_local_disk = _io_count;
        }
        return snap;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _inner;
    Mode _mode;
    int64_t _bytes_read{0};
    int64_t _io_count{0};
};

// Trace stores counters keyed by `const char*` (pointer comparison), and the
// literal we pass here may not share an address with the one inside
// persistent_index_sstable.cpp across translation units. Match by string value
// instead so the test is robust against the compiler's string pooling decisions.
int64_t find_trace_metric(const std::map<const char*, int64_t>& metrics, std::string_view name) {
    for (const auto& [k, v] : metrics) {
        if (k != nullptr && name == k) {
            return v;
        }
    }
    return 0;
}

// multi_get reads through a handle it opens itself, so the only way to give it a stream with
// NumericStatistics is to swap that handle at the sync point right after it is opened. Returns
// the DeferOp that tears the callback down.
void install_fake_stats_handle(FakeStatsInputStream::Mode mode) {
    SyncPoint::GetInstance()->SetCallBack("PersistentIndexSstable::multi_get:opened_file", [mode](void* arg) {
        auto* rf = static_cast<std::unique_ptr<RandomAccessFile>*>(arg);
        auto fake = std::make_shared<FakeStatsInputStream>((*rf)->stream(), mode);
        *rf = std::make_unique<RandomAccessFile>(fake, (*rf)->filename());
    });
    SyncPoint::GetInstance()->EnableProcessing();
}

void clear_fake_stats_handle() {
    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::multi_get:opened_file");
    SyncPoint::GetInstance()->DisableProcessing();
}

// Drive multi_get against a handle whose stream reports every read in one bucket, and assert the
// trace counters break the misses down into that bucket and leave the other at zero. Without this
// the four sstable_io_* increments have no positive-path coverage at all: every other test runs on
// a POSIX file whose snapshot is all zeroes, so deleting them outright would still pass.
void run_multi_get_io_breakdown_case(const std::string& test_dir, const std::string& filename,
                                     FakeStatsInputStream::Mode mode) {
    constexpr int kN = 256; // big enough that MultiGet actually reads data blocks from the file
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(lake::join_path(test_dir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kN; ++i) {
        map.emplace(fmt::format("io_key_{:08d}", i), std::make_pair(int64_t(1), IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, wf.get(), &filesize, &range_pb));
    ASSERT_OK(wf->close());

    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(test_dir, filename)));
    auto sst = std::make_unique<PersistentIndexSstable>();
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache.get()));

    std::vector<std::string> key_str(kN);
    std::vector<Slice> keys(kN);
    std::vector<IndexValue> values(kN, IndexValue(NullIndexValue));
    KeyIndexSet key_indexes;
    KeyIndexSet found;
    for (int i = 0; i < kN; ++i) {
        key_str[i] = fmt::format("io_key_{:08d}", i);
        keys[i] = Slice(key_str[i]);
        key_indexes.insert(i);
    }

    install_fake_stats_handle(mode);
    DeferOp clear_callback([&] { clear_fake_stats_handle(); });

    scoped_refptr<Trace> trace(new Trace);
    {
        ADOPT_TRACE(trace.get());
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));
    }
    ASSERT_EQ(found.size(), static_cast<size_t>(kN));

    auto metrics = trace->metrics()->Get();
    int64_t miss_cnt = find_trace_metric(metrics, "read_block_miss_cache_cnt");
    int64_t local_bytes = find_trace_metric(metrics, "sstable_io_local_disk_bytes");
    int64_t remote_bytes = find_trace_metric(metrics, "sstable_io_remote_bytes");
    int64_t local_count = find_trace_metric(metrics, "sstable_io_count_local_disk");
    int64_t remote_count = find_trace_metric(metrics, "sstable_io_count_remote");

    // The sstable layer must have missed its in-memory block cache at least once for
    // the counters to mean anything; otherwise the case isn't exercising the path.
    ASSERT_GT(miss_cnt, 0) << "MultiGet didn't trigger any sstable block file reads";

    if (mode == FakeStatsInputStream::kAllRemote) {
        EXPECT_EQ(0, local_bytes);
        EXPECT_EQ(0, local_count);
        EXPECT_GT(remote_bytes, 0);
        EXPECT_GT(remote_count, 0);
    } else {
        EXPECT_GT(local_bytes, 0);
        EXPECT_GT(local_count, 0);
        EXPECT_EQ(0, remote_bytes);
        EXPECT_EQ(0, remote_count);
    }
}

} // namespace

TEST_F(PersistentIndexSstableTest, test_multi_get_io_breakdown_local_disk) {
    run_multi_get_io_breakdown_case(kTestDir, "io_breakdown_local.sst", FakeStatsInputStream::kAllLocal);
}

TEST_F(PersistentIndexSstableTest, test_multi_get_io_breakdown_remote) {
    run_multi_get_io_breakdown_case(kTestDir, "io_breakdown_remote.sst", FakeStatsInputStream::kAllRemote);
}

// When the underlying stream exposes no NumericStatistics (the common shared-nothing
// case), multi_get must still emit the new counters at zero rather than crash or skip
// them entirely. This guards the nullptr-stats branch of take_index_sstable_io_snapshot.
TEST_F(PersistentIndexSstableTest, test_multi_get_io_breakdown_no_stats) {
    constexpr int kN = 16;
    const std::string filename = "io_breakdown_no_stats.sst";
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kN; ++i) {
        map.emplace(fmt::format("ns_key_{:08d}", i), std::make_pair(int64_t(1), IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, wf.get(), &filesize, &range_pb));
    ASSERT_OK(wf->close());

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache.get()));

    std::vector<std::string> key_str(kN);
    std::vector<Slice> keys(kN);
    std::vector<IndexValue> values(kN, IndexValue(NullIndexValue));
    KeyIndexSet key_indexes;
    KeyIndexSet found;
    for (int i = 0; i < kN; ++i) {
        key_str[i] = fmt::format("ns_key_{:08d}", i);
        keys[i] = Slice(key_str[i]);
        key_indexes.insert(i);
    }

    scoped_refptr<Trace> trace(new Trace);
    {
        ADOPT_TRACE(trace.get());
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));
    }
    ASSERT_EQ(found.size(), static_cast<size_t>(kN));

    auto metrics = trace->metrics()->Get();
    EXPECT_EQ(0, find_trace_metric(metrics, "sstable_io_local_disk_bytes"));
    EXPECT_EQ(0, find_trace_metric(metrics, "sstable_io_remote_bytes"));
    EXPECT_EQ(0, find_trace_metric(metrics, "sstable_io_count_local_disk"));
    EXPECT_EQ(0, find_trace_metric(metrics, "sstable_io_count_remote"));
}

// Schema contract: every field of IoStatsSnapshot must zero-initialize so the base
// `InputStream::get_io_stats_snapshot()` default (`return {};`) yields an all-zero
// snapshot and the trace counters degrade cleanly on streams that don't expose IO
// breakdown. Loud, focused guard against someone later adding a field without a
// default — TraceMetrics::Increment is a plain int64_t add, an uninitialized field
// would silently leak whatever garbage was on the stack into the trace.
TEST_F(PersistentIndexSstableTest, test_io_stats_snapshot_default_zeroed) {
    io::IoStatsSnapshot snap{};
    EXPECT_EQ(0, snap.bytes_read_local_disk);
    EXPECT_EQ(0, snap.bytes_read_remote);
    EXPECT_EQ(0, snap.bytes_write_local_disk);
    EXPECT_EQ(0, snap.bytes_write_remote);
    EXPECT_EQ(0, snap.io_count_local_disk);
    EXPECT_EQ(0, snap.io_count_remote);
    EXPECT_EQ(0, snap.io_ns_read_local_disk);
    EXPECT_EQ(0, snap.io_ns_read_remote);
    EXPECT_EQ(0, snap.io_ns_write_local_disk);
    EXPECT_EQ(0, snap.io_ns_write_remote);
    EXPECT_EQ(0, snap.prefetch_hit_count);
    EXPECT_EQ(0, snap.prefetch_wait_finish_ns);
    EXPECT_EQ(0, snap.prefetch_pending_ns);
}

namespace {

// Minimal seekable stream whose `get_io_stats_snapshot()` returns a recognizable
// sentinel. Used by the wrapper-forwarding test below to verify each wrapper
// layer in the InputStream hierarchy passes the call straight to its inner.
class SentinelSeekableStream : public io::SeekableInputStream {
public:
    static constexpr int64_t kSentinel = 0xC0DE;

    StatusOr<int64_t> read(void* /*data*/, int64_t /*count*/) override { return 0; }
    Status read_fully(void* /*data*/, int64_t /*count*/) override { return Status::OK(); }
    Status seek(int64_t /*position*/) override { return Status::OK(); }
    StatusOr<int64_t> position() override { return 0; }
    StatusOr<int64_t> get_size() override { return 1; }

    io::IoStatsSnapshot get_io_stats_snapshot() const override {
        io::IoStatsSnapshot snap;
        snap.bytes_read_local_disk = kSentinel;
        return snap;
    }
};

} // namespace

// Trivial wrappers in the InputStream / SeekableInputStream hierarchy must pass
// `get_io_stats_snapshot()` straight through to the wrapped stream — they are
// 1-line forwarders and easy to break silently. This test wraps a sentinel-
// emitting stream in each of:
//   - io::InputStreamWrapper         (be/src/io/input_stream.h)
//   - io::SeekableInputStreamWrapper (be/src/io/seekable_input_stream.h)
//   - SharedBufferedInputStream  (be/src/cache/scan/shared_buffered_input_stream.h)
//   - BundleSeekableInputStream      (be/src/fs/bundle_file.h)
// and asserts the sentinel byte count survives the forward. CompressedInputStream
// is left out because its constructor needs a real StreamDecompressor and the
// forwarder there is the identical 1-liner pattern.
TEST_F(PersistentIndexSstableTest, test_io_stats_snapshot_wrapper_forwarding) {
    // InputStreamWrapper
    {
        auto sentinel = std::make_unique<SentinelSeekableStream>();
        io::InputStreamWrapper w(std::unique_ptr<io::InputStream>(sentinel.release()));
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
    // SeekableInputStreamWrapper
    {
        auto sentinel = std::make_unique<SentinelSeekableStream>();
        io::SeekableInputStreamWrapper w(std::move(sentinel));
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
    // SharedBufferedInputStream
    {
        auto sentinel = std::make_shared<SentinelSeekableStream>();
        SharedBufferedInputStream w(sentinel, "" /*filename*/, 1 /*file_size*/);
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
    // BundleSeekableInputStream
    {
        auto sentinel = std::make_shared<SentinelSeekableStream>();
        BundleSeekableInputStream w(sentinel, 0 /*offset*/, 1 /*size*/);
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
}

// Tablet split samples the PK index for its boundaries, and a tablet born of an earlier split reads
// the SSTs its parent wrote -- they stay shared until MERGE rewrites them privately. So the budget
// has to be filled from inside the caller's own window: a table-wide density puts almost nothing
// there, which is exactly what leaves a narrow child without enough boundaries to split again.
TEST_F(PersistentIndexSstableTest, test_sample_data_keys_scoped_to_range) {
    // Enough keys that the index spans many blocks: the whole point is the contrast between a
    // window-scoped density and a table-wide one, and both need room to differ.
    const int N = 40000;
    const std::string filename = "test_sample_data_keys_in_range.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < N; i++) {
        map.emplace(fmt::format("test_key_{:016X}", i), std::make_pair(100, IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize, &range_pb));

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr(new_lru_cache(1024 * 1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));

    // A window over a tenth of the keys, the shape a child tablet has after one split.
    const std::string seek_key = fmt::format("test_key_{:016X}", N / 2);
    const std::string stop_key = fmt::format("test_key_{:016X}", N / 2 + N / 10);
    constexpr size_t kMaxSamples = 16;
    auto count_in_window = [&](const std::vector<std::string>& keys) {
        return static_cast<size_t>(std::count_if(
                keys.begin(), keys.end(), [&](const std::string& key) { return key >= seek_key && key < stop_key; }));
    };

    std::vector<std::string> window_samples;
    ASSERT_OK(sst->sample_data_keys(&window_samples, Slice(seek_key), Slice(stop_key), kMaxSamples));
    ASSERT_LE(window_samples.size(), kMaxSamples);
    for (const auto& key : window_samples) {
        EXPECT_GE(key, seek_key);
    }
    // A separator below stop_key can resolve to the first key of the following block, which may sit
    // past it; that is the one sample allowed to overshoot, and the split path filters it anyway.
    EXPECT_LE(window_samples.size() - count_in_window(window_samples), 1u);
    EXPECT_GE(count_in_window(window_samples), 4u);

    // The same budget spent over the whole table barely reaches the window.
    std::vector<std::string> table_samples;
    ASSERT_OK(sst->sample_data_keys(&table_samples, Slice(), Slice(), kMaxSamples));
    ASSERT_LE(table_samples.size(), kMaxSamples);
    EXPECT_GT(count_in_window(window_samples), count_in_window(table_samples));

    // A window past the last key holds no data keys at all: the only index entry at or after it is
    // the trailing short successor, which seeks past the end of the table.
    std::vector<std::string> beyond_samples;
    const std::string beyond_key = fmt::format("test_key_{:016X}", N + 1000);
    ASSERT_OK(sst->sample_data_keys(&beyond_samples, Slice(beyond_key), Slice(), kMaxSamples));
    EXPECT_TRUE(beyond_samples.empty());
}

// multi_get decodes every found value into one reused IndexValuesWithVerPB. Interleave keys whose values hold two
// versions, one version and a tombstone, so anything carried over from the previous key's value would show up as a
// wrong rss_rowid, a wrong version match or a tombstone read back as live.
TEST_F(PersistentIndexSstableTest, test_multi_get_reuses_value_message_across_keys) {
    const int N = 30;
    const std::string filename = "test_multi_get_reuses_value_message.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    sstable::Options options;
    sstable::TableBuilder builder(options, file.get());
    std::vector<std::string> key_str(N);
    for (int i = 0; i < N; i++) {
        key_str[i] = fmt::format("reuse_{:016X}", i);
        IndexValuesWithVerPB value_pb;
        auto add_value = [&](int64_t version, uint32_t rssid, uint32_t rowid) {
            auto* value = value_pb.add_values();
            value->set_version(version);
            value->set_rssid(rssid);
            value->set_rowid(rowid);
        };
        if (i % 3 == 0) {
            // Two versions, newest first.
            add_value(200, 2, i);
            add_value(100, 1, i);
        } else if (i % 3 == 1) {
            add_value(100, 1, i);
        } else {
            add_value(100, UINT32_MAX, UINT32_MAX);
        }
        ASSERT_OK(builder.Add(Slice(key_str[i]), Slice(value_pb.SerializeAsString())));
    }
    ASSERT_OK(builder.Finish());
    PersistentIndexSstableRangePB range_pb;
    auto [start_key, end_key] = builder.KeyRange();
    range_pb.set_start_key(start_key.to_string());
    range_pb.set_end_key(end_key.to_string());
    const uint64_t filesize = builder.FileSize();
    ASSERT_OK(file->close());

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache_ptr.get()));

    std::vector<Slice> keys(N);
    KeyIndexSet key_indexes;
    for (int i = 0; i < N; i++) {
        keys[i] = Slice(key_str[i]);
        key_indexes.insert(i);
    }
    auto rss_rowid = [](uint32_t rssid, uint32_t rowid) { return (static_cast<uint64_t>(rssid) << 32) | rowid; };

    // The newest value of every key.
    {
        std::vector<IndexValue> values(N, IndexValue(NullIndexValue));
        KeyIndexSet found;
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));
        ASSERT_EQ(key_indexes, found);
        for (int i = 0; i < N; i++) {
            const uint64_t expected = i % 3 == 0 ? rss_rowid(2, i) : (i % 3 == 1 ? rss_rowid(1, i) : NullIndexValue);
            ASSERT_EQ(expected, values[i].get_value()) << "key " << i;
        }
    }
    // Only the two-version keys hold a version-200 entry.
    {
        std::vector<IndexValue> values(N, IndexValue(NullIndexValue));
        KeyIndexSet found;
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, 200, values.data(), &found));
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(i % 3 == 0, found.count(i) > 0) << "key " << i;
            if (i % 3 == 0) {
                ASSERT_EQ(rss_rowid(2, i), values[i].get_value()) << "key " << i;
            }
        }
    }
    // Every key holds a version-100 entry: the older one of the two-version keys, the only one elsewhere.
    {
        std::vector<IndexValue> values(N, IndexValue(NullIndexValue));
        KeyIndexSet found;
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, 100, values.data(), &found));
        ASSERT_EQ(key_indexes, found);
        for (int i = 0; i < N; i++) {
            const uint64_t expected = i % 3 == 2 ? NullIndexValue : rss_rowid(1, i);
            ASSERT_EQ(expected, values[i].get_value()) << "key " << i;
        }
    }
}

// MultiGet reads the clock for one key in Table::kMultiGetTimingSampleInterval, and only when a trace will report
// the time: one at random among the first kInterval keys, then every kInterval-th. The counts it adds to the trace
// stay exact: every probe is found in the block already open, rejected by the bloom filter, or reads a block.
TEST_F(PersistentIndexSstableTest, test_multi_get_times_sampled_keys_only) {
    constexpr int kTableKeys = 4096;
    constexpr size_t kInterval = sstable::Table::kMultiGetTimingSampleInterval;
    const std::string filename = "test_multi_get_times_sampled_keys_only.sst";
    auto key_of = [](int n) { return fmt::format("sampled_key_{:08d}", n); };
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kTableKeys; ++i) {
        // Even numbers only: the odd ones are absent keys inside the table's range.
        map.emplace(key_of(2 * i), std::make_pair(int64_t(1), IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, wf.get(), &filesize, &range_pb));
    ASSERT_OK(wf->close());

    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    auto sst = std::make_unique<PersistentIndexSstable>();
    std::unique_ptr<Cache> cache(new_lru_cache(1024 * 1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(read_file), sstable_pb, cache.get()));

    // Positions, within each lookup, of the keys MultiGet timed.
    std::vector<size_t> timed;
    SyncPoint::GetInstance()->SetCallBack("sstable::Table::MultiGet:timed_probe",
                                          [&](void* arg) { timed.push_back(*static_cast<size_t*>(arg)); });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_callback([] {
        SyncPoint::GetInstance()->ClearCallBack("sstable::Table::MultiGet:timed_probe");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Looks up the keys numbered `numbers` under `trace` (none if nullptr) and returns how many were found.
    auto multi_get = [&](const std::vector<int>& numbers, Trace* trace) {
        std::vector<std::string> key_str;
        for (int n : numbers) {
            key_str.push_back(key_of(n));
        }
        std::vector<Slice> keys(key_str.begin(), key_str.end());
        std::vector<IndexValue> values(keys.size(), IndexValue(NullIndexValue));
        KeyIndexSet key_indexes;
        for (size_t i = 0; i < keys.size(); ++i) {
            key_indexes.insert(i);
        }
        KeyIndexSet found;
        if (trace != nullptr) {
            ADOPT_TRACE(trace);
            CHECK_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));
        } else {
            CHECK_OK(sst->multi_get(keys.data(), key_indexes, -1, values.data(), &found));
        }
        return found.size();
    };
    auto probes_in = [](const std::map<const char*, int64_t>& metrics) {
        return find_trace_metric(metrics, "continue_block_read_cnt") +
               find_trace_metric(metrics, "sst_bloom_filter_rows") +
               find_trace_metric(metrics, "read_block_hit_cache_cnt") +
               find_trace_metric(metrics, "read_block_miss_cache_cnt");
    };

    // Absent keys, as in an upsert of new primary keys: the bloom filter rejects nearly all of them and none is
    // found in an open block, so every key picked for timing reaches the end of the loop and is timed.
    std::vector<int> absent;
    for (int i = 0; i < 1000; ++i) {
        absent.push_back(2 * i + 1);
    }
    {
        scoped_refptr<Trace> trace(new Trace);
        ASSERT_EQ(0, multi_get(absent, trace.get()));
        ASSERT_FALSE(timed.empty());
        EXPECT_LT(timed.front(), kInterval);
        for (size_t j = 1; j < timed.size(); ++j) {
            EXPECT_EQ(timed[j - 1] + kInterval, timed[j]);
        }
        EXPECT_GE(timed.back() + kInterval, absent.size());
        auto metrics = trace->metrics()->Get();
        EXPECT_EQ(absent.size(), probes_in(metrics));
        EXPECT_EQ(0, find_trace_metric(metrics, "continue_block_read_cnt"));
        EXPECT_GT(find_trace_metric(metrics, "sst_bloom_filter_rows"), 900);
        for (std::string_view name : {"multiget_t1_us", "multiget_t2_us", "multiget_t3_us"}) {
            auto it = std::find_if(metrics.begin(), metrics.end(),
                                   [&](const auto& kv) { return kv.first != nullptr && name == kv.first; });
            ASSERT_TRUE(it != metrics.end()) << name;
            EXPECT_GE(it->second, 0) << name;
        }
    }

    // Present keys: most are found in the block already open and leave the loop before its timed end, so only some
    // of the keys picked for timing show up, all kInterval apart.
    std::vector<int> present;
    for (int i = 0; i < kTableKeys; ++i) {
        present.push_back(2 * i);
    }
    {
        timed.clear();
        scoped_refptr<Trace> trace(new Trace);
        ASSERT_EQ(present.size(), multi_get(present, trace.get()));
        EXPECT_LE(timed.size(), (present.size() + kInterval - 1) / kInterval);
        for (size_t pos : timed) {
            EXPECT_EQ(timed.front() % kInterval, pos % kInterval);
        }
        auto metrics = trace->metrics()->Get();
        EXPECT_EQ(present.size(), probes_in(metrics));
        EXPECT_GT(find_trace_metric(metrics, "continue_block_read_cnt"), 0);
        EXPECT_EQ(0, find_trace_metric(metrics, "sst_bloom_filter_rows"));
    }

    // Without a trace nothing would report the time, so no key is timed.
    timed.clear();
    ASSERT_EQ(0, multi_get(absent, nullptr));
    EXPECT_TRUE(timed.empty());
}

namespace {

// A composite key PRIMARY KEY(id BIGINT, type VARCHAR), as PrimaryKeyEncoder encodes it.
std::string make_id_type_key(int64_t id, const std::string& type) {
    uint64_t u = static_cast<uint64_t>(id) ^ (1ULL << 63);
    std::string key;
    for (int i = 7; i >= 0; --i) key.push_back(static_cast<char>((u >> (i * 8)) & 0xff));
    return key + type;
}

const sstable::KeyLayout kIdTypeLayout = {{8}, {0}};
const std::vector<std::string> kEventTypes = {"order_created", "order_paid", "order_shipped", "order_cancelled"};

// Turns sstable dictionary encoding on for the scope of a test.
struct ScopedKeyDictEncoding {
    explicit ScopedKeyDictEncoding(bool enable) : _old(config::lake_enable_pk_index_sst_dict_encoding) {
        config::lake_enable_pk_index_sst_dict_encoding = enable;
    }
    ~ScopedKeyDictEncoding() { config::lake_enable_pk_index_sst_dict_encoding = _old; }
    bool _old;
};

} // namespace

TEST_F(PersistentIndexSstableTest, test_pk_index_key_layout_matches_encoder) {
    // PRIMARY KEY(id BIGINT, type VARCHAR, level SMALLINT, day DATE, ts DATETIME, tag VARCHAR)
    const std::vector<LogicalType> types = {TYPE_BIGINT, TYPE_VARCHAR,  TYPE_SMALLINT,
                                            TYPE_DATE,   TYPE_DATETIME, TYPE_VARCHAR};
    Fields fields;
    std::vector<ColumnId> sort_key_idxes;
    for (size_t i = 0; i < types.size(); ++i) {
        auto field = std::make_shared<Field>(i, fmt::format("c{}", i), types[i], false);
        field->set_is_key(true);
        field->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        field->set_uid(i);
        fields.emplace_back(std::move(field));
        sort_key_idxes.push_back(i);
    }
    Schema schema(std::move(fields), PRIMARY_KEYS, sort_key_idxes);
    const auto layout = pk_index_key_layout(schema);
    ASSERT_EQ(types.size(), layout.size());
    const std::vector<uint32_t> expected_widths = {8, 0, 2, 4, 8, 0};
    for (size_t i = 0; i < layout.size(); ++i) {
        ASSERT_EQ(expected_widths[i], layout[i].fixed_width) << i;
    }

    // Encode keys with the real encoder: a layout that disagreed with it would cut the keys in the wrong places.
    const std::vector<std::string> type_values = {"login", std::string("pay\0ment", 8), "logout"};
    const int n = 3000;
    auto chunk = ChunkFactory::new_chunk(schema, n);
    for (int i = 0; i < n; ++i) {
        Datum id, type, level, day, ts, tag;
        id.set_int64(i);
        type.set_slice(Slice(type_values[i % 3]));
        level.set_int16(static_cast<int16_t>(i % 4));
        day.set_date(DateValue::create(2026, 1 + i % 2, 1));
        ts.set_timestamp(TimestampValue::create(2026, 11, 24, i % 24, (i / 24) % 60, i % 60, 0));
        const std::string tag_value = fmt::format("tag_{}", i % 5);
        tag.set_slice(Slice(tag_value));
        chunk->columns()[0]->as_mutable_ptr()->append_datum(id);
        chunk->columns()[1]->as_mutable_ptr()->append_datum(type);
        chunk->columns()[2]->as_mutable_ptr()->append_datum(level);
        chunk->columns()[3]->as_mutable_ptr()->append_datum(day);
        chunk->columns()[4]->as_mutable_ptr()->append_datum(ts);
        chunk->columns()[5]->as_mutable_ptr()->append_datum(tag);
    }
    MutableColumnPtr encoded;
    ASSERT_OK(PrimaryKeyEncoder::create_column(schema, &encoded, PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2));
    PrimaryKeyEncoder::encode(schema, *chunk, 0, n, encoded.get(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
    std::vector<std::string> keys;
    for (int i = 0; i < n; ++i) {
        keys.push_back(encoded->get(i).get_slice().to_string());
    }
    std::sort(keys.begin(), keys.end());

    sstable::KeyDictCollector collector(layout, 256);
    for (const auto& key : keys) {
        collector.add(Slice(key));
    }
    ASSIGN_OR_ABORT(auto dict, collector.finish(true));
    ASSERT_NE(nullptr, dict);
    // type, level, day and tag; ts has too many values.
    std::vector<uint32_t> dict_columns;
    for (const auto& column : dict->columns()) {
        dict_columns.push_back(column.column);
    }
    ASSERT_EQ(std::vector<uint32_t>({1, 2, 3, 5}), dict_columns);
    std::string middle_type;
    encoding_utils::encode_slice(Slice(type_values[1]), &middle_type, false);
    ASSERT_TRUE(std::binary_search(dict->columns()[0].values.begin(), dict->columns()[0].values.end(), middle_type));
    ASSERT_EQ(3, dict->columns()[0].values.size());
    ASSERT_EQ(5, dict->columns()[3].values.size());

    std::string stored;
    std::string prev;
    std::string decoded;
    for (const auto& key : keys) {
        dict->encode(Slice(key), &stored);
        ASSERT_LT(stored.size(), key.size());
        ASSERT_LT(prev, stored);
        prev = stored;
        ASSERT_OK(dict->decode(Slice(stored), &decoded));
        ASSERT_EQ(key, decoded);
    }

    // A single-column key has nothing to share.
    Fields single;
    auto field = std::make_shared<Field>(0, "c0", TYPE_VARCHAR, false);
    field->set_is_key(true);
    single.emplace_back(std::move(field));
    ASSERT_TRUE(pk_index_key_layout(Schema(std::move(single), PRIMARY_KEYS, {0})).empty());
}

TEST_F(PersistentIndexSstableTest, test_pk_index_key_layout_from_tablet_schema) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(2);
    auto add_column = [&](const std::string& name, const std::string& type, bool is_key) {
        auto* column = schema_pb.add_column();
        column->set_unique_id(schema_pb.column_size());
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(false);
        column->set_length(type == "VARCHAR" ? 64 : 8);
        if (!is_key) column->set_aggregation("REPLACE");
    };
    add_column("id", "BIGINT", true);
    add_column("type", "VARCHAR", true);
    add_column("v", "INT", false);
    const auto layout = pk_index_key_layout(TabletSchema::create(schema_pb));
    ASSERT_EQ(2, layout.size());
    ASSERT_EQ(8, layout[0].fixed_width);
    ASSERT_EQ(0, layout[1].fixed_width);

    // Off by default: no options, so builders write sstables exactly as before.
    ASSERT_EQ(nullptr, pk_index_key_dict_options(layout));
    ScopedKeyDictEncoding enable(true);
    auto options = pk_index_key_dict_options(layout);
    ASSERT_NE(nullptr, options);
    ASSERT_EQ(static_cast<uint32_t>(config::lake_pk_index_sst_dict_max_cardinality), options->max_cardinality);
    ASSERT_EQ(nullptr, pk_index_key_dict_options({}));
}

TEST_F(PersistentIndexSstableTest, test_build_sstable_with_key_dict) {
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int64_t id = 0; id < 5000; ++id) {
        map.emplace(make_id_type_key(id * 3, kEventTypes[id % kEventTypes.size()]),
                    std::make_pair(100 + id % 7, IndexValue(static_cast<uint64_t>(id))));
    }
    auto build = [&](const std::string& filename, std::unique_ptr<PersistentIndexSstable>* sst) {
        ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
        uint64_t filesize = 0;
        PersistentIndexSstableRangePB range_pb;
        ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize, &range_pb, kIdTypeLayout));
        ASSERT_OK(file->close());
        ASSERT_EQ(map.begin()->first, range_pb.start_key());
        ASSERT_EQ(map.rbegin()->first, range_pb.end_key());
        PersistentIndexSstablePB sstable_pb;
        sstable_pb.set_filename(filename);
        sstable_pb.set_filesize(filesize);
        sstable_pb.mutable_range()->CopyFrom(range_pb);
        ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
        *sst = std::make_unique<PersistentIndexSstable>();
        ASSERT_OK((*sst)->init(std::move(read_file), sstable_pb, nullptr));
    };
    std::unique_ptr<PersistentIndexSstable> plain;
    build("test_build_sstable_without_key_dict.sst", &plain);
    ASSERT_EQ(nullptr, plain->key_dict());

    ScopedKeyDictEncoding enable(true);
    std::unique_ptr<PersistentIndexSstable> sst;
    build("test_build_sstable_with_key_dict.sst", &sst);
    ASSERT_NE(nullptr, sst->key_dict());
    ASSERT_EQ(1, sst->key_dict()->columns().size());
    ASSERT_EQ(kEventTypes.size(), sst->key_dict()->columns()[0].values.size());
    ASSERT_LT(sst->sstable_pb().filesize(), plain->sstable_pb().filesize());

    // Every key is found with its value, and keys the sstable does not have, including ones whose type is
    // not in its dictionary, are not.
    std::vector<std::string> keys_str;
    for (int64_t id = 0; id < 5000; ++id) {
        keys_str.push_back(make_id_type_key(id * 3, kEventTypes[id % kEventTypes.size()]));
        keys_str.push_back(make_id_type_key(id * 3 + 1, kEventTypes[id % kEventTypes.size()]));
        keys_str.push_back(make_id_type_key(id * 3, "order_refunded"));
    }
    std::vector<Slice> keys(keys_str.begin(), keys_str.end());
    KeyIndexSet key_indexes;
    for (size_t i = 0; i < keys.size(); ++i) {
        key_indexes.insert(i);
    }
    for (int64_t version : {int64_t(-1), int64_t(100)}) {
        std::vector<IndexValue> values(keys.size(), IndexValue(NullIndexValue));
        KeyIndexSet found;
        ASSERT_OK(sst->multi_get(keys.data(), key_indexes, version, values.data(), &found));
        for (size_t i = 0; i < keys.size(); ++i) {
            const int64_t id = static_cast<int64_t>(i / 3);
            const bool present = i % 3 == 0 && (version < 0 || 100 + id % 7 == version);
            ASSERT_EQ(present, found.count(i) > 0) << i;
            if (present) {
                ASSERT_EQ(IndexValue(static_cast<uint64_t>(id)), values[i]);
            }
        }
    }

    // Iterators hand out the keys as they were added.
    sstable::ReadOptions read_options;
    std::unique_ptr<sstable::Iterator> iter(sst->new_iterator(read_options));
    auto it = map.begin();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++it) {
        ASSERT_EQ(it->first, iter->key().to_string());
    }
    ASSERT_OK(iter->status());
    ASSERT_TRUE(it == map.end());

    std::vector<std::string> samples;
    ASSERT_OK(sst->sample_data_keys(&samples, Slice(), Slice(), 10));
    ASSERT_FALSE(samples.empty());
    for (const auto& sample : samples) {
        ASSERT_TRUE(map.find(sample) != map.end());
    }
}

TEST_F(PersistentIndexSstableTest, test_stream_and_tombstone_builders_with_key_dict) {
    ScopedKeyDictEncoding enable(true);
    // More keys than a builder samples before it chooses the dictionary.
    const int n = 40000;
    std::vector<std::string> keys;
    for (int64_t id = 0; id < n / 4; ++id) {
        for (const auto& type : kEventTypes) {
            keys.push_back(make_id_type_key(id, type));
        }
    }
    std::sort(keys.begin(), keys.end());

    const std::string filename = "test_stream_builder_with_key_dict.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    PersistentIndexSstableStreamBuilder builder(std::move(file), "", kIdTypeLayout);
    for (const auto& key : keys) {
        ASSERT_OK(builder.add(Slice(key)));
    }
    uint64_t file_size = 0;
    ASSERT_OK(builder.finish(&file_size));
    ASSERT_EQ(keys.size(), builder.num_entries());
    ASSERT_EQ(keys.front(), builder.key_range().first.to_string());
    ASSERT_EQ(keys.back(), builder.key_range().second.to_string());

    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(file_size);
    sstable_pb.set_shared_rssid(9);
    sstable_pb.set_shared_version(5);
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    PersistentIndexSstable sst;
    ASSERT_OK(sst.init(std::move(read_file), sstable_pb, nullptr));
    ASSERT_NE(nullptr, sst.key_dict());
    std::vector<Slice> slices(keys.begin(), keys.end());
    KeyIndexSet key_indexes;
    for (size_t i = 0; i < slices.size(); ++i) {
        key_indexes.insert(i);
    }
    std::vector<IndexValue> values(slices.size(), IndexValue(NullIndexValue));
    KeyIndexSet found;
    ASSERT_OK(sst.multi_get(slices.data(), key_indexes, 5, values.data(), &found));
    ASSERT_EQ(key_indexes, found);
    for (size_t i = 0; i < slices.size(); ++i) {
        ASSERT_EQ(IndexValue((uint64_t(9) << 32) | i), values[i]) << i;
    }

    // A tombstone sstable over the same keys.
    const std::string tomb_filename = "test_tombstone_with_key_dict.sst";
    ASSIGN_OR_ABORT(auto tomb_file, fs::new_writable_file(lake::join_path(kTestDir, tomb_filename)));
    uint64_t tomb_size = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_tombstone_sstable(slices.data(), slices.size(), 7, tomb_file.get(),
                                                              &tomb_size, &range_pb, kIdTypeLayout));
    ASSERT_OK(tomb_file->close());
    ASSERT_EQ(keys.front(), range_pb.start_key());
    ASSERT_EQ(keys.back(), range_pb.end_key());
    PersistentIndexSstablePB tomb_pb;
    tomb_pb.set_filename(tomb_filename);
    tomb_pb.set_filesize(tomb_size);
    ASSIGN_OR_ABORT(auto tomb_read_file, fs::new_random_access_file(lake::join_path(kTestDir, tomb_filename)));
    PersistentIndexSstable tomb;
    ASSERT_OK(tomb.init(std::move(tomb_read_file), tomb_pb, nullptr));
    ASSERT_NE(nullptr, tomb.key_dict());
    std::vector<IndexValue> tomb_values(slices.size(), IndexValue(0));
    KeyIndexSet tomb_found;
    ASSERT_OK(tomb.multi_get(slices.data(), key_indexes, 7, tomb_values.data(), &tomb_found));
    ASSERT_EQ(key_indexes, tomb_found);
    for (const auto& value : tomb_values) {
        ASSERT_EQ(IndexValue(NullIndexValue), value);
    }
}

} // namespace starrocks::lake
