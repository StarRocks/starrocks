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
#include "common/config_primary_key_fwd.h"
#include "common/config_starlet_fwd.h"
#include "fs/bundle_file.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "io/core/input_stream.h"
#include "io/core/seekable_input_stream.h"
#include "io/shared_buffered_input_stream.h"
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
    TabletManager tablet_mgr(location_provider, 1024);
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
    SyncPoint::GetInstance()->EnableProcessing();

    auto sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto rf, fs::new_random_access_file(path));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    auto st = sst->init(std::move(rf), sstable_pb, nullptr, true, nullptr, metadata, &tablet_mgr);

    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::init:table_open_error");
    SyncPoint::GetInstance()->ClearCallBack("PersistentIndexSstable::drop_corrupted_cache");
    SyncPoint::GetInstance()->DisableProcessing();
    config::lake_clear_corrupted_cache_data = old;

    ASSERT_OK(st);
}
#endif // USE_STAROS && !BUILD_FORMAT_LIB

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
// shared-nothing UT can drive the local-disk vs remote breakdown that production
// gets from starlet's CacheFs. Every successful read_at_fully contributes its byte
// count to one bucket, selected by the mode passed at construction.
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

// Simulates the rare `_rf`-swap case where the second snapshot sees smaller counters
// than the first (e.g., when retry-after-corruption replaces the underlying file).
// First snapshot call returns inflated values; every subsequent call returns all zero.
// `multi_get` must clamp the resulting negative delta to zero before incrementing the
// trace metric — otherwise the metric would record a negative number.
class SwappingStatsInputStream : public io::SeekableInputStreamWrapper {
public:
    static constexpr int64_t kInflatedBefore = 1 << 20;

    explicit SwappingStatsInputStream(std::shared_ptr<io::SeekableInputStream> inner)
            : io::SeekableInputStreamWrapper(inner.get(), kDontTakeOwnership), _inner(std::move(inner)) {}

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        return _inner->read_at_fully(offset, out, count);
    }
    StatusOr<int64_t> read(void* data, int64_t count) override { return _inner->read(data, count); }

    io::IoStatsSnapshot get_io_stats_snapshot() const override {
        io::IoStatsSnapshot snap;
        if (_call_count++ == 0) {
            snap.bytes_read_local_disk = kInflatedBefore;
            snap.bytes_read_remote = kInflatedBefore;
            snap.io_count_local_disk = kInflatedBefore;
            snap.io_count_remote = kInflatedBefore;
        }
        return snap;
    }

private:
    std::shared_ptr<io::SeekableInputStream> _inner;
    mutable int _call_count = 0;
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

void run_multi_get_io_breakdown_case(const std::string& test_dir, const std::string& filename,
                                     FakeStatsInputStream::Mode mode) {
    // Force the non-parallel `multi_get` code path so the read goes through the wrapped `_rf`
    // we inject below. With parallel execution on (the default), multi_get opens a fresh
    // RandomAccessFile via `fs::new_random_access_file` and bypasses our fake entirely; that
    // production-only branch exercises the same snapshot helper against the real starlet
    // stream and is out of scope for a shared-nothing UT.
    bool saved_parallel = config::enable_pk_index_parallel_execution;
    config::enable_pk_index_parallel_execution = false;
    DeferOp restore_parallel([&] { config::enable_pk_index_parallel_execution = saved_parallel; });
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
    auto inner_stream = read_file->stream();
    auto fake_stream = std::make_shared<FakeStatsInputStream>(inner_stream, mode);
    auto wrapped_file = std::make_unique<RandomAccessFile>(fake_stream, read_file->filename());

    auto sst = std::make_unique<PersistentIndexSstable>();
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(wrapped_file), sstable_pb, cache.get()));

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
    // the new counters to mean anything; otherwise the case isn't exercising the path.
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

// `multi_get`'s retry-after-corruption path can replace the underlying file mid-call.
// When that happens the after-snapshot reads a fresh, all-zero counter set while
// before-snapshot still holds the prior file's running totals — the naive delta is
// negative. SwappingStatsInputStream simulates this by returning inflated values on
// the first snapshot call and zero on subsequent calls. The trace metrics must stay
// non-negative; without the `std::max<int64_t>(0, …)` clamp they would record the
// negative delta directly via TraceMetrics::Increment (plain int64 add, no floor).
TEST_F(PersistentIndexSstableTest, test_multi_get_io_breakdown_clamps_negative_delta) {
    bool saved_parallel = config::enable_pk_index_parallel_execution;
    config::enable_pk_index_parallel_execution = false;
    DeferOp restore_parallel([&] { config::enable_pk_index_parallel_execution = saved_parallel; });

    constexpr int kN = 16;
    const std::string filename = "io_breakdown_clamp.sst";
    ASSIGN_OR_ABORT(auto wf, fs::new_writable_file(lake::join_path(kTestDir, filename)));
    phmap::btree_map<std::string, IndexValueWithVer, std::less<>> map;
    for (int i = 0; i < kN; ++i) {
        map.emplace(fmt::format("clamp_key_{:08d}", i), std::make_pair(int64_t(1), IndexValue(i)));
    }
    uint64_t filesize = 0;
    PersistentIndexSstableRangePB range_pb;
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, wf.get(), &filesize, &range_pb));
    ASSERT_OK(wf->close());

    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    auto fake_stream = std::make_shared<SwappingStatsInputStream>(read_file->stream());
    auto wrapped_file = std::make_unique<RandomAccessFile>(fake_stream, read_file->filename());

    auto sst = std::make_unique<PersistentIndexSstable>();
    std::unique_ptr<Cache> cache(new_lru_cache(1024));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
    sstable_pb.mutable_range()->CopyFrom(range_pb);
    sstable_pb.mutable_fileset_id()->CopyFrom(UniqueId::gen_uid().to_proto());
    ASSERT_OK(sst->init(std::move(wrapped_file), sstable_pb, cache.get()));

    std::vector<std::string> key_str(kN);
    std::vector<Slice> keys(kN);
    std::vector<IndexValue> values(kN, IndexValue(NullIndexValue));
    KeyIndexSet key_indexes;
    KeyIndexSet found;
    for (int i = 0; i < kN; ++i) {
        key_str[i] = fmt::format("clamp_key_{:08d}", i);
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
    // Inflated-before minus zero-after would be roughly -kInflatedBefore per counter
    // if the clamp were removed. We assert non-negative — any positive residual
    // (e.g., from MultiGet's own reads on the inner stream) is acceptable because
    // SwappingStatsInputStream doesn't track real reads at all; it just produces
    // a deterministic before>after pair.
    EXPECT_GE(find_trace_metric(metrics, "sstable_io_local_disk_bytes"), 0);
    EXPECT_GE(find_trace_metric(metrics, "sstable_io_remote_bytes"), 0);
    EXPECT_GE(find_trace_metric(metrics, "sstable_io_count_local_disk"), 0);
    EXPECT_GE(find_trace_metric(metrics, "sstable_io_count_remote"), 0);
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
//   - io::InputStreamWrapper         (be/src/io/core/input_stream.h)
//   - io::SeekableInputStreamWrapper (be/src/io/core/seekable_input_stream.h)
//   - io::SharedBufferedInputStream  (be/src/io/shared_buffered_input_stream.h)
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
        io::SharedBufferedInputStream w(sentinel, "" /*filename*/, 1 /*file_size*/);
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
    // BundleSeekableInputStream
    {
        auto sentinel = std::make_shared<SentinelSeekableStream>();
        BundleSeekableInputStream w(sentinel, 0 /*offset*/, 1 /*size*/);
        EXPECT_EQ(SentinelSeekableStream::kSentinel, w.get_io_stats_snapshot().bytes_read_local_disk);
    }
}

} // namespace starrocks::lake
