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
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/utils.h"
#include "storage/persistent_index.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/merger.h"
#include "storage/sstable/options.h"
#include "storage/sstable/table.h"
#include "storage/sstable/table_builder.h"
#include "testutil/assert.h"
#include "util/phmap/btree.h"

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
    sstable::Table* sstable = nullptr;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, &sstable));
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
    delete sstable;
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
    sstable::Table* sstable = nullptr;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, &sstable));
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
    delete sstable;
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
        sstable::Table* sstable = nullptr;
        CHECK_OK(sstable::Table::Open(options, read_files[i].get(), fileszs[i], &sstable));
        sstable::Iterator* iter = sstable->NewIterator(read_options);
        list.emplace_back(iter);
        sstable_ptrs[i].reset(sstable);
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
    sstable::Table* sstable = nullptr;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, &sstable));
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
    delete sstable;
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
    ASSERT_OK(PersistentIndexSstable::build_sstable(map, file.get(), &filesize));
    // 2. open sstable
    std::unique_ptr<PersistentIndexSstable> sst = std::make_unique<PersistentIndexSstable>();
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
    std::unique_ptr<Cache> cache_ptr;
    cache_ptr.reset(new_lru_cache(100));
    PersistentIndexSstablePB sstable_pb;
    sstable_pb.set_filename(filename);
    sstable_pb.set_filesize(filesize);
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
        sstable::Table* sstable = nullptr;
        ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(lake::join_path(kTestDir, filename)));
        CHECK_OK(sstable::Table::Open(options, read_file.get(), filesz, &sstable));
        sstable::ReadOptions read_options;
        sstable::Iterator* iter = sstable->NewIterator(read_options);
        for (iter->SeekToFirst(); iter->Valid() && iter->status().ok(); iter->Next()) {
        }
        ASSERT_TRUE(iter->status().ok());
        delete iter;
        delete sstable;
    }
}

} // namespace starrocks::lake
