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

#include <gtest/gtest.h>

#include <ctime>
#include <set>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/lake/join_path.h"
#include "storage/lake/sstable/iterator.h"
#include "storage/lake/sstable/merger.h"
#include "storage/lake/sstable/options.h"
#include "storage/lake/sstable/table.h"
#include "storage/lake/sstable/table_builder.h"
#include "storage/persistent_index.h"
#include "testutil/assert.h"
#include "util/phmap/btree.h"

namespace starrocks::lake {

class LakePersistentIndexSstableTest : public ::testing::Test {
public:
    static void SetUpTestCase() { CHECK_OK(fs::create_directories(kTestDir)); }

    static void TearDownTestCase() { (void)fs::remove_all(kTestDir); }

protected:
    constexpr static const char* const kTestDir = "./lake_persistent_index_sstable_test";
};

TEST_F(LakePersistentIndexSstableTest, test_generate_sst_scan_and_check) {
    const int N = 10000;
    sstable::Options options;
    const std::string filename = "test1.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (int i = 0; i < N; i++) {
        std::string str = fmt::format("test_key_{:016X}", i);
        IndexValue val(i);
        builder.Add(Slice(str), Slice(val.v, 8));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    // scan & check
    sstable::Table* sstable;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(join_path(kTestDir, filename)));
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
}

TEST_F(LakePersistentIndexSstableTest, test_generate_sst_seek_and_check) {
    const int N = 10000;
    sstable::Options options;
    const std::string filename = "test2.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (int i = 0; i < N; i++) {
        std::string str = fmt::format("test_key_{:016X}", i);
        IndexValue val(i);
        builder.Add(Slice(str), Slice(val.v, 8));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    // seek & check
    sstable::Table* sstable;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(join_path(kTestDir, filename)));
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
}

TEST_F(LakePersistentIndexSstableTest, test_merge) {
    std::vector<sstable::Iterator*> list;
    std::vector<std::unique_ptr<RandomAccessFile>> read_files;
    std::vector<uint64_t> fileszs;
    fileszs.resize(3);
    read_files.resize(3);
    const int N = 10000;
    for (int i = 0; i < 3; ++i) {
        sstable::Options options;
        const std::string filename = fmt::format("test_merge_{}.sst", i);
        ASSIGN_OR_ABORT(auto file, fs::new_writable_file(join_path(kTestDir, filename)));
        sstable::TableBuilder builder(options, file.get());
        for (int j = 0; j < N; j++) {
            std::string str = fmt::format("test_key_{:016X}", j);
            IndexValue val(j * i);
            builder.Add(Slice(str), Slice(val.v, 8));
        }
        CHECK_OK(builder.Finish());
        uint64_t filesz = builder.FileSize();
        fileszs[i] = filesz;
        ASSIGN_OR_ABORT(read_files[i], fs::new_random_access_file(join_path(kTestDir, filename)));
    }
    sstable::Options options;
    sstable::ReadOptions read_options;
    for (int i = 0; i < 3; ++i) {
        sstable::Table* sstable;
        CHECK_OK(sstable::Table::Open(options, read_files[i].get(), fileszs[i], &sstable));
        sstable::Iterator* iter = sstable->NewIterator(read_options);
        list.emplace_back(iter);
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
    }

    ASSERT_EQ(N, map.size());
    const std::string filename = "test_merge_4.sst";
    ASSIGN_OR_ABORT(auto file, fs::new_writable_file(join_path(kTestDir, filename)));
    sstable::TableBuilder builder(options, file.get());
    for (auto& [k, v] : map) {
        builder.Add(Slice(k), Slice(v));
    }
    CHECK_OK(builder.Finish());
    uint64_t filesz = builder.FileSize();
    sstable::Table* sstable;
    ASSIGN_OR_ABORT(auto read_file, fs::new_random_access_file(join_path(kTestDir, filename)));
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
}

} // namespace starrocks::lake
