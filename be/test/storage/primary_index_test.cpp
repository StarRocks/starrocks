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

#include "storage/primary_index.h"

#include <gtest/gtest.h>

#include <random>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_dump.h"
#include "storage/primary_key_encoder.h"
#include "testutil/parallel_test.h"

using namespace starrocks;

namespace starrocks {

template <typename DatumType>
void test_pk_dump(PrimaryIndex* pk_index, const std::map<std::string, uint64_t>& current_index_stat) {
    std::srand(static_cast<unsigned int>(time(nullptr)));
    std::string kPrimaryIndexDumpDir = "./PrimaryIndexTest_test_index_dump_" + std::to_string(std::rand()) + "_" +
                                       std::to_string(static_cast<int64_t>(pthread_self()));
    std::string kPrimaryIndexDumpFile = kPrimaryIndexDumpDir + "/111.pkdump";
    bool created;
    FileSystem* fs = FileSystem::Default();
    ASSERT_TRUE(fs->create_dir_if_missing(kPrimaryIndexDumpDir, &created).ok());
    PrimaryKeyDumpPB dump_pb;
    {
        // dump primary index
        PrimaryKeyDump dump(kPrimaryIndexDumpFile);
        ASSERT_TRUE(dump.init_dump_file().ok());
        ASSERT_TRUE(pk_index->pk_dump(&dump, dump_pb.mutable_primary_index()).ok());
    }
    {
        // read primary index dump
        ASSERT_TRUE(PrimaryKeyDump::deserialize_pkcol_pkindex_from_meta(
                            kPrimaryIndexDumpFile, dump_pb, [&](const starrocks::Chunk& chunk) {},
                            [&](const std::string& filename, const starrocks::PartialKVsPB& kvs) {
                                for (int i = 0; i < kvs.keys_size(); i++) {
                                    auto search =
                                            current_index_stat.find(hexdump(kvs.keys(i).data(), kvs.keys(i).size()));
                                    ASSERT_TRUE(search != current_index_stat.end());
                                    ASSERT_TRUE(search->second == kvs.values(i));
                                }
                            })
                            .ok());
    }
    ASSERT_TRUE(fs::remove_all(kPrimaryIndexDumpDir).ok());
}

template <LogicalType field_type, typename DatumType>
void test_integral_pk() {
    auto f = std::make_shared<Field>(0, "c0", field_type, false);
    f->set_is_key(true);
    auto schema = std::make_shared<Schema>(Fields{f}, PRIMARY_KEYS, std::vector<ColumnId>{0});
    auto pk_index = TEST_create_primary_index(*schema);

    constexpr int kSegmentSize = 20;

    auto chunk = ChunkHelper::new_chunk(*schema, kSegmentSize);

    auto pk_col = down_cast<FixedLengthColumn<DatumType>*>(chunk->get_column_by_index(0).get());
    pk_col->resize(kSegmentSize);
    auto pk_data = pk_col->get_data().data();
    DatumType pk_value = 0;
    std::map<std::string, uint64_t> current_index_stat;

    // [0, kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
        current_index_stat[hexdump(reinterpret_cast<const char*>(&pk_data[i]), sizeof(DatumType))] = i;
    }
    ASSERT_TRUE(pk_index->insert(0, 0, *pk_col).ok());

    // [kSegmentSize, 2*kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
        current_index_stat[hexdump(reinterpret_cast<const char*>(&pk_data[i]), sizeof(DatumType))] =
                (((uint64_t)1) << 32) + i;
    }
    ASSERT_TRUE(pk_index->insert(1, 0, *pk_col).ok());

    // [2*kSegmentSize, 3*kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
        current_index_stat[hexdump(reinterpret_cast<const char*>(&pk_data[i]), sizeof(DatumType))] =
                (((uint64_t)2) << 32) + i;
    }
    ASSERT_TRUE(pk_index->insert(2, 0, *pk_col).ok());

    {
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, 2);
            if (rssid != static_cast<uint32_t>(-1)) {
                uint32_t rowid = v & ROWID_MASK;
                CHECK_EQ(rowid, i);
            }
        }
    }

    test_pk_dump<DatumType>(pk_index.get(), current_index_stat);

    PrimaryIndex::DeletesMap deletes;

    // [3*kSegmentSize, 4*kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
    }
    pk_index->upsert(3, 0, *pk_col, &deletes);
    CHECK_EQ(0, deletes.size());

    // upsert all the even numbers in range [0, 2 * kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = i * 2;
    }
    pk_index->upsert(4, 0, *pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    CHECK(deletes.find(0) != deletes.end());
    CHECK(deletes.find(1) != deletes.end());
    CHECK_EQ(kSegmentSize / 2, deletes[0].size());
    CHECK_EQ(kSegmentSize / 2, deletes[1].size());
    for (auto i = 0; i < deletes[0].size(); i++) {
        CHECK_EQ(i * 2, deletes[0][i]);
    }
    for (auto i = 0; i < deletes[1].size(); i++) {
        CHECK_EQ(i * 2, deletes[1][i]);
    }

    // replace, range [0, 2 * kSegmentSize]
    std::vector<uint32_t> replace_indexes;
    for (uint32_t i = 0; i < kSegmentSize; i++) {
        replace_indexes.push_back(i);
    }
    ASSERT_TRUE(pk_index->replace(5, 0, replace_indexes, *pk_col).ok());
    {
        // check result
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, 5);
        }
    }

    // remove all odd numbers in range [2 * kSegmentSize, 4 * kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = 2 * kSegmentSize + i * 2 + 1;
    }
    deletes.clear();
    pk_index->erase(*pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    CHECK(deletes.find(2) != deletes.end());
    CHECK(deletes.find(2) != deletes.end());
    CHECK_EQ(kSegmentSize / 2, deletes[2].size());
    CHECK_EQ(kSegmentSize / 2, deletes[3].size());
    for (auto i = 0; i < deletes[2].size(); i++) {
        CHECK_EQ(i * 2 + 1, deletes[2][i]);
    }
    for (auto i = 0; i < deletes[3].size(); i++) {
        CHECK_EQ(i * 2 + 1, deletes[3][i]);
    }
}

PARALLEL_TEST(PrimaryIndexTest, test_tinyint) {
    test_integral_pk<TYPE_TINYINT, int8_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_smallint) {
    test_integral_pk<TYPE_SMALLINT, int16_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_int) {
    test_integral_pk<TYPE_INT, int32_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_bigint) {
    test_integral_pk<TYPE_BIGINT, int64_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_largeint) {
    test_integral_pk<TYPE_LARGEINT, __int128>();
}

template <LogicalType field_type>
void test_binary_pk(int key_size) {
    std::string fill_str(key_size, 'a');
    auto f = std::make_shared<Field>(0, "c0", field_type, false);
    f->set_is_key(true);
    auto schema = std::make_shared<Schema>(Fields{f}, PRIMARY_KEYS, std::vector<ColumnId>{0});
    auto pk_index = TEST_create_primary_index(*schema);

    constexpr int kSegmentSize = 20;

    auto chunk = ChunkHelper::new_chunk(*schema, kSegmentSize);
    size_t pk_value = 0;

    auto pk_col = down_cast<BinaryColumn*>(chunk->get_column_by_index(0).get());
    pk_col->reserve(kSegmentSize);

    // [0, kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, pk_value++));
    }
    ASSERT_TRUE(pk_index->insert(0, 0, *pk_col).ok());

    std::map<std::string, uint64_t> current_index_stat;
    auto* keys = reinterpret_cast<const Slice*>(pk_col->raw_data());
    for (int i = 0; i < pk_col->size(); i++) {
        current_index_stat[hexdump(keys[i].data, keys[i].size)] = i;
    }

    // [kSegmentSize, 2*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, pk_value++));
    }
    ASSERT_TRUE(pk_index->insert(1, 0, *pk_col).ok());
    keys = reinterpret_cast<const Slice*>(pk_col->raw_data());
    for (int i = 0; i < pk_col->size(); i++) {
        current_index_stat[hexdump(keys[i].data, keys[i].size)] = (((uint64_t)1) << 32) + i;
    }

    // [2*kSegmentSize, 3*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, pk_value++));
    }
    ASSERT_TRUE(pk_index->insert(2, 0, *pk_col).ok());
    keys = reinterpret_cast<const Slice*>(pk_col->raw_data());
    for (int i = 0; i < pk_col->size(); i++) {
        current_index_stat[hexdump(keys[i].data, keys[i].size)] = (((uint64_t)2) << 32) + i;
    }

    {
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, 2);
            if (rssid != static_cast<uint32_t>(-1)) {
                uint32_t rowid = v & ROWID_MASK;
                CHECK_EQ(rowid, i);
            }
        }
    }

    PrimaryIndex::DeletesMap deletes;

    // [3*kSegmentSize, 4*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, pk_value++));
    }
    pk_index->upsert(3, 0, *pk_col, &deletes);
    CHECK_EQ(0, deletes.size());

    // upsert all the even numbers in range [0, 2 * kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, i * 2));
    }
    pk_index->upsert(4, 0, *pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    CHECK(deletes.find(0) != deletes.end());
    CHECK(deletes.find(1) != deletes.end());
    CHECK_EQ(kSegmentSize / 2, deletes[0].size());
    CHECK_EQ(kSegmentSize / 2, deletes[1].size());
    for (auto i = 0; i < deletes[0].size(); i++) {
        CHECK_EQ(i * 2, deletes[0][i]);
    }
    for (auto i = 0; i < deletes[1].size(); i++) {
        CHECK_EQ(i * 2, deletes[1][i]);
    }

    // replace all the odd numbers by indexes, range [0, 2 * kSegmentSize]
    pk_col->resize(0);
    std::vector<uint32_t> replace_indexes;
    for (int i = 0; i < kSegmentSize * 2; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, i));
    }
    for (uint32_t i = 0; i < kSegmentSize; i++) {
        replace_indexes.push_back(i * 2 + 1);
    }
    ASSERT_TRUE(pk_index->replace(5, 0, replace_indexes, *pk_col).ok());
    {
        // check result
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize * 2; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            if (i % 2 == 0) {
                CHECK_EQ(rssid, 4);
            } else {
                CHECK_EQ(rssid, 5);
            }
        }
    }

    // remove all odd numbers in range [2 * kSegmentSize, 4 * kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0_$1", fill_str, 2 * kSegmentSize + i * 2 + 1));
    }
    deletes.clear();
    pk_index->erase(*pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    CHECK(deletes.find(2) != deletes.end());
    CHECK(deletes.find(3) != deletes.end());
    CHECK_EQ(kSegmentSize / 2, deletes[2].size());
    CHECK_EQ(kSegmentSize / 2, deletes[3].size());
    for (auto i = 0; i < deletes[2].size(); i++) {
        CHECK_EQ(i * 2 + 1, deletes[2][i]);
    }
    for (auto i = 0; i < deletes[3].size(); i++) {
        CHECK_EQ(i * 2 + 1, deletes[3][i]);
    }
}

PARALLEL_TEST(PrimaryIndexTest, test_varchar1) {
    test_binary_pk<TYPE_VARCHAR>(1);
}

PARALLEL_TEST(PrimaryIndexTest, test_varchar64) {
    test_binary_pk<TYPE_VARCHAR>(64);
}

PARALLEL_TEST(PrimaryIndexTest, test_composite_key) {
    auto f1 = std::make_shared<Field>(0, "c0", TYPE_TINYINT, false);
    f1->set_is_key(true);
    auto f2 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    f2->set_is_key(true);
    auto schema = std::make_shared<Schema>(Fields{f1, f2}, PRIMARY_KEYS, std::vector<ColumnId>{0, 1});
    auto pk_index = TEST_create_primary_index(*schema);

    constexpr int kSegmentSize = 100;

    auto chunk = ChunkHelper::new_chunk(*schema, kSegmentSize);

    auto pk_col0 = down_cast<FixedLengthColumn<int8_t>*>(chunk->get_column_by_index(0).get());
    auto pk_col1 = down_cast<FixedLengthColumn<int16_t>*>(chunk->get_column_by_index(1).get());

    pk_col0->resize(0);
    pk_col1->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col0->append(i);
        pk_col1->append(i * 2);
    }

    MutableColumnPtr pk_column;
    PrimaryKeyEncoder::create_column(*schema, &pk_column);
    PrimaryKeyEncoder::encode(*schema, *chunk, 0, chunk->num_rows(), pk_column.get());

    ASSERT_TRUE(pk_index->insert(0, 0, *pk_column).ok());
    LOG(INFO) << "pk_index memory:" << pk_index->memory_usage();

    PrimaryIndex::DeletesMap deletes;
    pk_index->upsert(1, 0, *pk_column, &deletes);
    ASSERT_EQ(deletes.size(), 1);
    ASSERT_EQ(deletes[0].size(), kSegmentSize);

    std::vector<uint32_t> replace_indexes;
    for (uint32_t i = 0; i < kSegmentSize; i++) {
        replace_indexes.push_back(i);
    }
    ASSERT_TRUE(pk_index->replace(2, 0, replace_indexes, *pk_column).ok());
    {
        std::vector<uint64_t> rowids(pk_column->size());
        pk_index->get(*pk_column, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, 2);
        }
    }

    deletes.clear();
    pk_index->erase(*pk_column, &deletes);
    ASSERT_EQ(deletes.size(), 1);
    ASSERT_EQ(deletes[2].size(), kSegmentSize);

    ASSERT_TRUE(pk_index->replace(3, 0, replace_indexes, *pk_column).ok());
}

// TODO: test composite primary key

} // namespace starrocks
