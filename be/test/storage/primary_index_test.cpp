// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/primary_index.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "testutil/parallel_test.h"

using namespace starrocks::vectorized;

namespace starrocks {

template <FieldType field_type, typename DatumType>
void test_integral_pk() {
    auto f = std::make_shared<vectorized::Field>(0, "c0", field_type, false);
    f->set_is_key(true);
    auto schema = std::make_shared<vectorized::Schema>(Fields{f}, PRIMARY_KEYS, std::vector<ColumnId>{0});
    auto pk_index = TEST_create_primary_index(*schema);

    constexpr int kSegmentSize = 20;

    auto chunk = ChunkHelper::new_chunk(*schema, kSegmentSize);

    auto pk_col = down_cast<FixedLengthColumn<DatumType>*>(chunk->get_column_by_index(0).get());
    pk_col->resize(kSegmentSize);
    auto pk_data = pk_col->get_data().data();
    DatumType pk_value = 0;

    // [0, kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
    }
    ASSERT_TRUE(pk_index->insert(0, 0, *pk_col).ok());

    // [kSegmentSize, 2*kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
    }
    ASSERT_TRUE(pk_index->insert(1, 0, *pk_col).ok());

    // [2*kSegmentSize, 3*kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = pk_value++;
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

    // remove all odd numbers in range [2 * kSegmentSize, 4 * kSegmentSize)
    for (int i = 0; i < kSegmentSize; i++) {
        pk_data[i] = 2 * kSegmentSize + i * 2 + 1;
    }
    deletes.clear();
    pk_index->erase(*pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    {
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, -1);
        }
    }

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
    test_integral_pk<OLAP_FIELD_TYPE_TINYINT, int8_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_smallint) {
    test_integral_pk<OLAP_FIELD_TYPE_SMALLINT, int16_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_int) {
    test_integral_pk<OLAP_FIELD_TYPE_INT, int32_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_bigint) {
    test_integral_pk<OLAP_FIELD_TYPE_BIGINT, int64_t>();
}

PARALLEL_TEST(PrimaryIndexTest, test_largeint) {
    test_integral_pk<OLAP_FIELD_TYPE_LARGEINT, __int128>();
}

template <FieldType field_type>
void test_binary_pk() {
    auto f = std::make_shared<vectorized::Field>(0, "c0", field_type, false);
    f->set_is_key(true);
    auto schema = std::make_shared<vectorized::Schema>(Fields{f}, PRIMARY_KEYS, std::vector<ColumnId>{0});
    auto pk_index = TEST_create_primary_index(*schema);

    constexpr int kSegmentSize = 20;

    auto chunk = ChunkHelper::new_chunk(*schema, kSegmentSize);
    size_t pk_value = 0;

    auto pk_col = down_cast<BinaryColumn*>(chunk->get_column_by_index(0).get());
    pk_col->reserve(kSegmentSize);

    // [0, kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", pk_value++));
    }
    ASSERT_TRUE(pk_index->insert(0, 0, *pk_col).ok());

    // [kSegmentSize, 2*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", pk_value++));
    }
    ASSERT_TRUE(pk_index->insert(1, 0, *pk_col).ok());

    // [2*kSegmentSize, 3*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", pk_value++));
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

    PrimaryIndex::DeletesMap deletes;

    // [3*kSegmentSize, 4*kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", pk_value++));
    }
    pk_index->upsert(3, 0, *pk_col, &deletes);
    CHECK_EQ(0, deletes.size());

    // upsert all the even numbers in range [0, 2 * kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", i * 2));
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

    // remove all odd numbers in range [2 * kSegmentSize, 4 * kSegmentSize)
    pk_col->resize(0);
    for (int i = 0; i < kSegmentSize; i++) {
        pk_col->append(strings::Substitute("binary_pk_$0", 2 * kSegmentSize + i * 2 + 1));
    }
    deletes.clear();
    pk_index->erase(*pk_col, &deletes);
    CHECK_EQ(2, deletes.size());

    {
        std::vector<uint64_t> rowids(pk_col->size());
        pk_index->get(*pk_col, &rowids);
        for (uint32_t i = 0; i < kSegmentSize; i++) {
            uint64_t v = rowids[i];
            uint32_t rssid = v >> 32;
            CHECK_EQ(rssid, -1);
        }
    }

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

PARALLEL_TEST(PrimaryIndexTest, test_varchar) {
    test_binary_pk<OLAP_FIELD_TYPE_VARCHAR>();
}

PARALLEL_TEST(PrimaryIndexTest, test_composite_key) {
    auto f1 = std::make_shared<vectorized::Field>(0, "c0", OLAP_FIELD_TYPE_TINYINT, false);
    f1->set_is_key(true);
    auto f2 = std::make_shared<vectorized::Field>(1, "c1", OLAP_FIELD_TYPE_SMALLINT, false);
    f2->set_is_key(true);
    auto schema = std::make_shared<vectorized::Schema>(Fields{f1, f2}, PRIMARY_KEYS, std::vector<ColumnId>{0, 1});
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

    std::unique_ptr<vectorized::Column> pk_column;
    PrimaryKeyEncoder::create_column(*schema, &pk_column);
    PrimaryKeyEncoder::encode(*schema, *chunk, 0, chunk->num_rows(), pk_column.get());

    ASSERT_TRUE(pk_index->insert(0, 0, *pk_column).ok());
    LOG(INFO) << "pk_index memory:" << pk_index->memory_usage() << " capacity:" << pk_index->capacity();

    PrimaryIndex::DeletesMap deletes;
    pk_index->upsert(1, 0, *pk_column, &deletes);
    ASSERT_EQ(deletes.size(), 1);
    ASSERT_EQ(deletes[0].size(), kSegmentSize);

    deletes.clear();
    pk_index->erase(*pk_column, &deletes);
    ASSERT_EQ(deletes.size(), 1);
    ASSERT_EQ(deletes[1].size(), kSegmentSize);
}

// TODO: test composite primary key

} // namespace starrocks
