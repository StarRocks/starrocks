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

#include "exec/aggregate/agg_hash_map.h"

#include <gtest/gtest.h>

#include <any>
<<<<<<< HEAD
=======
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <string>
>>>>>>> 999418c8919... [Enhancement] Feed any_value only the rows that create a group (#64572)

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/value_generator.h"

namespace starrocks {

template <typename T>
std::vector<T> get_keys() {
    std::vector<T> keys(10);
    for (int i = 0; i < 10; i++) {
        keys[i] = DefaultValueGenerator<T>::next_value();
    }
    return keys;
}

template <>
std::vector<int32_t> get_keys<int32_t>() {
    std::vector<int32_t> keys(10);
    for (int i = 0; i < 10; i++) {
        keys[i] = i;
    }
    return keys;
};

std::any it_any;

template <typename HashMap, typename key_type>
void exec(HashMap& hash_map, std::vector<key_type> keys) {
    std::vector<int64_t> sums(10);
    for (int i = 0; i < 10; i++) {
        sums[i] = 1000 + i;
    }

    for (int32_t i = 0; i < 10; i++) {
        key_type key = keys[i];
        auto agg_data = (AggDataPtr)(&sums[i]);
        hash_map.emplace(key, agg_data);
    }

    it_any = hash_map.begin();

    using Iterator = typename HashMap::iterator;
    auto it = std::any_cast<Iterator>(it_any);

    auto end = hash_map.end();
    while (it != end) {
        std::cout << it->first << " value is " << *(int64_t*)it->second << "\n";
        it++;
    }
}

TEST(HashMapTest, Basic) {
    std::vector<AggHashMapVariant::Type> hash_map_types = {
            AggHashMapVariant::Type::phase1_string, AggHashMapVariant::Type::phase2_string,
            AggHashMapVariant::Type::phase1_int32, AggHashMapVariant::Type::phase2_int32,
            AggHashMapVariant::Type::phase1_int32_two_level};
    for (const auto& hash_map_type : hash_map_types) {
        std::any it_any;

        RuntimeState dummy;
        RuntimeProfile profile("dummy");
        AggStatistics statis(&profile);

        AggHashMapVariant variant;
        variant.init(&dummy, hash_map_type, &statis);
        variant.visit([](auto& hash_map_with_key) {
            if constexpr (std::is_same_v<typename decltype(hash_map_with_key->hash_map)::key_type, int32_t>) {
                exec(hash_map_with_key->hash_map, get_keys<int32_t>());
            } else if constexpr (std::is_same_v<typename decltype(hash_map_with_key->hash_map)::key_type, Slice>) {
                exec(hash_map_with_key->hash_map, get_keys<Slice>());
            } else {
                ASSERT_TRUE(false);
            }
        });
    }
}

TEST(HashMapTest, Insert) {
    // Test AggHashMapWithSerializedKeyFixedSize
    {
        const int chunk_size = 64;
        using TestAggHashMap = FixedSize16SliceAggHashMap<PhmapSeed1>;
        using TestAggHashMapKey = AggHashMapWithSerializedKeyFixedSize<TestAggHashMap>;
        RuntimeProfile profile("dummy");
        AggStatistics statis(&profile);
        TestAggHashMapKey key(chunk_size, &statis);
        key.has_null_column = true;
        key.fixed_byte_size = 8;
        MemPool pool;
        // chunk size
        // key columns
        const int num_rows = 32;
        std::vector<std::pair<LogicalType, bool>> types = {{TYPE_INT, true}, {TYPE_INT, false}};
        MutableColumns key_columns_mut;
        Buffer<AggDataPtr> agg_states(chunk_size);
        for (const auto& type : types) {
            key_columns_mut.emplace_back(ColumnHelper::create_column(TypeDescriptor(type.first), type.second));
            for (int i = 0; i < num_rows; ++i) {
                key_columns_mut.back()->append_datum(Datum(rand() % 16000));
            }
            key_columns_mut.back()->append_default();
        }
        Columns key_columns = ColumnHelper::to_columns(std::move(key_columns_mut));
        auto allocate_func = [&pool](auto& key) { return pool.allocate(16); };
        key.build_hash_map(key_columns[0]->size(), key_columns, &pool, allocate_func, &agg_states);
        using TestHashMapKey = TestAggHashMap::key_type;
        std::vector<TestHashMapKey> resv;
        for (auto [key, _] : key.hash_map) {
            resv.emplace_back(key);
        }
        MutableColumns res_columns;
        for (const auto& type : types) {
            res_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(type.first), type.second));
        }
        key.insert_keys_to_columns(resv, res_columns, resv.size());
        auto& l = down_cast<Int32Column*>(down_cast<NullableColumn*>(res_columns[0].get())->data_column_raw_ptr())
                          ->get_data();
        auto& r = down_cast<Int32Column*>(
                          down_cast<NullableColumn*>(key_columns[0]->as_mutable_raw_ptr())->data_column_raw_ptr())
                          ->get_data();
        std::set<int32_t> keys_sets;
        for (int& i : r) {
            keys_sets.insert(i);
        }
        std::set<int32_t> res_sets;
        for (int& i : l) {
            res_sets.insert(i);
        }
        ASSERT_EQ(res_sets.size(), keys_sets.size());
    }
}

TEST(HashMapTest, TwoLevelConvert) {
    std::vector<std::string> keys(1000);
    for (int i = 0; i < 1000; i++) {
        keys[i] = std::to_string(i);
    }

    SliceAggHashSet<PhmapSeed1> set;
    SliceAggTwoLevelHashSet<PhmapSeed1> two_level_set;

    for (auto& key : keys) {
        Slice slice = {key.data(), key.size()};
        set.emplace(slice);
    }
    two_level_set.insert(set.begin(), set.end());

    ASSERT_EQ(set.size(), two_level_set.size());
    for (const auto& key : set) {
        ASSERT_TRUE(two_level_set.contains(key));
    }
}

class AggHashMapKeyNotFoundsTest : public ::testing::Test {
public:
    template <typename HashMapWithKey>
    struct TestAllocateState {
        TestAllocateState(MemPool* pool) : _pool(pool) {}
        AggDataPtr operator()(const typename HashMapWithKey::KeyType& key) { return _pool->allocate(16); }
        AggDataPtr operator()(std::nullptr_t) { return _pool->allocate(16); }
        MemPool* _pool;
    };

    template <typename CppType>
    ColumnPtr CreateColumnWithType(LogicalType type, const std::vector<CppType>& datas, bool nullable) {
        auto col = ColumnHelper::create_column(TypeDescriptor(type), nullable);
        for (auto& data : datas) {
            if (type == LogicalType::TYPE_INT) {
                col->append_datum(data);
            } else if (type == LogicalType::TYPE_VARCHAR) {
                col->append_datum(data);
            } else {
                throw std::runtime_error("Unsupported type:" + std::to_string(type));
            }
        }
        return col;
    };

    void CheckNotFounds(const Filter& not_founds, const std::vector<uint8_t>& exp_datas) {
        DCHECK_EQ(not_founds.size(), exp_datas.size());
        for (auto i = 0; i < not_founds.size(); i++) {
            VLOG_ROW << "i:" << i << ", not_found:" << (int)not_founds[i] << ", expect:" << (int)exp_datas[i];
            DCHECK_EQ(not_founds[i], exp_datas[i]);
        }
    };

    template <typename TestAggHashMapKey, typename CppType>
    void TestAggHashMapAllocateAndComputeNonFounds(LogicalType type, bool nullable,
                                                   std::vector<std::vector<CppType>> test_datas,
                                                   std::vector<std::vector<uint8_t>> expect_not_founds) {
        RuntimeProfile profile("TestAggHashMapAllocateAndComputeNonFounds");
        AggStatistics statis(&profile);

        const auto chunk_size = 4;
        TestAggHashMapKey key(chunk_size, &statis);
        Buffer<AggDataPtr> agg_states(chunk_size);
        MemPool pool;
        Filter not_founds;

        // For fixed size key, need set key's fixed size
        if constexpr (std::is_same_v<TestAggHashMapKey,
                                     AggHashMapWithSerializedKeyFixedSize<FixedSize16SliceAggHashMap<PhmapSeed1>>>) {
            key.fixed_byte_size = sizeof(CppType);
            key.has_null_column = nullable;
        }

        {
            Columns key_columns;
            key_columns.emplace_back(CreateColumnWithType<CppType>(type, test_datas[0], nullable));
            key.build_hash_map(key_columns[0]->size(), key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool),
                               &agg_states);
            DCHECK_EQ(not_founds.size(), 0);
            CheckNotFounds(not_founds, {});
        }

        {
            Columns key_columns;
            key_columns.emplace_back(CreateColumnWithType<CppType>(type, test_datas[1], nullable));
            key.build_hash_map_with_selection(key_columns[0]->size(), key_columns, &pool,
                                              TestAllocateState<TestAggHashMapKey>(&pool), &agg_states, &not_founds);
            CheckNotFounds(not_founds, expect_not_founds[1]);
        }

        {
            Columns key_columns;
            key_columns.emplace_back(CreateColumnWithType<CppType>(type, test_datas[2], nullable));
            key.build_hash_map_with_selection_and_allocation(key_columns[0]->size(), key_columns, &pool,
                                                             TestAllocateState<TestAggHashMapKey>(&pool), &agg_states,
                                                             &not_founds);
            CheckNotFounds(not_founds, expect_not_founds[2]);
        }

        {
            Columns key_columns;
            key_columns.emplace_back(CreateColumnWithType<CppType>(type, test_datas[3], nullable));
            key.build_hash_map_with_selection(key_columns[0]->size(), key_columns, &pool,
                                              TestAllocateState<TestAggHashMapKey>(&pool), &agg_states, &not_founds);
            CheckNotFounds(not_founds, expect_not_founds[3]);
        }

        {
            Columns key_columns;
            key_columns.emplace_back(CreateColumnWithType<CppType>(type, test_datas[4], nullable));
            key.build_hash_map_with_selection_and_allocation(key_columns[0]->size(), key_columns, &pool,
                                                             TestAllocateState<TestAggHashMapKey>(&pool), &agg_states,
                                                             &not_founds);
            CheckNotFounds(not_founds, expect_not_founds[4]);
        }
    }

    template <typename TestAggHashMapKey>
    void TestAggHashMapKeyWithIntType(int nullable) {
        TestAggHashMapAllocateAndComputeNonFounds<TestAggHashMapKey, int32_t>(LogicalType::TYPE_INT, nullable,
                                                                              Int32TestData, ExpectNotFoundsData);
    }

    template <typename TestAggHashMapKey>
    void TestAggHashMapKeyWithStringType(int nullable) {
        TestAggHashMapAllocateAndComputeNonFounds<TestAggHashMapKey, Slice>(LogicalType::TYPE_VARCHAR, nullable,
                                                                            StringTestData, ExpectNotFoundsData);
    }

protected:
    std::vector<std::vector<int32_t>> Int32TestData{
            {1, 2, 1, 1}, {1, 2, 1, 1}, {1, 2, 3, 3}, {4, 4, 4, 4}, {5, 5, 5, 5}};
    std::vector<std::vector<Slice>> StringTestData{{"1", "2", "1", "1"},
                                                   {"1", "2", "1", "1"},
                                                   {"1", "2", "3", "3"},
                                                   {"4", "4", "4", "4"},
                                                   {"5", "5", "5", "5"}};
    std::vector<std::vector<uint8_t>> ExpectNotFoundsData{{}, {0, 0, 0, 0}, {0, 0, 1, 0}, {1, 1, 1, 1}, {1, 0, 0, 0}};
};

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_Int32AggHashMapWithOneNumberKey) {
    using TestAggHashMapKey = Int32AggHashMapWithOneNumberKey<PhmapSeed1>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(false);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_NullInt32AggHashMapWithOneNumberKey) {
    using TestAggHashMapKey = NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_OneStringAggHashMap) {
    using TestAggHashMapKey = OneStringAggHashMap<PhmapSeed1>;
    TestAggHashMapKeyWithStringType<TestAggHashMapKey>(false);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_NullOneStringAggHashMap) {
    using TestAggHashMapKey = NullOneStringAggHashMap<PhmapSeed2>;
    TestAggHashMapKeyWithStringType<TestAggHashMapKey>(true);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_AggHashMapWithSerializedKey) {
    using TestAggHashMapKey = SerializedKeyAggHashMap<PhmapSeed1>;
    TestAggHashMapKeyWithStringType<TestAggHashMapKey>(true);
}

<<<<<<< HEAD
=======
// build_hash_map_marking_new_groups must mark exactly the rows that created a group, including the row that
// created the NULL-key group: the aggregator feeds first-value-only functions (any_value) only these rows.
class AggHashMapNewGroupsTest : public ::testing::Test {
protected:
    template <typename HashMapWithKey>
    struct AllocateState {
        explicit AllocateState(MemPool* pool) : _pool(pool) {}
        AggDataPtr operator()(const typename HashMapWithKey::KeyType& key) { return _pool->allocate(16); }
        AggDataPtr operator()(std::nullptr_t) { return _pool->allocate(16); }
        MemPool* _pool;
    };

    // nullopt = NULL key.
    using Chunks = std::vector<std::vector<std::optional<int32_t>>>;

    // A nullable chunk of NULLs only is passed as an only-null const column when `only_null_as_const`.
    template <typename HashMapWithKey>
    void check(bool nullable, bool string_key, const Chunks& chunks, bool only_null_as_const = true) {
        RuntimeProfile profile("AggHashMapNewGroupsTest");
        AggStatistics statistics(&profile);
        HashMapWithKey key(4096, &statistics);
        MemPool pool;
        std::map<std::optional<int32_t>, AggDataPtr> states;
        for (const auto& chunk : chunks) {
            const size_t rows = chunk.size();
            const bool all_null = only_null_as_const && nullable && rows > 0 &&
                                  std::all_of(chunk.begin(), chunk.end(), [](const auto& v) { return !v; });
            ColumnPtr column;
            if (all_null) {
                column = ColumnHelper::create_const_null_column(rows);
            } else {
                auto c = ColumnHelper::create_column(TypeDescriptor(string_key ? TYPE_VARCHAR : TYPE_INT), nullable);
                for (const auto& v : chunk) {
                    if (!v) {
                        c->append_nulls(1);
                    } else if (string_key) {
                        std::string str = std::to_string(*v);
                        c->append_datum(Datum(Slice(str)));
                    } else {
                        c->append_datum(Datum(*v));
                    }
                }
                column = std::move(c);
            }
            Columns key_columns{column};
            Buffer<AggDataPtr> agg_states(rows);
            Filter new_groups;
            key.build_hash_map_marking_new_groups(rows, key_columns, &pool, AllocateState<HashMapWithKey>(&pool),
                                                  &agg_states, &new_groups);
            ASSERT_EQ(rows, new_groups.size());
            for (size_t i = 0; i < rows; i++) {
                auto [it, inserted] = states.emplace(chunk[i], agg_states[i]);
                EXPECT_EQ(inserted, new_groups[i] == 1) << "row " << i;
                EXPECT_EQ(it->second, agg_states[i]) << "row " << i;
            }
        }
    }

    // The NULL-key group is created mid-chunk, then only hit again.
    const Chunks _nullable_chunks{{1, 2, 1, std::nullopt, std::nullopt, 3},
                                  {3, std::nullopt, 4, 4, 1},
                                  {5, 6, 5, 6},
                                  {std::nullopt, std::nullopt}};
    // The NULL-key group is created by an only-null chunk.
    const Chunks _only_null_first{{std::nullopt, std::nullopt, std::nullopt}, {1, std::nullopt, 1, 2}};
    const Chunks _non_null_chunks{{1, 2, 1, 1}, {2, 3, 3, 4}, {}, {5, 5, 1, 6}};
};

TEST_F(AggHashMapNewGroupsTest, OneNumberKey) {
    check<Int32AggHashMapWithOneNumberKey<PhmapSeed1>>(false, false, _non_null_chunks);
}

TEST_F(AggHashMapNewGroupsTest, NullableOneNumberKey) {
    check<NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>>(true, false, _nullable_chunks);
    check<NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>>(true, false, _only_null_first);
    check<NullInt32AggHashMapWithOneNumberKey<PhmapSeed1>>(true, false, _non_null_chunks);
}

TEST_F(AggHashMapNewGroupsTest, OneStringKey) {
    check<OneStringAggHashMap<PhmapSeed1>>(false, true, _non_null_chunks);
}

TEST_F(AggHashMapNewGroupsTest, NullableOneStringKey) {
    check<NullOneStringAggHashMap<PhmapSeed1>>(true, true, _nullable_chunks);
    check<NullOneStringAggHashMap<PhmapSeed1>>(true, true, _only_null_first);
}

TEST_F(AggHashMapNewGroupsTest, SerializedKey) {
    check<SerializedKeyAggHashMap<PhmapSeed1>>(true, true, _nullable_chunks, false);
    check<SerializedKeyAggHashMap<PhmapSeed1>>(false, true, _non_null_chunks);
}

// One row as large as LARGE_VARCHAR_LENGTH_THRESHOLD used to force the whole chunk onto the
// per-row path, because the staging buffer was sized max_row_size * chunk_size (> INT32_MAX here).
// Rows are now packed back to back, so the buffer only has to hold the row itself.
TEST_F(AggHashMapKeyNotFoundsTest, SerializedKeyKeepsOneLargeRowOnTheBatchPath) {
    using TestAggHashMapKey = SerializedKeyAggHashMap<PhmapSeed1>;

    RuntimeProfile profile("SerializedKeyKeepsOneLargeRowOnTheBatchPath");
    AggStatistics statistics(&profile);
    constexpr int kEstimatedChunkSize = 4096;
    TestAggHashMapKey key(kEstimatedChunkSize, &statistics);

    std::string large_value(TypeDescriptor::LARGE_VARCHAR_LENGTH_THRESHOLD, 'x');
    auto column = BinaryColumn::create();
    column->append(Slice(large_value));
    Columns key_columns{column};

    Buffer<AggDataPtr> agg_states(1);
    MemPool pool;
    key.build_hash_map(1, key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool), &agg_states);

    EXPECT_TRUE(key.key_buffer.batched());
    EXPECT_EQ(1, key.hash_map.size());
    EXPECT_NE(nullptr, agg_states[0]);
    // The one row plus its length header and the SIMD padding, rounded up to MemPool's power of
    // two -- not 4096 rows' worth.
    EXPECT_LE(key.key_buffer.capacity(), 2 * (large_value.size() + 64));

    Filter not_founds;
    key.build_hash_map_with_selection(1, key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool), &agg_states,
                                      &not_founds);
    ASSERT_EQ(1, not_founds.size());
    EXPECT_EQ(0, not_founds[0]);
}

namespace {

// Three key columns covering the encodings the buffer has to lay out: a nullable string with
// nulls, a fixed-width int, and a string that crosses the compact length header's one-byte limit.
Columns make_mixed_key_columns(size_t rows, size_t long_row, size_t long_len) {
    auto s0 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto i1 = Int32Column::create();
    auto s2 = BinaryColumn::create();
    for (size_t i = 0; i < rows; ++i) {
        if (i % 5 == 3) {
            s0->append_nulls(1);
        } else {
            s0->append_datum(Slice(std::string(i % 7, 'a' + i % 3)));
        }
        i1->append(static_cast<int32_t>(i % 11));
        s2->append(Slice(std::string(i == long_row ? long_len : 1 + i % 300, 'k')));
    }
    return Columns{std::move(s0), std::move(i1), std::move(s2)};
}

size_t max_serialize_size(const Columns& columns) {
    size_t max_size = 0;
    for (const auto& c : columns) max_size += c->max_one_element_serialize_size_compact();
    return max_size;
}

} // namespace

TEST(SerializedKeyBufferTest, PacksRowsBackToBackWithTheRowWiseEncoding) {
    constexpr size_t kRows = 257;
    Columns key_columns = make_mixed_key_columns(kRows, /*long_row=*/100, /*long_len=*/5000);

    SerializedKeyBuffer buffer;
    ASSERT_TRUE(buffer.serialize(key_columns, kRows));
    ASSERT_TRUE(buffer.batched());

    std::vector<uint8_t> expected(max_serialize_size(key_columns) + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    size_t total = 0;
    size_t longest = 0;
    for (size_t i = 0; i < kRows; ++i) {
        // Byte-identical to the per-row fallback, or the two paths would split logical keys.
        uint8_t* cursor = expected.data();
        for (const auto& c : key_columns) cursor += c->serialize_compact(i, cursor);
        const Slice key = buffer.key(i);
        ASSERT_EQ(static_cast<size_t>(cursor - expected.data()), key.size) << "row " << i;
        ASSERT_EQ(0, memcmp(expected.data(), key.data, key.size)) << "row " << i;
        if (i > 0) {
            const Slice prev = buffer.key(i - 1);
            ASSERT_EQ(prev.data + prev.size, key.data) << "rows are not back to back at " << i;
        }
        total += key.size;
        longest = std::max(longest, key.size);
    }
    // Exactly the keys plus padding, rounded up to MemPool's power of two.
    EXPECT_GE(buffer.capacity(), total + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    EXPECT_LT(buffer.capacity(), 2 * (total + SLICE_MEMEQUAL_OVERFLOW_PADDING));
    // The actual longest row, not the sum of per-column maxima.
    EXPECT_EQ(longest, buffer.max_row_size());
}

// All key columns fixed-width (including a nullable column without nulls): no size pass, rows at a
// constant stride. The bytes must still be exactly the per-row encoding.
TEST(SerializedKeyBufferTest, FixedWidthColumnsSkipTheSizePass) {
    constexpr size_t kRows = 100;
    auto a = Int32Column::create();
    auto b = NullableColumn::create(Int64Column::create(), NullColumn::create());
    for (size_t i = 0; i < kRows; ++i) {
        a->append(static_cast<int32_t>(i));
        b->append_datum(static_cast<int64_t>(i * 7));
    }
    Columns key_columns{std::move(a), std::move(b)};
    const uint32_t stride = sizeof(int32_t) + sizeof(bool) + sizeof(int64_t);
    ASSERT_EQ(sizeof(int32_t), key_columns[0]->serialize_batch_fixed_row_size());
    ASSERT_EQ(sizeof(bool) + sizeof(int64_t), key_columns[1]->serialize_batch_fixed_row_size());

    SerializedKeyBuffer buffer;
    ASSERT_TRUE(buffer.serialize(key_columns, kRows));
    EXPECT_EQ(stride, buffer.max_row_size());
    uint8_t expected[64];
    for (size_t i = 0; i < kRows; ++i) {
        uint8_t* cursor = expected;
        for (const auto& c : key_columns) cursor += c->serialize_compact(i, cursor);
        const Slice key = buffer.key(i);
        ASSERT_EQ(stride, key.size) << "row " << i;
        ASSERT_EQ(stride, static_cast<size_t>(cursor - expected)) << "row " << i;
        ASSERT_EQ(0, memcmp(expected, key.data, key.size)) << "row " << i;
    }

    // A null makes the nullable column variable-width again: the size pass runs, same bytes.
    auto c = NullableColumn::create(Int64Column::create(), NullColumn::create());
    for (size_t i = 0; i < kRows; ++i) {
        if (i % 3 == 0) {
            c->append_nulls(1);
        } else {
            c->append_datum(static_cast<int64_t>(i));
        }
    }
    EXPECT_EQ(0u, c->serialize_batch_fixed_row_size());
    Columns with_nulls{key_columns[0], std::move(c)};
    ASSERT_TRUE(buffer.serialize(with_nulls, kRows));
    for (size_t i = 0; i < kRows; ++i) {
        uint8_t* cursor = expected;
        for (const auto& col : with_nulls) cursor += col->serialize_compact(i, cursor);
        const Slice key = buffer.key(i);
        ASSERT_EQ(static_cast<size_t>(cursor - expected), key.size) << "row " << i;
        ASSERT_EQ(0, memcmp(expected, key.data, key.size)) << "row " << i;
    }
}

// Above the batch limit the chunk goes to the caller's per-row path, which sizes its one-row
// buffer from max_row_size() -- so that must be right even when nothing was batched.
TEST(SerializedKeyBufferTest, FallsBackAboveTheBatchLimitAndReportsTheLongestRow) {
    constexpr size_t kRows = 64;
    Columns key_columns = make_mixed_key_columns(kRows, /*long_row=*/9, /*long_len=*/3000);
    std::vector<uint8_t> row(max_serialize_size(key_columns) + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    size_t total = 0;
    size_t longest = 0;
    for (size_t i = 0; i < kRows; ++i) {
        uint8_t* cursor = row.data();
        for (const auto& c : key_columns) cursor += c->serialize_compact(i, cursor);
        total += cursor - row.data();
        longest = std::max<size_t>(longest, cursor - row.data());
    }

    SerializedKeyBuffer buffer;
    buffer.set_max_batch_bytes_for_test(total - 1);
    EXPECT_FALSE(buffer.serialize(key_columns, kRows));
    EXPECT_FALSE(buffer.batched());
    EXPECT_EQ(longest, buffer.max_row_size());

    buffer.set_max_batch_bytes_for_test(total);
    EXPECT_TRUE(buffer.serialize(key_columns, kRows)) << "exactly at the limit still batches";
}

// A single long row in a 4096-row chunk used to size the staging buffer at 4096 * that row;
// now it costs the row once. The results must not change.
TEST_F(AggHashMapKeyNotFoundsTest, SerializedKeyBufferHoldsTheChunkNotMaxRowTimesChunk) {
    using TestAggHashMapKey = SerializedKeyAggHashMap<PhmapSeed1>;
    constexpr size_t kRows = 4096;
    constexpr size_t kLongLen = 100 * 1024;

    RuntimeProfile profile("SerializedKeyBufferHoldsTheChunkNotMaxRowTimesChunk");
    AggStatistics statistics(&profile);
    TestAggHashMapKey key(kRows, &statistics);
    MemPool pool;
    Buffer<AggDataPtr> agg_states(kRows);

    Columns key_columns = make_mixed_key_columns(kRows, /*long_row=*/17, kLongLen);
    key.build_hash_map(kRows, key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool), &agg_states);
    const size_t max_row = max_serialize_size(key_columns);
    EXPECT_GT(max_row, kLongLen);

    // Same distinct keys as a reference set over the per-row encoding.
    std::set<std::string> distinct;
    std::vector<uint8_t> row(max_row + SLICE_MEMEQUAL_OVERFLOW_PADDING);
    size_t total = 0;
    for (size_t i = 0; i < kRows; ++i) {
        uint8_t* cursor = row.data();
        for (const auto& c : key_columns) cursor += c->serialize_compact(i, cursor);
        distinct.emplace(reinterpret_cast<const char*>(row.data()), cursor - row.data());
        total += cursor - row.data();
    }
    // Sized by the chunk's real key bytes (within MemPool's power-of-two rounding), where the
    // stride layout needed max_row * kRows -- ~400 MiB here.
    EXPECT_LT(key.key_buffer.capacity(), 2 * (total + SLICE_MEMEQUAL_OVERFLOW_PADDING));
    EXPECT_LT(key.key_buffer.capacity() * 100, max_row * kRows) << "buffer sized by the long row times the chunk";
    EXPECT_EQ(distinct.size(), key.hash_map.size());

    // A second, short chunk probes the same map through the same (not reallocated) buffer.
    const size_t capacity = key.key_buffer.capacity();
    Filter not_founds;
    key.build_hash_map_with_selection(kRows, key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool),
                                      &agg_states, &not_founds);
    EXPECT_EQ(capacity, key.key_buffer.capacity());
    for (size_t i = 0; i < kRows; ++i) {
        ASSERT_EQ(0, not_founds[i]) << "row " << i;
    }
}

// The set gained a per-row path it did not have (a chunk whose keys exceed INT32_MAX used to be
// staged at max_row_size * chunk_size regardless). It must build exactly the set the batch path does.
TEST(AggHashSetOfSerializedKeyTest, PerRowPathMatchesBatchPath) {
    using TestSet = AggHashSetOfSerializedKey<SliceAggHashSet<PhmapSeed1>>;
    constexpr size_t kRows = 1000;
    Columns key_columns = make_mixed_key_columns(kRows, /*long_row=*/7, /*long_len=*/4000);

    RuntimeProfile profile("PerRowPathMatchesBatchPath");
    AggStatistics statistics(&profile);
    MemPool pool;

    TestSet batch(kRows, &statistics);
    batch.build_set<true>(kRows, key_columns, &pool, nullptr);

    TestSet by_rows(kRows, &statistics);
    by_rows.build_set_by_rows<true>(kRows, key_columns, &pool, nullptr, max_serialize_size(key_columns));

    ASSERT_EQ(batch.hash_set.size(), by_rows.hash_set.size());
    for (const auto& k : batch.hash_set) {
        EXPECT_TRUE(by_rows.hash_set.contains(k)) << "key missing from the per-row build";
    }

    Filter not_founds(kRows, 0); // build_set() sizes this; the direct call does not
    by_rows.build_set_by_rows<false>(kRows, key_columns, &pool, &not_founds, max_serialize_size(key_columns));
    for (size_t i = 0; i < kRows; ++i) {
        ASSERT_EQ(0, not_founds[i]) << "row " << i;
    }
}

// get_max_serialize_size() sizes the staging buffer for serialize_batch(), which writes the
// compact key encoding: a one-byte length prefix for a string that fits in one, not the four-byte
// prefix of the persisted encoding. So the bound comes from max_one_element_serialize_size_compact(),
// and -- the part that actually matters -- it has to cover what serialize_batch() lays down.
TEST(PartitionHashMapSerializedKeyTest, SumsOrdinaryColumnSerializeSizes) {
    PartitionHashMapWithSerializedKey<SlicePartitionHashMap<PhmapSeed1>> partition_map(4);

    auto string_column = BinaryColumn::create();
    string_column->append("abc");
    auto int_column = Int32Column::create();
    int_column->append(7);
    Columns key_columns{string_column, int_column};

    auto size_or = partition_map.get_max_serialize_size(key_columns);
    ASSERT_TRUE(size_or.ok()) << size_or.status();
    EXPECT_EQ(string_column->max_one_element_serialize_size_compact() +
                      int_column->max_one_element_serialize_size_compact(),
              size_or.value());
    // Spelled out: one length byte plus "abc", then a bare int32.
    EXPECT_EQ(static_cast<uint32_t>(1 + 3 + sizeof(int32_t)), size_or.value());

    const uint32_t max_one_row_size = size_or.value();
    Buffer<uint32_t> slice_sizes(1, 0);
    std::vector<uint8_t> buffer(max_one_row_size + 32, 0);
    for (const auto& key_column : key_columns) {
        key_column->serialize_batch(buffer.data(), slice_sizes, 1, max_one_row_size);
    }
    EXPECT_LE(slice_sizes[0], max_one_row_size) << "serialize_batch() overran the bound it was given";
}

>>>>>>> 999418c8919... [Enhancement] Feed any_value only the rows that create a group (#64572)
TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_FixedSize16SliceAggHashMap) {
    using TestAggHashMap = FixedSize16SliceAggHashMap<PhmapSeed1>;
    using TestAggHashMapKey = AggHashMapWithSerializedKeyFixedSize<TestAggHashMap>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

} // namespace starrocks
