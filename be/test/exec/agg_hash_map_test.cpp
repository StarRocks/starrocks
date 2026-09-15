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

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_FixedSize16SliceAggHashMap) {
    using TestAggHashMap = FixedSize16SliceAggHashMap<PhmapSeed1>;
    using TestAggHashMapKey = AggHashMapWithSerializedKeyFixedSize<TestAggHashMap>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

// Direct-array INT GROUP BY wrappers (P1.C): default min_value=0, so the
// existing Int32TestData/{1,2,3,4,5} values fit both uint8 and uint16
// cells unchanged. Reuses the shared not-founds harness so each variant
// exercises the same four build paths the framework already covers.
TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_CompressibleInt32_Uint8) {
    using TestAggHashMapKey = AggHashMapWithOneCompressibleInt32Key<RangeUInt8AggHashMap<PhmapSeed1>>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(false);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_NullCompressibleInt32_Uint8) {
    using TestAggHashMapKey = AggHashMapWithOneNullableCompressibleInt32Key<RangeUInt8AggHashMap<PhmapSeed1>>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_CompressibleInt32_Uint16) {
    using TestAggHashMapKey = AggHashMapWithOneCompressibleInt32Key<RangeUInt16AggHashMap<PhmapSeed1>>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(false);
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_NullCompressibleInt32_Uint16) {
    using TestAggHashMapKey = AggHashMapWithOneNullableCompressibleInt32Key<RangeUInt16AggHashMap<PhmapSeed1>>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

// ============================================================================
// Round-trip + min_value handling for the compressible-int wrapper.
// ============================================================================
// Drives build_hash_map -> insert_keys_to_columns and asserts that the
// stored uint8/uint16 keys correctly restore to (key + min_value) in the
// output Int32Column. Two min_value setups: zero (canonical case) and
// INT32_MIN (overflow-safe path: unsigned narrowing must not wrap).
class CompressibleInt32KeyRoundTripTest : public ::testing::Test {
protected:
    template <typename KeyT>
    void run(int32_t min_value, const std::vector<int32_t>& input) {
        using HashMap = std::conditional_t<std::is_same_v<KeyT, uint8_t>, RangeUInt8AggHashMap<PhmapSeed1>,
                                           RangeUInt16AggHashMap<PhmapSeed1>>;
        using KeyImpl = AggHashMapWithOneCompressibleInt32Key<HashMap>;

        RuntimeProfile profile("CompressibleInt32KeyRoundTripTest");
        AggStatistics statis(&profile);
        const int chunk_size = static_cast<int>(input.size());
        KeyImpl key(chunk_size, &statis);
        key.set_min(min_value);

        // Build a single-column Int32 input chunk.
        MutableColumns key_columns_mut;
        key_columns_mut.emplace_back(ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), false));
        for (int32_t v : input) {
            key_columns_mut.back()->append_datum(Datum(v));
        }
        Columns key_columns = ColumnHelper::to_columns(std::move(key_columns_mut));

        MemPool pool;
        Buffer<AggDataPtr> agg_states(chunk_size);
        auto allocate_func = [&pool](auto&) { return pool.allocate(16); };
        key.build_hash_map(chunk_size, key_columns, &pool, allocate_func, &agg_states);

        // Collect distinct keys in insertion order, then run them back
        // through insert_keys_to_columns and check restored values match
        // the input set. ResultVector uses ColumnAllocator (not the
        // std default), so use the wrapper's own alias. The fixed-map
        // iterator exposes only operator->, so iterate explicitly
        // instead of using a structured-binding range-for.
        typename KeyImpl::ResultVector resv;
        std::set<int32_t> expected_distinct(input.begin(), input.end());
        for (auto it = key.hash_map.begin(); it != key.hash_map.end(); ++it) {
            resv.emplace_back(it->first);
        }
        ASSERT_EQ(resv.size(), expected_distinct.size());

        MutableColumns res_columns;
        res_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), false));
        key.insert_keys_to_columns(resv, res_columns, resv.size());

        auto& restored = down_cast<Int32Column*>(res_columns[0].get())->get_data();
        std::set<int32_t> restored_set(restored.begin(), restored.end());
        ASSERT_EQ(restored_set, expected_distinct) << "min_value=" << min_value << ", input_size=" << input.size();
    }
};

TEST_F(CompressibleInt32KeyRoundTripTest, Uint8_MinZero) {
    run<uint8_t>(0, {0, 1, 2, 1, 255, 100, 255});
}

TEST_F(CompressibleInt32KeyRoundTripTest, Uint8_MinPositive) {
    run<uint8_t>(1000, {1000, 1001, 1002, 1000, 1255, 1100});
}

// INT32_MIN edge: (val - min) must use unsigned arithmetic so that
// INT32_MIN - INT32_MIN = 0 in uint8, not signed overflow.
TEST_F(CompressibleInt32KeyRoundTripTest, Uint8_MinInt32) {
    run<uint8_t>(INT32_MIN, {INT32_MIN, INT32_MIN + 1, INT32_MIN + 2, INT32_MIN + 255});
}

TEST_F(CompressibleInt32KeyRoundTripTest, Uint16_MinZero) {
    std::vector<int32_t> vals;
    for (int32_t v : {0, 1, 100, 1000, 65535, 30000, 1000}) {
        vals.push_back(v);
    }
    run<uint16_t>(0, vals);
}

TEST_F(CompressibleInt32KeyRoundTripTest, Uint16_MinNegative) {
    run<uint16_t>(-32768, {-32768, -32767, 0, 32767, -32768});
}

TEST_F(CompressibleInt32KeyRoundTripTest, Uint16_MinInt32) {
    run<uint16_t>(INT32_MIN, {INT32_MIN, INT32_MIN + 1, INT32_MIN + 65535, INT32_MIN + 32768});
}

// Nullable path: only_null short-circuit + has_null mixed input.
class NullCompressibleInt32KeyTest : public ::testing::Test {};

TEST_F(NullCompressibleInt32KeyTest, OnlyNullShortCircuit) {
    using KeyImpl = AggHashMapWithOneNullableCompressibleInt32Key<RangeUInt8AggHashMap<PhmapSeed1>>;
    RuntimeProfile profile("NullCompressibleInt32KeyTest");
    AggStatistics statis(&profile);
    const int chunk_size = 4;
    KeyImpl key(chunk_size, &statis);
    key.set_min(0);

    // Construct an only_null nullable Int32 column.
    auto col = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
    col->append_nulls(chunk_size);
    Columns key_columns;
    key_columns.emplace_back(std::move(col));

    MemPool pool;
    Buffer<AggDataPtr> agg_states(chunk_size);
    auto allocate_func = [&pool](auto&&...) { return pool.allocate(16); };
    key.build_hash_map(chunk_size, key_columns, &pool, allocate_func, &agg_states);

    // All four rows must resolve to the single null_key_data slot.
    ASSERT_NE(agg_states[0], nullptr);
    for (int i = 1; i < chunk_size; ++i) {
        ASSERT_EQ(agg_states[i], agg_states[0]);
    }
    ASSERT_EQ(key.hash_map.size(), 0);
}

TEST_F(NullCompressibleInt32KeyTest, MixedNullAndValues) {
    using KeyImpl = AggHashMapWithOneNullableCompressibleInt32Key<RangeUInt8AggHashMap<PhmapSeed1>>;
    RuntimeProfile profile("NullCompressibleInt32KeyTest");
    AggStatistics statis(&profile);
    const int chunk_size = 6;
    KeyImpl key(chunk_size, &statis);
    key.set_min(0);

    auto col = ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_INT), true);
    col->append_datum(Datum(int32_t{1}));
    col->append_nulls(1);
    col->append_datum(Datum(int32_t{1}));
    col->append_datum(Datum(int32_t{2}));
    col->append_nulls(1);
    col->append_datum(Datum(int32_t{2}));
    Columns key_columns;
    key_columns.emplace_back(std::move(col));

    MemPool pool;
    Buffer<AggDataPtr> agg_states(chunk_size);
    auto allocate_func = [&pool](auto&&...) { return pool.allocate(16); };
    key.build_hash_map(chunk_size, key_columns, &pool, allocate_func, &agg_states);

    // Distinct non-null keys (1, 2) -> 2 cells; nulls share null_key_data.
    ASSERT_EQ(key.hash_map.size(), 2);
    ASSERT_EQ(agg_states[0], agg_states[2]); // both rows with value 1
    ASSERT_EQ(agg_states[3], agg_states[5]); // both rows with value 2
    ASSERT_EQ(agg_states[1], agg_states[4]); // both null rows
    ASSERT_NE(agg_states[0], agg_states[3]); // value 1 != value 2
    ASSERT_NE(agg_states[0], agg_states[1]); // value 1 != null
}

} // namespace starrocks
