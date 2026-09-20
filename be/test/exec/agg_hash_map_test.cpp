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

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/partition/partition_hash_map.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"
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

// convert_to_two_level() rebuilds the wrapper and only moves the table entries across, so
// everything living outside the table -- the NULL group and the fields Aggregator sets once
// after construction -- has to be carried by hand.

// agg_key_state_is_convertible<> is the compile-time guard sitting on each declared (src, dst)
// pair. Every pair convert_to_two_level() actually declares has to pass it...
static_assert(
        agg_key_state_is_convertible<SerializedKeyAggHashMap<PhmapSeed1>, SerializedKeyTwoLevelAggHashMap<PhmapSeed1>>);
static_assert(
        agg_key_state_is_convertible<SerializedKeyAggHashMap<PhmapSeed2>, SerializedKeyTwoLevelAggHashMap<PhmapSeed2>>);
static_assert(agg_key_state_is_convertible<OneStringAggHashMap<PhmapSeed1>, OneStringTwoLevelAggHashMap<PhmapSeed1>>);
static_assert(agg_key_state_is_convertible<OneStringAggHashMap<PhmapSeed2>, OneStringTwoLevelAggHashMap<PhmapSeed2>>);
static_assert(
        agg_key_state_is_convertible<NullOneStringAggHashMap<PhmapSeed1>, NullOneStringTwoLevelAggHashMap<PhmapSeed1>>);
static_assert(
        agg_key_state_is_convertible<NullOneStringAggHashMap<PhmapSeed2>, NullOneStringTwoLevelAggHashMap<PhmapSeed2>>);
static_assert(
        agg_key_state_is_convertible<SerializedKeyAggHashSet<PhmapSeed1>, SerializedTwoLevelKeyAggHashSet<PhmapSeed1>>);
static_assert(
        agg_key_state_is_convertible<SerializedKeyAggHashSet<PhmapSeed2>, SerializedTwoLevelKeyAggHashSet<PhmapSeed2>>);
static_assert(agg_key_state_is_convertible<OneStringAggHashSet<PhmapSeed1>, OneStringTwoLevelAggHashSet<PhmapSeed1>>);
static_assert(agg_key_state_is_convertible<OneStringAggHashSet<PhmapSeed2>, OneStringTwoLevelAggHashSet<PhmapSeed2>>);
static_assert(
        agg_key_state_is_convertible<NullOneStringAggHashSet<PhmapSeed1>, NullOneStringTwoLevelAggHashSet<PhmapSeed1>>);
static_assert(
        agg_key_state_is_convertible<NullOneStringAggHashSet<PhmapSeed2>, NullOneStringTwoLevelAggHashSet<PhmapSeed2>>);

// ...and every way of silently dropping out-of-table state has to be rejected. Without these the
// guard could be vacuously true and nobody would notice.
static_assert(!agg_key_state_is_convertible<NullOneStringAggHashMap<PhmapSeed1>,
                                            OneStringTwoLevelAggHashMap<PhmapSeed1>>); // NULL group
static_assert(!agg_key_state_is_convertible<NullOneStringAggHashSet<PhmapSeed1>,
                                            OneStringTwoLevelAggHashSet<PhmapSeed1>>); // NULL group
static_assert(!agg_key_state_is_convertible<SerializedKeyFixedSize16AggHashMap<PhmapSeed1>,
                                            SerializedKeyTwoLevelAggHashMap<PhmapSeed1>>); // has_null_column
static_assert(!agg_key_state_is_convertible<SerializedKeyAggHashSetFixedSize16<PhmapSeed1>,
                                            SerializedTwoLevelKeyAggHashSet<PhmapSeed1>>); // fixed_byte_size
static_assert(!agg_key_state_is_convertible<CompressedFixedSize8AggHashMap<PhmapSeed1>,
                                            Int32TwoLevelAggHashMapWithOneNumberKey<PhmapSeed1>>); // compression ctx

// Drives one declared (src, dst) pair end to end: fill the single-level container, convert, then
// require that the active alternative really is the two-level one and that nothing was lost.
template <typename SrcWithKey, typename DstWithKey>
void check_map_converts_to_two_level(const char* what, AggHashMapVariant::Type src_type) {
    SCOPED_TRACE(what);
    constexpr int kNumKeys = 1000;
    std::vector<std::string> key_storage(kNumKeys);
    std::vector<int64_t> values(kNumKeys);
    for (int i = 0; i < kNumKeys; i++) {
        key_storage[i] = "key-" + std::to_string(i);
        values[i] = 1000 + i;
    }
    int64_t null_key_value = 4242;

    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics statis(&profile);

    AggHashMapVariant variant;
    variant.init(&dummy, src_type, &statis);
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (std::is_same_v<MapType, SrcWithKey>) {
            for (int i = 0; i < kNumKeys; i++) {
                hash_map_with_key->hash_map.emplace(typename MapType::KeyType(Slice(key_storage[i])),
                                                    (AggDataPtr)(&values[i]));
            }
            if constexpr (MapType::has_single_null_key) {
                hash_map_with_key->set_null_key_data((AggDataPtr)(&null_key_value));
            }
        } else {
            FAIL() << "unexpected active alternative before the conversion";
        }
    });

    variant.convert_to_two_level(&dummy);

    constexpr size_t kNullGroups = SrcWithKey::has_single_null_key ? 1 : 0;
    ASSERT_EQ(static_cast<size_t>(kNumKeys) + kNullGroups, variant.size());
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (std::is_same_v<MapType, DstWithKey>) {
            ASSERT_EQ(static_cast<size_t>(kNumKeys), hash_map_with_key->hash_map.size());
            for (int i = 0; i < kNumKeys; i++) {
                auto it = hash_map_with_key->hash_map.find(typename MapType::KeyType(Slice(key_storage[i])));
                ASSERT_TRUE(it != hash_map_with_key->hash_map.end()) << "missing key " << key_storage[i];
                EXPECT_EQ(values[i], *(int64_t*)it->second);
            }
            if constexpr (MapType::has_single_null_key) {
                ASSERT_NE(nullptr, hash_map_with_key->get_null_key_data());
                EXPECT_EQ(null_key_value, *(int64_t*)hash_map_with_key->get_null_key_data());
            }
        } else {
            FAIL() << "convert_to_two_level() did not switch to the two-level map";
        }
    });
}

template <typename SrcWithKey, typename DstWithKey>
void check_set_converts_to_two_level(const char* what, AggHashSetVariant::Type src_type) {
    SCOPED_TRACE(what);
    constexpr int kNumKeys = 1000;
    std::vector<std::string> key_storage(kNumKeys);
    for (int i = 0; i < kNumKeys; i++) {
        key_storage[i] = "key-" + std::to_string(i);
    }

    RuntimeState dummy;
    RuntimeProfile profile("dummy");
    AggStatistics statis(&profile);

    AggHashSetVariant variant;
    variant.init(&dummy, src_type, &statis);
    variant.visit([&](auto& hash_set_with_key) {
        using SetType = std::remove_reference_t<decltype(*hash_set_with_key)>;
        if constexpr (std::is_same_v<SetType, SrcWithKey>) {
            for (int i = 0; i < kNumKeys; i++) {
                hash_set_with_key->hash_set.emplace(typename SetType::KeyType(Slice(key_storage[i])));
            }
            if constexpr (SetType::has_single_null_key) {
                hash_set_with_key->has_null_key = true;
            }
        } else {
            FAIL() << "unexpected active alternative before the conversion";
        }
    });

    variant.convert_to_two_level(&dummy);

    constexpr size_t kNullGroups = SrcWithKey::has_single_null_key ? 1 : 0;
    ASSERT_EQ(static_cast<size_t>(kNumKeys) + kNullGroups, variant.size());
    variant.visit([&](auto& hash_set_with_key) {
        using SetType = std::remove_reference_t<decltype(*hash_set_with_key)>;
        if constexpr (std::is_same_v<SetType, DstWithKey>) {
            ASSERT_EQ(static_cast<size_t>(kNumKeys), hash_set_with_key->hash_set.size());
            for (int i = 0; i < kNumKeys; i++) {
                EXPECT_TRUE(hash_set_with_key->hash_set.contains(typename SetType::KeyType(Slice(key_storage[i]))));
            }
            if constexpr (SetType::has_single_null_key) {
                EXPECT_TRUE(hash_set_with_key->has_null_key);
            }
        } else {
            FAIL() << "convert_to_two_level() did not switch to the two-level set";
        }
    });
}

TEST(HashMapTest, ConvertToTwoLevelPreservesEveryDeclaredMapPair) {
    using Type = AggHashMapVariant::Type;
    check_map_converts_to_two_level<SerializedKeyAggHashMap<PhmapSeed1>, SerializedKeyTwoLevelAggHashMap<PhmapSeed1>>(
            "phase1_slice", Type::phase1_slice);
    check_map_converts_to_two_level<SerializedKeyAggHashMap<PhmapSeed2>, SerializedKeyTwoLevelAggHashMap<PhmapSeed2>>(
            "phase2_slice", Type::phase2_slice);
    check_map_converts_to_two_level<OneStringAggHashMap<PhmapSeed1>, OneStringTwoLevelAggHashMap<PhmapSeed1>>(
            "phase1_string", Type::phase1_string);
    check_map_converts_to_two_level<OneStringAggHashMap<PhmapSeed2>, OneStringTwoLevelAggHashMap<PhmapSeed2>>(
            "phase2_string", Type::phase2_string);
    check_map_converts_to_two_level<NullOneStringAggHashMap<PhmapSeed1>, NullOneStringTwoLevelAggHashMap<PhmapSeed1>>(
            "phase1_null_string", Type::phase1_null_string);
    check_map_converts_to_two_level<NullOneStringAggHashMap<PhmapSeed2>, NullOneStringTwoLevelAggHashMap<PhmapSeed2>>(
            "phase2_null_string", Type::phase2_null_string);
}

TEST(HashMapTest, ConvertToTwoLevelPreservesEveryDeclaredSetPair) {
    using Type = AggHashSetVariant::Type;
    check_set_converts_to_two_level<SerializedKeyAggHashSet<PhmapSeed1>, SerializedTwoLevelKeyAggHashSet<PhmapSeed1>>(
            "phase1_slice", Type::phase1_slice);
    check_set_converts_to_two_level<SerializedKeyAggHashSet<PhmapSeed2>, SerializedTwoLevelKeyAggHashSet<PhmapSeed2>>(
            "phase2_slice", Type::phase2_slice);
    check_set_converts_to_two_level<OneStringAggHashSet<PhmapSeed1>, OneStringTwoLevelAggHashSet<PhmapSeed1>>(
            "phase1_string", Type::phase1_string);
    check_set_converts_to_two_level<OneStringAggHashSet<PhmapSeed2>, OneStringTwoLevelAggHashSet<PhmapSeed2>>(
            "phase2_string", Type::phase2_string);
    check_set_converts_to_two_level<NullOneStringAggHashSet<PhmapSeed1>, NullOneStringTwoLevelAggHashSet<PhmapSeed1>>(
            "phase1_null_string", Type::phase1_null_string);
    check_set_converts_to_two_level<NullOneStringAggHashSet<PhmapSeed2>, NullOneStringTwoLevelAggHashSet<PhmapSeed2>>(
            "phase2_null_string", Type::phase2_null_string);
}

// Guards the latent trap: a combined fixed-size key whose has_null_column is dropped stops
// writing the null byte into the key and silently merges distinct groups.
TEST(HashMapTest, CopyAggKeyStateCarriesCombinedFixedSizeFields) {
    RuntimeProfile profile("dummy");
    AggStatistics statis(&profile);

    {
        using FixedKeyMap = SerializedKeyFixedSize16AggHashMap<PhmapSeed1>;
        FixedKeyMap src(64, &statis);
        src.has_null_column = true;
        src.fixed_byte_size = 9;

        FixedKeyMap dst(64, &statis);
        ASSERT_FALSE(dst.has_null_column);
        ASSERT_EQ(-1, dst.fixed_byte_size);

        copy_agg_key_state(src, dst);
        EXPECT_TRUE(dst.has_null_column);
        EXPECT_EQ(9, dst.fixed_byte_size);
    }

    {
        using FixedKeySet = SerializedKeyAggHashSetFixedSize16<PhmapSeed1>;
        FixedKeySet src(64, &statis);
        src.has_null_column = true;
        src.fixed_byte_size = 9;

        FixedKeySet dst(64, &statis);
        copy_agg_key_state(src, dst);
        EXPECT_TRUE(dst.has_null_column);
        EXPECT_EQ(9, dst.fixed_byte_size);
    }
}

TEST(HashMapTest, CopyAggKeyStateCarriesCompressedKeyContext) {
    RuntimeProfile profile("dummy");
    AggStatistics statis(&profile);

    using CompressedMap = CompressedFixedSize8AggHashMap<PhmapSeed1>;
    CompressedMap src(64, &statis);
    src.used_bits = {3, 5};
    src.offsets = {0, 3};
    src.bases = {std::any(int32_t(7)), std::any(int32_t(11))};

    CompressedMap dst(64, &statis);
    ASSERT_TRUE(dst.used_bits.empty());

    copy_agg_key_state(src, dst);
    EXPECT_EQ(std::vector<int>({3, 5}), dst.used_bits);
    EXPECT_EQ(std::vector<int>({0, 3}), dst.offsets);
    ASSERT_EQ(2u, dst.bases.size());
    EXPECT_EQ(7, std::any_cast<int32_t>(dst.bases[0]));
    EXPECT_EQ(11, std::any_cast<int32_t>(dst.bases[1]));
}

// The copy itself has to stay tolerant: the visitor it runs under is instantiated for every
// alternative sharing the key type, including pairs that never occur at runtime. Rejecting those
// is agg_key_state_is_convertible<>'s job, not this one's.
TEST(HashMapTest, CopyAggKeyStateSkipsStateTheDestinationCannotHold) {
    RuntimeProfile profile("dummy");
    AggStatistics statis(&profile);

    SerializedKeyFixedSize16AggHashMap<PhmapSeed1> fixed_src(64, &statis);
    fixed_src.has_null_column = true;
    fixed_src.fixed_byte_size = 9;

    SerializedKeyAggHashMap<PhmapSeed1> plain_dst(64, &statis);
    copy_agg_key_state(fixed_src, plain_dst);

    CompressedFixedSize8AggHashMap<PhmapSeed1> compressed_src(64, &statis);
    compressed_src.used_bits = {3, 5};
    copy_agg_key_state(compressed_src, plain_dst);

    // Nothing to assert on plain_dst -- it has no such fields. The point is that this compiles
    // and does not touch the destination.
    EXPECT_EQ(0u, plain_dst.hash_map.size());
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

TEST_F(AggHashMapKeyNotFoundsTest, SerializedKeyUsesPerRowPathWhenBatchEstimateExceedsInt32) {
    using TestAggHashMapKey = SerializedKeyAggHashMap<PhmapSeed1>;

    RuntimeProfile profile("SerializedKeyUsesPerRowPathWhenBatchEstimateExceedsInt32");
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

    EXPECT_EQ(0, key.max_one_row_size);
    EXPECT_EQ(1, key.hash_map.size());
    EXPECT_NE(nullptr, agg_states[0]);

    Filter not_founds;
    key.build_hash_map_with_selection(1, key_columns, &pool, TestAllocateState<TestAggHashMapKey>(&pool), &agg_states,
                                      &not_founds);
    ASSERT_EQ(1, not_founds.size());
    EXPECT_EQ(0, not_founds[0]);
}

TEST(PartitionHashMapSerializedKeyTest, SumsOrdinaryColumnSerializeSizes) {
    PartitionHashMapWithSerializedKey<SlicePartitionHashMap<PhmapSeed1>> partition_map(4);

    auto string_column = BinaryColumn::create();
    string_column->append("abc");
    auto int_column = Int32Column::create();
    int_column->append(7);
    Columns key_columns{string_column, int_column};

    auto size_or = partition_map.get_max_serialize_size(key_columns);
    ASSERT_TRUE(size_or.ok()) << size_or.status();
    EXPECT_EQ(string_column->max_one_element_serialize_size() + int_column->max_one_element_serialize_size(),
              size_or.value());
}

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_FixedSize16SliceAggHashMap) {
    using TestAggHashMap = FixedSize16SliceAggHashMap<PhmapSeed1>;
    using TestAggHashMapKey = AggHashMapWithSerializedKeyFixedSize<TestAggHashMap>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

} // namespace starrocks
