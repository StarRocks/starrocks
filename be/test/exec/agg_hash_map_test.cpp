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
#include <limits>
#include <numeric>
#include <set>
#include <string>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_set.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/serialized_key_buffer.h"
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

template <PhmapSeed seed>
std::ostream& operator<<(std::ostream& os, const SliceWithHash32<seed>& key) {
    return os << key.slice();
}

template <typename T>
inline constexpr bool is_slice_with_hash32 =
        std::is_same_v<T, SliceWithHash32<PhmapSeed1>> || std::is_same_v<T, SliceWithHash32<PhmapSeed2>>;

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
            } else if constexpr (is_slice_with_hash32<typename decltype(hash_map_with_key->hash_map)::key_type>) {
                using KeyType = typename decltype(hash_map_with_key->hash_map)::key_type;
                std::vector<KeyType> keys;
                for (const auto& s : get_keys<Slice>()) {
                    keys.emplace_back(s);
                }
                exec(hash_map_with_key->hash_map, keys);
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

TEST_F(AggHashMapKeyNotFoundsTest, TestAllocateAndComputeNonFounds_FixedSize16SliceAggHashMap) {
    using TestAggHashMap = FixedSize16SliceAggHashMap<PhmapSeed1>;
    using TestAggHashMapKey = AggHashMapWithSerializedKeyFixedSize<TestAggHashMap>;
    TestAggHashMapKeyWithIntType<TestAggHashMapKey>(true);
}

// The slice agg maps cache the key's crc in the key, which must stay 16 bytes: the agg state prefix holds a copy of
// the key, so a bigger key would silently grow every group.
static_assert(sizeof(SliceAggHashMap<PhmapSeed1>::key_type) == sizeof(Slice));
static_assert(sizeof(SliceAggTwoLevelHashMap<PhmapSeed2>::key_type) == sizeof(Slice));
static_assert(alignof(SerializedKeyAggHashMap<PhmapSeed1>::KeyType) == alignof(Slice));

// The cached hash, the map's hash functor and the hash the probe paths prefetch with must all agree, or the probes
// degrade without any wrong result showing up.
TEST(SliceWithHash32Test, HashIsTheSliceCrcMixedByTheMap) {
    using Key1 = SliceWithHash32<PhmapSeed1>;
    using Key2 = SliceWithHash32<PhmapSeed2>;
    const std::string value = "a key that is longer than eight bytes";
    for (const Slice s : {Slice(value), Slice(value.data(), 3), Slice()}) {
        EXPECT_EQ(static_cast<uint32_t>(SliceHashWithSeed<PhmapSeed1>()(s)), Key1(s).hash);
        EXPECT_EQ(static_cast<uint32_t>(SliceHashWithSeed<PhmapSeed2>()(s)), Key2(s).hash);
        EXPECT_EQ((phmap_mix_with_seed<8, PhmapSeed1>()(Key1(s).hash)),
                  SliceAggHashMap<PhmapSeed1>().hash_function()(Key1(s)));
        EXPECT_EQ((phmap_mix_with_seed<8, PhmapSeed2>()(Key2(s).hash)),
                  SliceAggHashMap<PhmapSeed2>().hash_function()(Key2(s)));
    }
    // The mix must reach the high bits, which H1 needs once the table outgrows 2^25 slots.
    size_t high_bits = 0;
    for (int i = 0; i < 64; i++) {
        high_bits |= SliceAggHashMap<PhmapSeed1>().hash_function()(Key1(Slice(std::to_string(i))));
    }
    EXPECT_NE(0, high_bits >> 32);
}

// A key of 4 GiB or more stores its length in front of its bytes. Building one for real needs 4 GiB, so this spells
// out the escaped encoding of a small key and checks that it is the same key as the plain one everywhere.
TEST(SliceWithHash32Test, EscapedKeyEqualsPlainKey) {
    using Key = SliceWithHash32<PhmapSeed1>;
    const std::string value = "hello, escaped key";
    std::vector<uint8_t> storage(Key::kEscapeHeader + value.size() + SLICE_MEMEQUAL_OVERFLOW_PADDING, 0);
    const uint64_t real_size = value.size();
    memcpy(storage.data(), &real_size, sizeof(real_size));
    memcpy(storage.data() + Key::kEscapeHeader, value.data(), value.size());

    Key escaped;
    escaped.data = reinterpret_cast<const char*>(storage.data() + Key::kEscapeHeader);
    escaped.size = Key::kEscape;
    escaped.hash = Key::hash_of(Slice(value));
    const Key plain{Slice(value)};

    EXPECT_EQ(value.size(), escaped.real_size());
    EXPECT_EQ(Slice(value), static_cast<Slice>(escaped));
    EXPECT_TRUE(SliceWithHash32Equal<PhmapSeed1>()(plain, escaped));
    EXPECT_TRUE(SliceWithHash32Equal<PhmapSeed1>()(escaped, plain));

    MemPool pool;
    const Key persisted = persist_agg_slice_key(escaped, &pool);
    EXPECT_EQ(Key::kEscape, persisted.size);
    EXPECT_NE(escaped.data, persisted.data);
    EXPECT_EQ(Slice(value), persisted.slice());
    EXPECT_TRUE(SliceWithHash32Equal<PhmapSeed1>()(plain, persisted));

    SliceAggHashMap<PhmapSeed1> map;
    int64_t state = 7;
    map.emplace(persisted, reinterpret_cast<AggDataPtr>(&state));
    auto it = map.find(plain);
    ASSERT_TRUE(it != map.end());
    EXPECT_EQ(&state, reinterpret_cast<int64_t*>(it->second));

    // A plain key is never copied into the scratch pool.
    MemPool scratch;
    const Key built = make_agg_slice_key<Key>(Slice(value), &scratch);
    EXPECT_EQ(value.data(), built.data);
    EXPECT_EQ(value.size(), built.size);
    EXPECT_EQ(plain.hash, built.hash);
    EXPECT_EQ(0, scratch.total_reserved_bytes());
}

// Grows each slice map through many rehashes and probes it again: a wrong cached hash does not crash, it shows up as
// the wrong number of groups or as a key that maps to a different state.
template <typename MapWithKey>
void check_slice_map_groups_survive_rehash(void (*make_chunk)(int, int, Columns*)) {
    constexpr int kGroups = 200000;
    constexpr int kChunkSize = 4096;
    RuntimeProfile profile("check_slice_map_groups_survive_rehash");
    AggStatistics statistics(&profile);
    MapWithKey map(kChunkSize, &statistics);
    MemPool pool;
    struct Alloc {
        MemPool* pool;
        AggDataPtr operator()(const typename MapWithKey::KeyType&) { return pool->allocate(8); }
        AggDataPtr operator()(std::nullptr_t) { return pool->allocate(8); }
    };

    std::vector<AggDataPtr> first_states(kGroups, nullptr);
    Buffer<AggDataPtr> agg_states(kChunkSize);
    for (int pass = 0; pass < 2; pass++) {
        for (int begin = 0; begin < kGroups; begin += kChunkSize) {
            const int n = std::min(kChunkSize, kGroups - begin);
            Columns columns;
            make_chunk(begin, n, &columns);
            map.build_hash_map(n, columns, &pool, Alloc{&pool}, &agg_states);
            for (int i = 0; i < n; i++) {
                ASSERT_NE(nullptr, agg_states[i]);
                if (pass == 0) {
                    first_states[begin + i] = agg_states[i];
                } else {
                    ASSERT_EQ(first_states[begin + i], agg_states[i]) << "group " << begin + i;
                }
            }
        }
        ASSERT_EQ(static_cast<size_t>(kGroups), map.hash_map.size());
    }
    std::sort(first_states.begin(), first_states.end());
    EXPECT_TRUE(std::adjacent_find(first_states.begin(), first_states.end()) == first_states.end());
}

static void make_string_int_chunk(int begin, int n, Columns* columns) {
    auto strings = BinaryColumn::create();
    auto ints = Int32Column::create();
    for (int i = begin; i < begin + n; i++) {
        strings->append("group-key-" + std::to_string(i % 1000));
        ints->append(i / 1000);
    }
    columns->emplace_back(std::move(strings));
    columns->emplace_back(std::move(ints));
}

static void make_string_chunk(int begin, int n, Columns* columns) {
    auto strings = BinaryColumn::create();
    for (int i = begin; i < begin + n; i++) {
        strings->append("one-string-key-" + std::to_string(i));
    }
    columns->emplace_back(std::move(strings));
}

TEST(SliceWithHash32Test, SerializedKeyGroupsSurviveRehash) {
    check_slice_map_groups_survive_rehash<SerializedKeyAggHashMap<PhmapSeed1>>(make_string_int_chunk);
    check_slice_map_groups_survive_rehash<SerializedKeyAggHashMap<PhmapSeed2>>(make_string_int_chunk);
    check_slice_map_groups_survive_rehash<SerializedKeyTwoLevelAggHashMap<PhmapSeed1>>(make_string_int_chunk);
}

TEST(SliceWithHash32Test, OneStringKeyGroupsSurviveRehash) {
    check_slice_map_groups_survive_rehash<OneStringAggHashMap<PhmapSeed1>>(make_string_chunk);
    check_slice_map_groups_survive_rehash<OneStringAggHashMap<PhmapSeed2>>(make_string_chunk);
    check_slice_map_groups_survive_rehash<OneStringTwoLevelAggHashMap<PhmapSeed2>>(make_string_chunk);
}

// Writes the key into the state prefix, as AllocateState does, so a test can read it back the way
// convert_hash_map_to_chunk does.
template <typename MapWithKey>
struct KeyPrefixAlloc {
    MemPool* pool;
    AggDataPtr operator()(const typename MapWithKey::KeyType& key) {
        AggDataPtr state = pool->allocate(sizeof(key) + 8);
        memcpy(state, &key, sizeof(key));
        return state;
    }
    AggDataPtr operator()(std::nullptr_t) { return pool->allocate(sizeof(typename MapWithKey::KeyType) + 8); }
};

static std::string string_key_of(int id) {
    return "group-key-" + std::to_string(id % 1000);
}

// One (string, int) row per id; ids are distinct keys.
static void make_string_int_rows(const std::vector<int>& ids, Columns* columns) {
    auto strings = BinaryColumn::create();
    auto ints = Int32Column::create();
    for (int id : ids) {
        strings->append(string_key_of(id));
        ints->append(id / 1000);
    }
    columns->emplace_back(std::move(strings));
    columns->emplace_back(std::move(ints));
}

static void make_string_rows(const std::vector<int>& ids, Columns* columns) {
    auto strings = BinaryColumn::create();
    for (int id : ids) {
        strings->append("one-string-key-" + std::to_string(id));
    }
    columns->emplace_back(std::move(strings));
}

static std::vector<int> id_range(int begin, int end) {
    std::vector<int> ids(end - begin);
    std::iota(ids.begin(), ids.end(), begin);
    return ids;
}

// The per-row path (a chunk whose keys exceed the batch limit) and the per-column path serialize and hash the key
// separately; the same key has to land in the same group from both.
TEST(SliceWithHash32Test, PerRowAndPerColumnPathsAgreeOnTheKey) {
    using Map = SerializedKeyAggHashMap<PhmapSeed1>;
    constexpr int kKeys = 1000;
    RuntimeProfile profile("PerRowAndPerColumnPathsAgreeOnTheKey");
    AggStatistics statistics(&profile);
    Map map(4096, &statistics);
    MemPool pool;
    const std::string large_value(TypeDescriptor::LARGE_VARCHAR_LENGTH_THRESHOLD, 'x');
    // Rows are packed, so reaching the per-row path for real takes a >2 GiB chunk. Lower the limit
    // instead: the large row alone exceeds it, a chunk of short keys stays well below.
    map.key_buffer.set_max_batch_bytes_for_test(large_value.size());

    auto make_chunk = [&](bool with_large) {
        auto column = BinaryColumn::create();
        for (int i = 0; i < kKeys; i++) {
            column->append("short-key-" + std::to_string(i));
        }
        if (with_large) {
            column->append(Slice(large_value));
        }
        return Columns{std::move(column)};
    };

    Buffer<AggDataPtr> by_rows(kKeys + 1);
    map.build_hash_map(kKeys + 1, make_chunk(true), &pool, KeyPrefixAlloc<Map>{&pool}, &by_rows);
    ASSERT_FALSE(map.key_buffer.batched()) << "the first chunk must take the per-row path";
    ASSERT_EQ(static_cast<size_t>(kKeys + 1), map.hash_map.size());

    Buffer<AggDataPtr> by_cols(kKeys);
    map.build_hash_map(kKeys, make_chunk(false), &pool, KeyPrefixAlloc<Map>{&pool}, &by_cols);
    ASSERT_TRUE(map.key_buffer.batched()) << "the second chunk must take the per-column path";
    ASSERT_EQ(static_cast<size_t>(kKeys + 1), map.hash_map.size());
    for (int i = 0; i < kKeys; i++) {
        ASSERT_EQ(by_rows[i], by_cols[i]) << "key " << i;
    }

    // And back on the per-row path, the large key finds its own group again.
    Buffer<AggDataPtr> again(kKeys + 1);
    map.build_hash_map(kKeys + 1, make_chunk(true), &pool, KeyPrefixAlloc<Map>{&pool}, &again);
    ASSERT_EQ(static_cast<size_t>(kKeys + 1), map.hash_map.size());
    EXPECT_EQ(by_rows[kKeys], again[kKeys]);
}

// The selection and limit builds probe with find(key, hash). Run them on a grown table, both with prefetch forced
// on (the precomputed hash is used) and forced off (the functor recomputes it from the cached crc).
template <typename MapWithKey>
void check_selection_and_limit_probes(void (*make_rows)(const std::vector<int>&, Columns*)) {
    constexpr int kGroups = 60000;
    constexpr int kChunkSize = 4096;
    const double saved_ratio = config::agg_prefetch_l2_ratio;
    DeferOp restore_ratio([&]() { config::agg_prefetch_l2_ratio = saved_ratio; });
    for (const double ratio : {0.0, 1e9}) {
        SCOPED_TRACE(ratio == 0.0 ? "prefetch" : "no prefetch");
        config::agg_prefetch_l2_ratio = ratio;
        RuntimeProfile profile("check_selection_and_limit_probes");
        AggStatistics statistics(&profile);
        MapWithKey map(kChunkSize, &statistics);
        MemPool pool;
        Buffer<AggDataPtr> agg_states(kChunkSize);
        std::vector<AggDataPtr> states(kGroups, nullptr);
        for (int begin = 0; begin < kGroups; begin += kChunkSize) {
            const int end = std::min(kGroups, begin + kChunkSize);
            Columns columns;
            make_rows(id_range(begin, end), &columns);
            map.build_hash_map(end - begin, columns, &pool, KeyPrefixAlloc<MapWithKey>{&pool}, &agg_states);
            std::copy(agg_states.begin(), agg_states.begin() + (end - begin), states.begin() + begin);
        }
        ASSERT_EQ(static_cast<size_t>(kGroups), map.hash_map.size());
        ASSERT_EQ(ratio == 0.0, agg_should_prefetch_table(map.hash_map));

        // Even rows hit existing groups, odd rows are new keys.
        std::vector<int> ids(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            ids[i] = i % 2 == 0 ? i * 7 : kGroups + i;
        }
        Columns columns;
        make_rows(ids, &columns);
        Filter not_founds;
        map.build_hash_map_with_selection(kChunkSize, columns, &pool, KeyPrefixAlloc<MapWithKey>{&pool}, &agg_states,
                                          &not_founds);
        ASSERT_EQ(static_cast<size_t>(kGroups), map.hash_map.size());
        for (int i = 0; i < kChunkSize; i++) {
            ASSERT_EQ(i % 2, not_founds[i]) << "row " << i;
            if (i % 2 == 0) {
                ASSERT_EQ(states[ids[i]], agg_states[i]) << "row " << i;
            }
        }

        // With room for 100 more groups, exactly the first 100 new keys get in; the existing keys still resolve.
        constexpr int kRoom = 100;
        map.build_hash_map_with_limit(kChunkSize, columns, &pool, KeyPrefixAlloc<MapWithKey>{&pool}, &agg_states,
                                      &not_founds, kGroups + kRoom);
        ASSERT_EQ(static_cast<size_t>(kGroups + kRoom), map.hash_map.size());
        int inserted = 0;
        for (int i = 0; i < kChunkSize; i++) {
            if (i % 2 == 0) {
                ASSERT_EQ(0, not_founds[i]) << "row " << i;
                ASSERT_EQ(states[ids[i]], agg_states[i]) << "row " << i;
            } else if (inserted < kRoom) {
                ASSERT_EQ(0, not_founds[i]) << "row " << i;
                inserted++;
            } else {
                ASSERT_EQ(1, not_founds[i]) << "row " << i;
            }
        }
    }
}

TEST(SliceWithHash32Test, SelectionAndLimitProbesFindTheCachedHash) {
    check_selection_and_limit_probes<SerializedKeyAggHashMap<PhmapSeed1>>(make_string_int_rows);
    check_selection_and_limit_probes<SerializedKeyTwoLevelAggHashMap<PhmapSeed2>>(make_string_int_rows);
    check_selection_and_limit_probes<OneStringAggHashMap<PhmapSeed1>>(make_string_rows);
    check_selection_and_limit_probes<OneStringTwoLevelAggHashMap<PhmapSeed2>>(make_string_rows);
}

// A nullable string column with NULLs goes through its own per-row path: NULLs share one out-of-table state and the
// rest must still group by value.
TEST(SliceWithHash32Test, NullableStringKeyGroupsWithNulls) {
    using Map = NullOneStringAggHashMap<PhmapSeed1>;
    constexpr int kRows = 4096;
    // Coprime with the NULL period 5, so every value also shows up in non-NULL rows.
    constexpr int kDistinct = 301;
    RuntimeProfile profile("NullableStringKeyGroupsWithNulls");
    AggStatistics statistics(&profile);
    Map map(kRows, &statistics);
    MemPool pool;

    auto make_chunk = [&]() {
        auto data = BinaryColumn::create();
        auto nulls = NullColumn::create();
        for (int i = 0; i < kRows; i++) {
            data->append("nullable-key-" + std::to_string(i % kDistinct));
            nulls->append(i % 5 == 0);
        }
        return Columns{NullableColumn::create(std::move(data), std::move(nulls))};
    };

    Buffer<AggDataPtr> first(kRows);
    Buffer<AggDataPtr> second(kRows);
    map.build_hash_map(kRows, make_chunk(), &pool, KeyPrefixAlloc<Map>{&pool}, &first);
    map.build_hash_map(kRows, make_chunk(), &pool, KeyPrefixAlloc<Map>{&pool}, &second);
    ASSERT_NE(nullptr, map.get_null_key_data());
    ASSERT_EQ(static_cast<size_t>(kDistinct), map.hash_map.size());
    std::vector<AggDataPtr> by_value(kDistinct, nullptr);
    for (int i = 0; i < kRows; i++) {
        ASSERT_EQ(first[i], second[i]) << "row " << i;
        if (i % 5 == 0) {
            ASSERT_EQ(map.get_null_key_data(), first[i]) << "row " << i;
        } else if (by_value[i % kDistinct] == nullptr) {
            by_value[i % kDistinct] = first[i];
        } else {
            ASSERT_EQ(by_value[i % kDistinct], first[i]) << "row " << i;
        }
    }
}

// convert_hash_map_to_chunk reads each key back out of the agg state prefix and hands it to insert_keys_to_columns
// as a Slice: the round trip must give back every key exactly once.
TEST(SliceWithHash32Test, KeysRoundTripThroughTheStatePrefix) {
    constexpr int kGroups = 10000;
    RuntimeProfile profile("KeysRoundTripThroughTheStatePrefix");
    AggStatistics statistics(&profile);
    MemPool pool;
    {
        using Map = SerializedKeyAggHashMap<PhmapSeed1>;
        Map map(kGroups, &statistics);
        Columns columns;
        make_string_int_rows(id_range(0, kGroups), &columns);
        Buffer<AggDataPtr> agg_states(kGroups);
        map.build_hash_map(kGroups, columns, &pool, KeyPrefixAlloc<Map>{&pool}, &agg_states);

        map.results.clear();
        for (const auto& [key, state] : map.hash_map) {
            map.results.push_back(*reinterpret_cast<const Map::KeyType*>(state));
        }
        auto strings = BinaryColumn::create();
        auto ints = Int32Column::create();
        MutableColumns out;
        out.emplace_back(std::move(strings));
        out.emplace_back(std::move(ints));
        map.insert_keys_to_columns(map.results, out, kGroups);
        ASSERT_EQ(static_cast<size_t>(kGroups), out[0]->size());
        std::set<std::pair<std::string, int32_t>> seen;
        const auto* out_strings = down_cast<const BinaryColumn*>(out[0].get());
        const auto* out_ints = down_cast<const Int32Column*>(out[1].get());
        for (int i = 0; i < kGroups; i++) {
            seen.emplace(out_strings->get_slice(i).to_string(), out_ints->immutable_data()[i]);
        }
        ASSERT_EQ(static_cast<size_t>(kGroups), seen.size());
        for (int id = 0; id < kGroups; id++) {
            ASSERT_TRUE(seen.count({string_key_of(id), id / 1000})) << "missing key " << id;
        }
    }
    {
        using Map = OneStringAggHashMap<PhmapSeed2>;
        Map map(kGroups, &statistics);
        Columns columns;
        make_string_rows(id_range(0, kGroups), &columns);
        Buffer<AggDataPtr> agg_states(kGroups);
        map.build_hash_map(kGroups, columns, &pool, KeyPrefixAlloc<Map>{&pool}, &agg_states);

        map.results.clear();
        for (const auto& [key, state] : map.hash_map) {
            map.results.push_back(*reinterpret_cast<const Map::KeyType*>(state));
        }
        MutableColumns out;
        out.emplace_back(BinaryColumn::create());
        map.insert_keys_to_columns(map.results, out, kGroups);
        std::set<std::string> seen;
        const auto* out_strings = down_cast<const BinaryColumn*>(out[0].get());
        for (int i = 0; i < kGroups; i++) {
            seen.insert(out_strings->get_slice(i).to_string());
        }
        ASSERT_EQ(static_cast<size_t>(kGroups), seen.size());
        for (int id = 0; id < kGroups; id++) {
            ASSERT_TRUE(seen.count("one-string-key-" + std::to_string(id))) << "missing key " << id;
        }
    }
}

// The two-level conversion re-inserts every key by its cached hash; the converted map must keep grouping the same
// keys into the same states when the build continues on it.
TEST(SliceWithHash32Test, BuildContinuesAfterTwoLevelConversion) {
    using OneLevel = SerializedKeyAggHashMap<PhmapSeed1>;
    using TwoLevel = SerializedKeyTwoLevelAggHashMap<PhmapSeed1>;
    constexpr int kGroups = 20000;
    constexpr int kChunkSize = 4096;
    RuntimeState dummy;
    dummy.set_chunk_size(kChunkSize);
    RuntimeProfile profile("BuildContinuesAfterTwoLevelConversion");
    AggStatistics statistics(&profile);
    AggHashMapVariant variant;
    variant.init(&dummy, AggHashMapVariant::Type::phase1_slice, &statistics);
    MemPool pool;

    std::vector<AggDataPtr> states(2 * kGroups, nullptr);
    auto build = [&](int begin, int end, bool check) {
        variant.visit([&](auto& hash_map_with_key) {
            using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
            if constexpr (std::is_same_v<MapType, OneLevel> || std::is_same_v<MapType, TwoLevel>) {
                Buffer<AggDataPtr> agg_states(kChunkSize);
                for (int b = begin; b < end; b += kChunkSize) {
                    const int e = std::min(end, b + kChunkSize);
                    Columns columns;
                    make_string_int_rows(id_range(b, e), &columns);
                    hash_map_with_key->build_hash_map(e - b, columns, &pool, KeyPrefixAlloc<MapType>{&pool},
                                                      &agg_states);
                    for (int i = 0; i < e - b; i++) {
                        if (check) {
                            ASSERT_EQ(states[b + i], agg_states[i]) << "key " << b + i;
                        } else {
                            states[b + i] = agg_states[i];
                        }
                    }
                }
            } else {
                FAIL() << "unexpected active alternative";
            }
        });
    };

    build(0, kGroups, false);
    variant.convert_to_two_level(&dummy);
    ASSERT_EQ(static_cast<size_t>(kGroups), variant.size());
    build(0, kGroups, true);
    build(kGroups, 2 * kGroups, false);
    ASSERT_EQ(static_cast<size_t>(2 * kGroups), variant.size());
    build(0, 2 * kGroups, true);
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        EXPECT_TRUE((std::is_same_v<MapType, TwoLevel>));
    });
}

} // namespace starrocks
