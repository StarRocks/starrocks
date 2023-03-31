// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/aggregate/agg_hash_map.h"

#include <gtest/gtest.h>

#include <any>
#include <variant>

#include "exec/vectorized/aggregate/agg_hash_set.h"

namespace starrocks::vectorized {

using StringAggHashMap = phmap::flat_hash_map<std::string, AggDataPtr>;

struct HashMapVariants {
    enum class Type { empty = 0, int32, string, int32_two_level };
    Type type;
    std::unique_ptr<Int32AggHashMap<PhmapSeed1>> int32;
    std::unique_ptr<Int32AggTwoLevelHashMap<PhmapSeed1>> int32_two_level;
    std::unique_ptr<StringAggHashMap> string;

    void init(Type type_) {
        type = type_;
        switch (type_) {
        case Type::empty: {
            string = std::make_unique<StringAggHashMap>();
            break;
        }
        case Type::int32: {
            int32 = std::make_unique<Int32AggHashMap<PhmapSeed1>>();
            break;
        }
        case Type::int32_two_level: {
            int32_two_level = std::make_unique<Int32AggTwoLevelHashMap<PhmapSeed1>>();
            break;
        }
        case Type::string: {
            string = std::make_unique<StringAggHashMap>();
            break;
        }
        }
    }
};

#define APPLY_FOR_VARIANTS_SINGLE_LEVEL(M) \
    M(int32)                               \
    M(string)                              \
    M(int32_two_level)

template <typename HashMap>
HashMap& get_hash_map(HashMapVariants& variants);

template <>
Int32AggHashMap<PhmapSeed1>& get_hash_map<Int32AggHashMap<PhmapSeed1>>(HashMapVariants& variants) {
    return *variants.int32;
}

template <>
StringAggHashMap& get_hash_map<StringAggHashMap>(HashMapVariants& variants) {
    return *variants.string;
}

template <>
Int32AggTwoLevelHashMap<PhmapSeed1>& get_hash_map<Int32AggTwoLevelHashMap<PhmapSeed1>>(HashMapVariants& variants) {
    return *variants.int32_two_level;
}

template <typename T>
T get_keys();

template <>
std::vector<int32_t> get_keys<std::vector<int32_t>>() {
    std::vector<int32_t> keys(10);
    for (int i = 0; i < 10; i++) {
        keys[i] = i;
    }
    return keys;
};

template <>
std::vector<std::string> get_keys<std::vector<std::string>>() {
    std::vector<std::string> keys(10);
    for (int i = 0; i < 10; i++) {
        keys[i] = std::to_string(i);
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
        AggDataPtr agg_data = (AggDataPtr)(&sums[i]);
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
    std::any it_any;

    HashMapVariants variants;

    variants.init(HashMapVariants::Type::string);

#define M(NAME)                                                                                           \
    else if (variants.type == HashMapVariants::Type::NAME)                                                \
            exec<decltype(variants.NAME)::element_type, decltype(variants.NAME)::element_type::key_type>( \
                    get_hash_map<decltype(variants.NAME)::element_type>(variants),                        \
                    get_keys<std::vector<decltype(variants.NAME)::element_type::key_type>>());
    if (false) {
    }
    APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
    else {
        std::cout << "shouldn't go here!"
                  << "\n";
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

} // namespace starrocks::vectorized
