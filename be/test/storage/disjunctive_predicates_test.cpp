// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/disjunctive_predicates.h"

#include <gtest/gtest.h>

#include <memory>

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "runtime/primitive_type.h"
#include "simd/simd.h"
#include "storage/vectorized_column_predicate.h"
#include "util/value_generator.h"

namespace starrocks::vectorized {
struct SegDataGenerator {
    inline static int sequence = 0;
    static int next_value() { return sequence++; }
};

template <int mod>
struct SegDataGeneratorWithRange {
    inline static int sequence = 0;
    static int next_value() { return sequence++ % mod; }
};

TEST(DisjunctivePredicatesTest, TwoPredicateTest) {
    // schema int, int
    constexpr const int chunk_size = 4096;
    constexpr PrimitiveType TYPE0 = TYPE_INT;
    constexpr PrimitiveType TYPE1 = TYPE_INT;

    auto column0 = RunTimeColumnType<TYPE0>::create(chunk_size);
    auto column1 = RunTimeColumnType<TYPE1>::create(chunk_size);
    ContainerIniter<SegDataGenerator, RunTimeColumnType<TYPE0>::Container, chunk_size>::init(column0->get_data());
    ContainerIniter<SegDataGeneratorWithRange<4>, RunTimeColumnType<TYPE1>::Container, chunk_size>::init(
            column1->get_data());
    Columns columns = {column0, column1};
    Chunk::SlotHashMap hash_map;
    hash_map[0] = 0;
    hash_map[1] = 1;
    auto chunk = std::make_shared<Chunk>(columns, hash_map);
    chunk->_cid_to_index[0] = 0;
    chunk->_cid_to_index[1] = 1;

    ObjectPool pool;
    ConjunctivePredicates conjuncts0;
    // > 1
    conjuncts0.vec_preds().push_back(pool.add(new_column_ge_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, "2000")));
    ConjunctivePredicates conjuncts1;
    conjuncts1.vec_preds().push_back(pool.add(new_column_ge_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 1, "2")));
    std::vector<uint8_t> dict_mapping;
    dict_mapping.resize(4);
    dict_mapping[2] = 1;
    dict_mapping[3] = 1;
    auto dict =
            pool.add(new_column_dict_conjuct_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 1, std::move(dict_mapping)));
    conjuncts1.non_vec_preds().push_back(dict);

    DisjunctivePredicates predicates;
    predicates.predicate_list().push_back(conjuncts0);
    predicates.predicate_list().push_back(conjuncts1);

    std::vector<uint8_t> selection;
    selection.resize(chunk_size);
    predicates.evaluate(chunk.get(), selection.data());

    size_t sz = SIMD::count_nonzero(selection);
    ASSERT_EQ(sz, 3096);
}

} // namespace starrocks::vectorized