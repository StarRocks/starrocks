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

#include "exprs/runtime_filter.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>
#include <utility>

#include "column/column_helper.h"
#include "exprs/runtime_filter_bank.h"
#include "simd/simd.h"

namespace starrocks {

class RuntimeFilterTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

public:
};

TEST_F(RuntimeFilterTest, TestSimdBlockFilter) {
    SimdBlockFilter bf0;
    bf0.init(100);
    for (int i = 1; i <= 200; i += 17) {
        bf0.insert_hash(i);
    }
    for (int i = 1; i <= 200; i += 17) {
        EXPECT_FALSE(bf0.test_hash(i + 1));
        EXPECT_TRUE(bf0.test_hash(i));
    }
}

TEST_F(RuntimeFilterTest, TestSimdBlockFilterSerialize) {
    SimdBlockFilter bf0;
    bf0.init(100);
    for (int i = 1; i <= 200; i += 17) {
        bf0.insert_hash(i);
    }
    size_t ser_size = bf0.max_serialized_size();
    std::vector<uint8_t> buf(ser_size, 0);
    EXPECT_EQ(bf0.serialize(buf.data()), ser_size);

    SimdBlockFilter bf1;
    EXPECT_EQ(bf1.deserialize(buf.data()), ser_size);
    for (int i = 1; i <= 200; i += 17) {
        EXPECT_FALSE(bf1.test_hash(i + 1));
        EXPECT_TRUE(bf1.test_hash(i));
    }

    EXPECT_TRUE(bf0.check_equal(bf1));
}

TEST_F(RuntimeFilterTest, TestSimdBlockFilterMerge) {
    SimdBlockFilter bf0;
    bf0.init(100);
    for (int i = 1; i <= 200; i += 17) {
        bf0.insert_hash(i);
    }

    SimdBlockFilter bf1;
    bf1.init(100);
    for (int i = 2; i <= 200; i += 17) {
        bf1.insert_hash(i);
    }

    SimdBlockFilter bf2;
    bf2.init(100);
    bf2.merge(bf0);
    bf2.merge(bf1);
    for (int i = 1; i <= 200; i += 17) {
        EXPECT_TRUE(bf2.test_hash(i));
        EXPECT_TRUE(bf2.test_hash(i + 1));
        EXPECT_FALSE(bf2.test_hash(i + 2));
    }
}
static std::string alphabet0 =
        "abcdefgh"
        "igklmnop"
        "qrstuvwx"
        "yzABCDEF"
        "GHIGKLMN"
        "OPQRSTUV"
        "WXYZ=%01"
        "23456789";

static std::string alphabet1 = "~!@#$%^&*()_+{}|:\"<>?[]\\;',./";

static std::shared_ptr<BinaryColumn> gen_random_binary_column(const std::string& alphabet, size_t avg_length,
                                                              size_t num_rows) {
    auto col = BinaryColumn::create();
    col->reserve(num_rows);
    std::random_device rd;
    std::uniform_int_distribution<size_t> length_g(0, 2 * avg_length);
    std::uniform_int_distribution<size_t> g(0, alphabet.size());

    for (auto i = 0; i < num_rows; ++i) {
        size_t length = length_g(rd);
        std::string s;
        s.reserve(length);
        for (auto i = 0; i < length; ++i) {
            s.push_back(alphabet[g(rd)]);
        }
        col->append(Slice(s));
    }
    return col;
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilter) {
    RuntimeBloomFilter<TYPE_INT> bf;
    JoinRuntimeFilter* rf = &bf;
    bf.init(100);
    for (int i = 0; i <= 200; i += 17) {
        bf.insert(&i);
    }
    EXPECT_EQ(bf.min_value(), 0);
    EXPECT_EQ(bf.max_value(), 187);
    for (int i = 0; i <= 200; i += 17) {
        EXPECT_TRUE(bf._test_data(i));
        EXPECT_FALSE(bf._test_data(i + 1));
    }
    EXPECT_FALSE(rf->has_null());
    bf.insert(nullptr);
    EXPECT_TRUE(rf->has_null());
    EXPECT_EQ(bf.min_value(), 0);
    EXPECT_EQ(bf.max_value(), 187);

    // test evaluate.
    TypeDescriptor type_desc(TYPE_INT);
    ColumnPtr column = ColumnHelper::create_column(type_desc, false);
    auto* col = ColumnHelper::as_raw_column<RunTimeTypeTraits<TYPE_INT>::ColumnType>(column);
    for (int i = 0; i <= 200; i += 1) {
        col->append(i);
    }
    Chunk chunk;
    chunk.append_column(column, 0);
    JoinRuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    auto& selection = ctx.selection;
    selection.assign(column->size(), 1);
    rf->compute_hash({column.get()}, &ctx);
    rf->evaluate(column.get(), &ctx);
    chunk.filter(selection);
    // 0 17 34 ... 187
    EXPECT_EQ(chunk.num_rows(), 12);
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterSlice) {
    RuntimeBloomFilter<TYPE_VARCHAR> bf;
    // JoinRuntimeFilter* rf = &bf;
    std::vector<std::string> data = {"aa", "bb", "cc", "dd"};
    std::vector<Slice> values;
    for (const auto& s : data) {
        values.emplace_back(Slice(s));
    }
    bf.init(100);
    for (auto& s : values) {
        bf.insert(&s);
    }
    EXPECT_EQ(bf.min_value(), values[0]);
    EXPECT_EQ(bf.max_value(), values[values.size() - 1]);
    for (auto& s : values) {
        EXPECT_TRUE(bf._test_data(s));
    }
    std::vector<std::string> ex_data = {"ee", "ff", "gg"};
    for (const auto& s : ex_data) {
        EXPECT_FALSE(bf._test_data(Slice(s)));
    }
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterSerialize) {
    RuntimeBloomFilter<TYPE_INT> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    bf0.init(100);
    int rf_version = RF_VERSION_V2;
    for (int i = 0; i <= 200; i += 17) {
        bf0.insert(&i);
    }

    size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf0);
    std::vector<uint8_t> buffer(max_size, 0);
    size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf_version, rf0, buffer.data());
    buffer.resize(actual_size);

    JoinRuntimeFilter* rf1 = nullptr;
    ObjectPool pool;
    RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf1, buffer.data(), actual_size);
    EXPECT_TRUE(rf1->check_equal(*rf0));
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterSerialize2) {
    RuntimeBloomFilter<TYPE_INT> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    bf0.init(100);
    for (int i = 0; i <= 200; i += 17) {
        bf0.insert(&i);
    }
    EXPECT_EQ(bf0.min_value(), 0);
    EXPECT_EQ(bf0.max_value(), 187);

    RuntimeBloomFilter<TYPE_VARCHAR> bf1;
    JoinRuntimeFilter* rf1 = &bf1;
    std::vector<std::string> data = {"aa", "bb", "cc", "dd"};
    std::vector<Slice> values;
    for (const auto& s : data) {
        values.emplace_back(Slice(s));
    }
    bf1.init(200);
    for (auto& s : values) {
        bf1.insert(&s);
    }
    EXPECT_EQ(bf1.min_value(), values[0]);
    EXPECT_EQ(bf1.max_value(), values[values.size() - 1]);

    int rf_version = RF_VERSION_V2;

    ObjectPool pool;
    size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf0);
    std::vector<uint8_t> buffer0(max_size, 0);
    size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf_version, rf0, buffer0.data());
    buffer0.resize(actual_size);
    JoinRuntimeFilter* rf2 = nullptr;
    RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf2, buffer0.data(), actual_size);

    max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf1);
    buffer0.assign(max_size, 0);
    actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf_version, rf1, buffer0.data());
    buffer0.resize(actual_size);
    JoinRuntimeFilter* rf3 = nullptr;
    RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf3, buffer0.data(), actual_size);

    EXPECT_TRUE(rf2->check_equal(*rf0));
    EXPECT_TRUE(rf3->check_equal(*rf1));
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterMerge) {
    RuntimeBloomFilter<TYPE_INT> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    bf0.init(100);
    for (int i = 0; i <= 200; i += 17) {
        bf0.insert(&i);
    }
    EXPECT_EQ(bf0.min_value(), 0);
    EXPECT_EQ(bf0.max_value(), 187);

    RuntimeBloomFilter<TYPE_INT> bf1;
    JoinRuntimeFilter* rf1 = &bf1;
    bf1.init(100);
    for (int i = 1; i <= 200; i += 17) {
        bf1.insert(&i);
    }
    EXPECT_EQ(bf1.min_value(), 1);
    EXPECT_EQ(bf1.max_value(), 188);

    RuntimeBloomFilter<TYPE_INT> bf2;
    bf2.init(100);
    bf2.merge(rf0);
    bf2.merge(rf1);
    for (int i = 0; i <= 200; i += 17) {
        EXPECT_TRUE(bf2._test_data(i));
        EXPECT_TRUE(bf2._test_data(i + 1));
        EXPECT_FALSE(bf2._test_data(i + 2));
    }
    EXPECT_EQ(bf2.min_value(), 0);
    EXPECT_EQ(bf2.max_value(), 188);
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterMerge2) {
    RuntimeBloomFilter<TYPE_VARCHAR> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    std::vector<std::string> data = {"bb", "cc", "dd"};
    {
        std::vector<Slice> values;
        for (const auto& s : data) {
            values.emplace_back(Slice(s));
        }
        bf0.init(100);
        for (auto& s : values) {
            bf0.insert(&s);
        }
        // bb - dd
        EXPECT_EQ(bf0.min_value(), values[0]);
        EXPECT_EQ(bf0.max_value(), values[values.size() - 1]);
    }

    RuntimeBloomFilter<TYPE_VARCHAR> bf1;
    JoinRuntimeFilter* rf1 = &bf1;
    std::vector<std::string> data2 = {"aa", "bb", "cc", "dc"};

    {
        std::vector<Slice> values;
        for (const auto& s : data2) {
            values.emplace_back(Slice(s));
        }
        bf1.init(100);
        for (auto& s : values) {
            bf1.insert(&s);
        }
        // aa - dc
        EXPECT_EQ(bf1.min_value(), values[0]);
        EXPECT_EQ(bf1.max_value(), values[values.size() - 1]);
    }

    // range aa - dd
    rf0->merge(rf1);
    EXPECT_EQ(bf0.min_value(), Slice("aa", 2));
    EXPECT_EQ(bf0.max_value(), Slice("dd", 2));
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterMerge3) {
    RuntimeBloomFilter<TYPE_VARCHAR> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    ObjectPool pool;
    int rf_version = RF_VERSION_V2;
    {
        std::vector<std::string> data = {"bb", "cc", "dd"};
        std::vector<Slice> values;
        for (const auto& s : data) {
            values.emplace_back(Slice(s));
        }
        bf0.init(100);
        for (auto& s : values) {
            bf0.insert(&s);
        }

        size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf0);
        std::string buf(max_size, 0);
        size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf_version, rf0, (uint8_t*)buf.data());
        buf.resize(actual_size);

        RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf0, (const uint8_t*)buf.data(), actual_size);
    }

    auto* pbf0 = static_cast<RuntimeBloomFilter<TYPE_VARCHAR>*>(rf0);
    EXPECT_EQ(pbf0->min_value(), Slice("bb", 2));
    EXPECT_EQ(pbf0->max_value(), Slice("dd", 2));

    RuntimeBloomFilter<TYPE_VARCHAR> bf1;
    JoinRuntimeFilter* rf1 = &bf1;
    {
        std::vector<std::string> data = {"aa", "cc", "dc"};
        std::vector<Slice> values;
        for (const auto& s : data) {
            values.emplace_back(Slice(s));
        }
        bf1.init(100);
        for (auto& s : values) {
            bf1.insert(&s);
        }

        size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf1);
        std::string buf(max_size, 0);
        size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf_version, rf1, (uint8_t*)buf.data());
        buf.resize(actual_size);
        RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf1, (const uint8_t*)buf.data(), actual_size);
    }

    auto* pbf1 = static_cast<RuntimeBloomFilter<TYPE_VARCHAR>*>(rf1);
    EXPECT_EQ(pbf1->min_value(), Slice("aa", 2));
    EXPECT_EQ(pbf1->max_value(), Slice("dc", 2));

    // range aa - dd
    rf0->merge(rf1);
    // out of scope, we expect aa and dd would be still alive.
    EXPECT_EQ(pbf0->min_value(), Slice("aa", 2));
    EXPECT_EQ(pbf0->max_value(), Slice("dd", 2));
}

typedef std::function<void(BinaryColumn*, std::vector<uint32_t>&, std::vector<size_t>&)> PartitionByFunc;
typedef std::function<void(JoinRuntimeFilter*, JoinRuntimeFilter::RunningContext*)> GrfConfigFunc;

using TestHelper = std::function<void(size_t, size_t, PartitionByFunc, GrfConfigFunc)>;

template <bool compatibility>
void test_grf_helper_template(size_t num_rows, size_t num_partitions, const PartitionByFunc& part_func,
                              const GrfConfigFunc& grf_config_func) {
    std::vector<RuntimeBloomFilter<TYPE_VARCHAR>> bfs(num_partitions);
    std::vector<JoinRuntimeFilter*> rfs(num_partitions);
    for (auto p = 0; p < num_partitions; ++p) {
        rfs[p] = &bfs[p];
    }

    ObjectPool pool;
    auto column = gen_random_binary_column(alphabet0, 100, num_rows);
    std::vector<size_t> num_rows_per_partitions(num_partitions, 0);
    std::vector<uint32_t> hash_values;
    part_func(column.get(), hash_values, num_rows_per_partitions);
    for (auto p = 0; p < num_partitions; ++p) {
        bfs[p].init(num_rows_per_partitions[p]);
    }

    for (auto i = 0; i < num_rows; ++i) {
        auto slice = column->get_slice(i);
        bfs[hash_values[i]].insert(&slice);
    }

    int rf_version = RF_VERSION_V2;

    std::vector<std::string> serialized_rfs(num_partitions);
    for (auto p = 0; p < num_partitions; ++p) {
        size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rfs[p]);
        serialized_rfs[p].resize(max_size, 0);
        size_t actual_size =
                RuntimeFilterHelper::serialize_runtime_filter(rf_version, rfs[p], (uint8_t*)serialized_rfs[p].data());
        serialized_rfs[p].resize(actual_size);
    }

    RuntimeBloomFilter<TYPE_VARCHAR> grf;
    for (auto p = 0; p < num_partitions; ++p) {
        JoinRuntimeFilter* grf_component;
        RuntimeFilterHelper::deserialize_runtime_filter(&pool, &grf_component, (const uint8_t*)serialized_rfs[p].data(),
                                                        serialized_rfs[p].size());
        ASSERT_EQ(grf_component->size(), num_rows_per_partitions[p]);
        grf.concat(grf_component);
    }
    ASSERT_EQ(grf.size(), num_rows);
    JoinRuntimeFilter::RunningContext running_ctx;
    grf_config_func(&grf, &running_ctx);
    {
        running_ctx.selection.assign(num_rows, 1);
        running_ctx.use_merged_selection = false;
        running_ctx.compatibility = compatibility;
        grf.compute_hash({column.get()}, &running_ctx);
        grf.evaluate(column.get(), &running_ctx);
        auto true_count = SIMD::count_nonzero(running_ctx.selection.data(), num_rows);
        ASSERT_EQ(true_count, num_rows);
    }
    {
        size_t negative_num_rows = 100;
        auto negative_column = gen_random_binary_column(alphabet1, 100, negative_num_rows);
        running_ctx.selection.assign(negative_num_rows, 1);
        running_ctx.use_merged_selection = false;
        grf.compute_hash({negative_column.get()}, &running_ctx);
        grf.evaluate(negative_column.get(), &running_ctx);
        auto true_count = SIMD::count_nonzero(running_ctx.selection.data(), negative_num_rows);
        ASSERT_LE((double)true_count / negative_num_rows, 0.5);
    }
}

TestHelper test_grf_helper = test_grf_helper_template<true>;

void test_colocate_or_bucket_shuffle_grf_helper(size_t num_rows, size_t num_partitions, size_t num_buckets,
                                                std::vector<int> bucketseq_to_partition,
                                                TRuntimeFilterBuildJoinMode::type mode) {
    auto part_by_func = [num_rows, num_buckets, &bucketseq_to_partition](BinaryColumn* column,
                                                                         std::vector<uint32_t>& hash_values,
                                                                         std::vector<size_t>& num_rows_per_partitions) {
        hash_values.assign(num_rows, 0);
        column->crc32_hash(hash_values.data(), 0, num_rows);
        std::vector<size_t> num_rows_per_bucket(num_buckets, 0);
        for (auto i = 0; i < num_rows; ++i) {
            hash_values[i] %= num_buckets;
            ++num_rows_per_bucket[hash_values[i]];
            hash_values[i] = bucketseq_to_partition[hash_values[i]];
            ++num_rows_per_partitions[hash_values[i]];
        }
        static constexpr uint32_t BUCKET_ABSENT = 2147483647;
        for (auto b = 0; b < num_buckets; ++b) {
            if (num_rows_per_bucket[b] == 0) {
                bucketseq_to_partition[b] = BUCKET_ABSENT;
            }
        }
    };
    auto grf_config_func = [&bucketseq_to_partition, &mode](JoinRuntimeFilter* grf,
                                                            JoinRuntimeFilter::RunningContext* ctx) {
        grf->set_join_mode(mode);
        ctx->bucketseq_to_partition = &bucketseq_to_partition;
    };
    test_grf_helper(num_rows, num_partitions, part_by_func, grf_config_func);
}

void test_colocate_grf_helper(size_t num_rows, size_t num_partitions, size_t num_buckets,
                              std::vector<int> bucketseq_to_partition) {
    test_colocate_or_bucket_shuffle_grf_helper(num_rows, num_partitions, num_buckets, std::move(bucketseq_to_partition),
                                               TRuntimeFilterBuildJoinMode::COLOCATE);
}

void test_bucket_shuffle_grf_helper(size_t num_rows, size_t num_partitions, size_t num_buckets,
                                    std::vector<int> bucketseq_to_partition) {
    test_colocate_or_bucket_shuffle_grf_helper(num_rows, num_partitions, num_buckets, std::move(bucketseq_to_partition),
                                               TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
}

TEST_F(RuntimeFilterTest, TestColocateRuntimeFilter1) {
    test_colocate_grf_helper(100, 3, 6, {1, 1, 0, 0, 2, 2});
}

TEST_F(RuntimeFilterTest, TestColocateRuntimeFilter2) {
    test_colocate_grf_helper(100, 3, 7, {0, 1, 2, 0, 1, 2, 1});
}

TEST_F(RuntimeFilterTest, TestColocateRuntimeFilter3) {
    test_colocate_grf_helper(100, 1, 7, {0, 0, 0, 0, 0, 0, 0});
}

TEST_F(RuntimeFilterTest, TestColocateRuntimeFilterWithBucketAbsent1) {
    test_colocate_grf_helper(3, 1, 7, {0, 0, 0, 0, 0, 0, 0});
}

TEST_F(RuntimeFilterTest, TestColocateRuntimeFilterWithBucketAbsent2) {
    test_colocate_grf_helper(3, 3, 4, {0, 1, 2, 0});
}

void test_partitioned_or_shuffle_hash_bucket_grf_helper(size_t num_rows, size_t num_partitions,
                                                        TRuntimeFilterBuildJoinMode::type type) {
    auto part_by_func = [num_rows, num_partitions](BinaryColumn* column, std::vector<uint32_t>& hash_values,
                                                   std::vector<size_t>& num_rows_per_partitions) {
        hash_values.assign(num_rows, HashUtil::FNV_SEED);
        column->fnv_hash(hash_values.data(), 0, num_rows);
        for (auto i = 0; i < num_rows; ++i) {
            hash_values[i] %= num_partitions;
            ++num_rows_per_partitions[hash_values[i]];
        }
    };
    auto grf_config_func = [type](JoinRuntimeFilter* grf, JoinRuntimeFilter::RunningContext* ctx) {
        grf->set_join_mode(type);
    };
    test_grf_helper(num_rows, num_partitions, part_by_func, grf_config_func);
}

void test_partitioned_grf_helper(size_t num_rows, size_t num_partitions) {
    test_partitioned_or_shuffle_hash_bucket_grf_helper(num_rows, num_partitions,
                                                       TRuntimeFilterBuildJoinMode::PARTITIONED);
}
void test_shuffle_hash_bucket_grf_helper(size_t num_rows, size_t num_partitions) {
    test_partitioned_or_shuffle_hash_bucket_grf_helper(num_rows, num_partitions,
                                                       TRuntimeFilterBuildJoinMode::SHUFFLE_HASH_BUCKET);
}
TEST_F(RuntimeFilterTest, TestPartitionedRuntimeFilter1) {
    test_partitioned_grf_helper(100, 1);
}
TEST_F(RuntimeFilterTest, TestPartitionedRuntimeFilter2) {
    test_partitioned_grf_helper(100, 3);
}
TEST_F(RuntimeFilterTest, TestPartitionedRuntimeFilter3) {
    test_partitioned_grf_helper(100, 5);
}

TEST_F(RuntimeFilterTest, TestShuffleHashBucketRuntimeFilter1) {
    test_shuffle_hash_bucket_grf_helper(100, 1);
}
TEST_F(RuntimeFilterTest, TestShuffleHashBucketRuntimeFilter2) {
    test_shuffle_hash_bucket_grf_helper(100, 3);
}
TEST_F(RuntimeFilterTest, TestShuffleHashBucketRuntimeFilter3) {
    test_shuffle_hash_bucket_grf_helper(100, 5);
}

void test_local_hash_bucket_grf_helper(size_t num_rows, size_t num_partitions) {
    auto part_by_func = [num_rows, num_partitions](BinaryColumn* column, std::vector<uint32_t>& hash_values,
                                                   std::vector<size_t>& num_rows_per_partitions) {
        hash_values.assign(num_rows, 0);
        column->crc32_hash(hash_values.data(), 0, num_rows);
        for (auto i = 0; i < num_rows; ++i) {
            hash_values[i] %= num_partitions;
            ++num_rows_per_partitions[hash_values[i]];
        }
    };
    auto grf_config_func = [](JoinRuntimeFilter* grf, JoinRuntimeFilter::RunningContext* ctx) {
        grf->set_join_mode(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
    };
    test_grf_helper(num_rows, num_partitions, part_by_func, grf_config_func);
}

TEST_F(RuntimeFilterTest, TestLocalHashBucketRuntimeFilter1) {
    test_bucket_shuffle_grf_helper(100, 3, 6, {1, 1, 0, 0, 2, 2});
}

TEST_F(RuntimeFilterTest, TestLocalHashBucketRuntimeFilter2) {
    test_bucket_shuffle_grf_helper(100, 3, 7, {0, 1, 2, 0, 1, 2, 1});
}

TEST_F(RuntimeFilterTest, TestLocalHashBucketRuntimeFilter3) {
    test_bucket_shuffle_grf_helper(100, 1, 7, {0, 0, 0, 0, 0, 0, 0});
}

TEST_F(RuntimeFilterTest, TestLocalHashBucketRuntimeFilterWithBucketAbsent1) {
    test_bucket_shuffle_grf_helper(3, 1, 7, {0, 0, 0, 0, 0, 0, 0});
}

TEST_F(RuntimeFilterTest, TestLocalHashBucketRuntimeFilterWithBucketAbsent2) {
    test_bucket_shuffle_grf_helper(3, 3, 4, {0, 1, 2, 0});
}

TEST_F(RuntimeFilterTest, TestGlobalRuntimeFilterMinMax) {
    RuntimeBloomFilter<TYPE_INT> prototype;
    ObjectPool pool;

    RuntimeBloomFilter<TYPE_INT>* global = prototype.create_empty(&pool);
    for (int i = 0; i < 3; i++) {
        RuntimeBloomFilter<TYPE_INT> local;
        local.init(10);
        for (int j = 0; j < 4; j++) {
            int value = (i + 1) * 10 + j;
            local.insert(&value);
        }
        global->concat(&local);
    }
    EXPECT_EQ(global->min_value(), 10);
    EXPECT_EQ(global->max_value(), 33);
}

void TestMultiColumnsOnRuntimeFilter(TRuntimeFilterBuildJoinMode::type join_mode, std::vector<ColumnPtr> columns,
                                     int64_t num_rows, int64_t num_partitions,
                                     std::vector<int32_t> bucketseq_to_partition) {
    std::vector<uint32_t> expected_hash_values;
    std::vector<size_t> num_rows_per_partitions(num_partitions, 0);

    switch (join_mode) {
    case TRuntimeFilterBuildJoinMode::BORADCAST: {
        break;
    }
    case TRuntimeFilterBuildJoinMode::PARTITIONED:
    case TRuntimeFilterBuildJoinMode::SHUFFLE_HASH_BUCKET: {
        expected_hash_values.assign(num_rows, HashUtil::FNV_SEED);
        for (auto& column : columns) {
            column->fnv_hash(expected_hash_values.data(), 0, num_rows);
        }
        for (auto i = 0; i < num_rows; ++i) {
            expected_hash_values[i] %= num_partitions;
            ++num_rows_per_partitions[expected_hash_values[i]];
        }
        break;
    }
    case TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET:
    case TRuntimeFilterBuildJoinMode::COLOCATE: {
        expected_hash_values.assign(num_rows, 0);
        DCHECK_LT(0, bucketseq_to_partition.size());
        auto num_buckets = bucketseq_to_partition.size();
        for (auto& column : columns) {
            column->crc32_hash(expected_hash_values.data(), 0, num_rows);
        }
        std::vector<size_t> num_rows_per_bucket(num_buckets, 0);
        for (auto i = 0; i < num_rows; ++i) {
            expected_hash_values[i] %= num_buckets;
            ++num_rows_per_bucket[expected_hash_values[i]];
            expected_hash_values[i] = bucketseq_to_partition[expected_hash_values[i]];
            ++num_rows_per_partitions[expected_hash_values[i]];
        }
        static constexpr uint32_t BUCKET_ABSENT = 2147483647;
        for (auto b = 0; b < num_buckets; ++b) {
            if (num_rows_per_bucket[b] == 0) {
                bucketseq_to_partition[b] = BUCKET_ABSENT;
            }
        }
    }
    default:
        break;
    }

    JoinRuntimeFilter::RunningContext running_ctx;
    running_ctx.selection.assign(num_rows, 2);
    running_ctx.use_merged_selection = false;
    running_ctx.compatibility = true;
    running_ctx.bucketseq_to_partition = &bucketseq_to_partition;
    std::vector<Column*> column_ptrs;
    for (auto& column : columns) {
        column_ptrs.push_back(column.get());
    }

    int32_t num_column = columns.size();
    std::vector<RuntimeBloomFilter<TYPE_INT>> bfs(num_column * num_partitions);
    std::vector<RuntimeBloomFilter<TYPE_INT>> gfs(num_column);
    for (int i = 0; i < num_column; i++) {
        auto& column = columns[i];
        for (auto p = 0; p < num_partitions; ++p) {
            auto pp = p + (i * num_partitions);
            bfs[pp].init(num_rows_per_partitions[p]);
        }
        for (auto j = 0; j < num_rows; ++j) {
            auto ele = column->get(j).get_int32();
            auto pp = expected_hash_values[j] + (i * num_partitions);
            bfs[pp].insert(&ele);
        }
        for (auto p = 0; p < num_partitions; ++p) {
            auto pp = p + (i * num_partitions);
            gfs[i].concat(&bfs[pp]);
        }
        ASSERT_EQ(gfs[i].size(), num_rows);
        ASSERT_EQ(gfs[i].num_hash_partitions(), num_partitions);
    }
    // compute hash
    {
        auto& grf = gfs[0];
        grf.set_join_mode(join_mode);
        grf.compute_hash(column_ptrs, &running_ctx);
        auto& ctx_hash_values = running_ctx.hash_values;
        for (auto i = 0; i < num_rows; i++) {
            DCHECK_EQ(ctx_hash_values[i], expected_hash_values[i]);
        }
    }

    for (int i = 0; i < num_column; i++) {
        auto& grf = gfs[i];
        grf.set_join_mode(join_mode);
        grf.evaluate(column_ptrs[i], &running_ctx);
        auto true_count = SIMD::count_nonzero(running_ctx.selection.data(), num_rows);
        ASSERT_EQ(true_count, num_rows);
    }
}

ColumnPtr CreateSeriesColumnInt32(int32_t num_rows, bool nullable) {
    auto type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr column = ColumnHelper::create_column(type_desc, nullable);
    std::vector<int32_t> elements(num_rows);
    std::iota(elements.begin(), elements.end(), 0);
    for (auto& x : elements) {
        column->append_datum(Datum((int32_t)x));
    }
    return column;
}

TEST_F(RuntimeFilterTest, TestMultiColumnsOnRuntimeFilter_BucketJoin) {
    std::vector<ColumnPtr> columns;
    int32_t num_rows = 100;
    int32_t num_partition = 10;
    for (int i = 0; i < 10; i++) {
        columns.push_back(CreateSeriesColumnInt32(100, true));
    }

    return TestMultiColumnsOnRuntimeFilter(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET, columns, num_rows,
                                           num_partition, {0, 0, 1, 2, 2, 1});
}

TEST_F(RuntimeFilterTest, TestMultiColumnsOnRuntimeFilter_ShuffleJoin) {
    std::vector<ColumnPtr> columns;
    int32_t num_rows = 100;
    int32_t num_partition = 10;
    for (int i = 0; i < 10; i++) {
        columns.push_back(CreateSeriesColumnInt32(100, true));
    }
    return TestMultiColumnsOnRuntimeFilter(TRuntimeFilterBuildJoinMode::PARTITIONED, columns, num_rows, num_partition,
                                           {});
}

} // namespace starrocks
