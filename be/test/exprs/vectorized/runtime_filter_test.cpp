// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/runtime_filter.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "exprs/vectorized/runtime_filter_bank.h"

namespace starrocks {
namespace vectorized {

class RuntimeFilterTest : public ::testing::Test {
public:
    void SetUp() {}
    void TearDown() {}

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
        EXPECT_TRUE(bf.test_data(i));
        EXPECT_FALSE(bf.test_data(i + 1));
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
    auto& selection = ctx.selection;
    selection.assign(column->size(), 1);
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
        EXPECT_TRUE(bf.test_data(s));
    }
    std::vector<std::string> ex_data = {"ee", "ff", "gg"};
    for (const auto& s : ex_data) {
        EXPECT_FALSE(bf.test_data(Slice(s)));
    }
}

TEST_F(RuntimeFilterTest, TestJoinRuntimeFilterSerialize) {
    RuntimeBloomFilter<TYPE_INT> bf0;
    JoinRuntimeFilter* rf0 = &bf0;
    bf0.init(100);
    for (int i = 0; i <= 200; i += 17) {
        bf0.insert(&i);
    }

    size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf0);
    std::vector<uint8_t> buffer(max_size, 0);
    size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf0, buffer.data());
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

    ObjectPool pool;
    size_t max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf0);
    std::vector<uint8_t> buffer0(max_size, 0);
    size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf0, buffer0.data());
    buffer0.resize(actual_size);
    JoinRuntimeFilter* rf2 = nullptr;
    RuntimeFilterHelper::deserialize_runtime_filter(&pool, &rf2, buffer0.data(), actual_size);

    max_size = RuntimeFilterHelper::max_runtime_filter_serialized_size(rf1);
    buffer0.assign(max_size, 0);
    actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf1, buffer0.data());
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
        EXPECT_TRUE(bf2.test_data(i));
        EXPECT_TRUE(bf2.test_data(i + 1));
        EXPECT_FALSE(bf2.test_data(i + 2));
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
        size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf0, (uint8_t*)buf.data());
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
        size_t actual_size = RuntimeFilterHelper::serialize_runtime_filter(rf1, (uint8_t*)buf.data());
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

} // namespace vectorized
} // namespace starrocks
