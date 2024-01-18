// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/hyperloglog_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "exprs/vectorized/mock_vectorized_expr.h"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {
class HyperLogLogFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        ctx_ptr.reset(FunctionContext::create_test_context());
        ctx = ctx_ptr.get();
    }

private:
    std::unique_ptr<FunctionContext> ctx_ptr;
    FunctionContext* ctx;
};

TEST_F(HyperLogLogFunctionsTest, hllEmptyTest) {
    {
        Columns c;
        auto column = HyperloglogFunctions::hll_empty(ctx, c).value();

        ASSERT_TRUE(column->is_constant());

        auto* hll = ColumnHelper::get_const_value<TYPE_HLL>(column);

        ASSERT_EQ(1, hll->empty().size());
    }
}

TEST_F(HyperLogLogFunctionsTest, hllHashTest) {
    {
        Columns columns;

        auto col1 = BinaryColumn::create();
        col1->append(Slice("test1"));
        col1->append(Slice("test2"));
        col1->append(Slice("test3"));
        col1->append(Slice("test1"));

        columns.push_back(col1);

        auto v = HyperloglogFunctions::hll_hash(ctx, columns).value();

        ASSERT_TRUE(v->is_object());

        auto p = ColumnHelper::cast_to<TYPE_HLL>(v);
        ASSERT_EQ(1, p->get_object(0)->estimate_cardinality());
        ASSERT_EQ(1, p->get_object(1)->estimate_cardinality());
        ASSERT_EQ(1, p->get_object(2)->estimate_cardinality());
        ASSERT_EQ(1, p->get_object(3)->estimate_cardinality());

        p->get_object(0)->merge(*p->get_object(1));

        ASSERT_EQ(2, p->get_object(0)->estimate_cardinality());
    }
}

TEST_F(HyperLogLogFunctionsTest, hllCardinalityTest) {
    {
        Columns columns;

        auto col1 = HyperLogLogColumn::create();

        HyperLogLog h1;
        h1.update(1);
        HyperLogLog h2;
        h2.update(1);
        h2.update(2);
        h2.update(1);

        HyperLogLog h3;
        h3.update(2);
        h3.update(2);
        h3.update(2);

        HyperLogLog h4;
        h4.update(3);
        h4.update(2);
        h4.update(5);

        col1->append(std::move(h1));
        col1->append(std::move(h2));
        col1->append(std::move(h3));
        col1->append(std::move(h4));

        columns.push_back(col1);

        auto v = HyperloglogFunctions::hll_cardinality(ctx, columns).value();

        ASSERT_TRUE(v->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BIGINT>(v);
        ASSERT_EQ(1, p->get_data()[0]);
        ASSERT_EQ(2, p->get_data()[1]);
        ASSERT_EQ(1, p->get_data()[2]);
        ASSERT_EQ(3, p->get_data()[3]);
    }
}

TEST_F(HyperLogLogFunctionsTest, hllCardinalityFromStringTest) {
    {
        Columns columns;

        auto col1 = BinaryColumn::create();

        HyperLogLog h1;
        h1.update(1);
        HyperLogLog h2;
        h2.update(1);
        h2.update(2);
        h2.update(1);

        HyperLogLog h3;
        h3.update(2);
        h3.update(2);
        h3.update(2);

        HyperLogLog h4;
        h4.update(3);
        h4.update(2);
        h4.update(5);

        uint8_t s1[h1.max_serialized_size()];
        uint8_t s2[h2.max_serialized_size()];
        uint8_t s3[h3.max_serialized_size()];
        uint8_t s4[h4.max_serialized_size()];

        size_t sz1 = h1.serialize(s1);
        size_t sz2 = h2.serialize(s2);
        size_t sz3 = h3.serialize(s3);
        size_t sz4 = h4.serialize(s4);

        col1->append(Slice(s1, sz1));
        col1->append(Slice(s2, sz2));
        col1->append(Slice(s3, sz3));
        col1->append(Slice(s4, sz4));

        HyperLogLog t1;

        columns.push_back(col1);

        auto v = HyperloglogFunctions::hll_cardinality_from_string(ctx, columns).value();

        ASSERT_TRUE(v->is_numeric());

        auto p = ColumnHelper::cast_to<TYPE_BIGINT>(v);
        ASSERT_EQ(h1.estimate_cardinality(), p->get_data()[0]);
        ASSERT_EQ(h2.estimate_cardinality(), p->get_data()[1]);
        ASSERT_EQ(h3.estimate_cardinality(), p->get_data()[2]);
        ASSERT_EQ(h4.estimate_cardinality(), p->get_data()[3]);
    }
}

TEST_F(HyperLogLogFunctionsTest, hllSerializeTest) {
    {
        auto col1 = HyperLogLogColumn::create();

        HyperLogLog h1;
        h1.update(1);
        HyperLogLog h2;
        h2.update(1);
        h2.update(2);
        h2.update(1);

        HyperLogLog h3;
        h3.update(2);
        h3.update(2);
        h3.update(2);

        HyperLogLog h4;
        h4.update(3);
        h4.update(2);
        h4.update(5);

        HyperLogLog h5;
        for (int i = 0; i < 10000; i++) {
            h5.update(i);
        }

        HyperLogLog h6;
        for (int i = 0; i < 100000; i++) {
            h6.update(i);
        }

        col1->append(std::move(h1));
        col1->append(std::move(h2));
        col1->append(std::move(h3));
        col1->append(std::move(h4));
        col1->append(std::move(h5));
        col1->append(std::move(h6));

        ColumnPtr v = nullptr;
<<<<<<< HEAD
        v = HyperloglogFunction::hll_cardinality(ctx, {col1});
        ASSERT_TRUE(v->is_numeric());
        auto expect = ColumnHelper::cast_to<TYPE_BIGINT>(v);


        v = HyperloglogFunction::hll_serialize(ctx, {col1});
        ASSERT_TRUE(v->is_binary());
        v = HyperloglogFunction::hll_deserialize(ctx, {v});
        ASSERT_TRUE(v->is_object());
        v = HyperloglogFunction::hll_cardinality(ctx, {v});
=======
        v = HyperloglogFunctions::hll_cardinality(ctx, {col1}).value();
        ASSERT_TRUE(v->is_numeric());
        auto expect = ColumnHelper::cast_to<TYPE_BIGINT>(v);

        v = HyperloglogFunctions::hll_serialize(ctx, {col1}).value();
        ASSERT_TRUE(v->is_binary());
        v = HyperloglogFunctions::hll_deserialize(ctx, {v}).value();
        ASSERT_TRUE(v->is_object());
        v = HyperloglogFunctions::hll_cardinality(ctx, {v}).value();
>>>>>>> 2.5.18
        ASSERT_TRUE(v->is_numeric());

        auto autcal = ColumnHelper::cast_to<TYPE_BIGINT>(v);

        ASSERT_EQ(expect->size(), autcal->size());
        for (size_t i = 0; i < expect->size(); i++) {
            ASSERT_EQ(expect->get_data()[i], autcal->get_data()[i]);
        }
    }
}
} // namespace starrocks::vectorized
