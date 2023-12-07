// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "exprs/vectorized/like_predicate.h"
#include "exprs/vectorized/mock_vectorized_expr.h"

namespace starrocks::vectorized {

class LikeTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    TExprNode expr_node;
};

TEST_F(LikeTest, startConstPatternLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("test1%", 1);

    for (int j = 0; j < 20; ++j) {
        str->append("test" + std::to_string(j));
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        if (l >= 10 || l == 1) {
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_FALSE(v->get_data()[l]);
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, endConstPatternLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto null = NullColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%9test", 1);

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "test");
        null->append(j % 2 == 0);
    }

    columns.push_back(NullableColumn::create(str, null));
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_nullable());
    ASSERT_FALSE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int l = 0; l < 20; ++l) {
        if (l % 2 == 0) {
            ASSERT_TRUE(result->is_null(l));
        } else {
            ASSERT_FALSE(result->is_null(l));
        }

        if (l == 19 || l == 9) {
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_FALSE(v->get_data()[l]);
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, substringConstPatternLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%2test1%", 1);

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "test" + std::to_string(j));
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        if (l == 12) {
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_FALSE(v->get_data()[l]);
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, haystackConstantLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto haystack = ColumnHelper::create_const_column<TYPE_VARCHAR>("CHINA", 1);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%IN%", 1);

    columns.push_back(haystack);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, haystackConstantLikeLargerThanHyperscan) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto haystack = ColumnHelper::create_const_column<TYPE_VARCHAR>("CHINA", 1);

#define LONG_PATTERN_LEN 16384
    char large_pattern[LONG_PATTERN_LEN];
    memset(large_pattern, 'a', LONG_PATTERN_LEN);
    large_pattern[LONG_PATTERN_LEN - 1] = 0;
    large_pattern[0] = 'N';
    large_pattern[1] = '%';
    large_pattern[2] = 'I';
    large_pattern[3] = '%';
#undef LONG_PATTERN_LEN

    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(large_pattern, 1);

    columns.push_back(haystack);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, haystackNullableLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto haystack = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%es%", 1);

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        if (j % 2 == 0) {
            null->append(0);
            haystack->append("test");
        } else {
            null->append(1);
            haystack->append("test");
        }
    }

    columns.push_back(NullableColumn::create(haystack, null));
    columns.push_back(pattern);
    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_nullable());
    ASSERT_FALSE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int l = 0; l < 20; ++l) {
        if (l % 2 == 0) {
            ASSERT_FALSE(result->is_null(l));
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_TRUE(result->is_null(l));
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, patternEmptyLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%%", 1);

    for (int j = 0; j < 20; ++j) {
        str->append("test");
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        ASSERT_TRUE(v->get_data()[l]);
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, patternStrAndPatternBothEmptyLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);

    for (int j = 0; j < 20; ++j) {
        str->append("");
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        ASSERT_TRUE(v->get_data()[l]);
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, patternStrAndPatternBothEmptyExplicitNullPtrLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);

    const char* null_ptr = nullptr;
    for (int j = 0; j < 20; ++j) {
        str->append(Slice(null_ptr, 0));
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        ASSERT_TRUE(v->get_data()[l]);
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, patternOnlyNullLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = ColumnHelper::create_const_null_column(1);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("%Test%", 1);

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_TRUE(result->is_nullable());

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, rowsPatternLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "test" + std::to_string(j));

        if (j % 2 == 0) {
            pattern->append("%test" + std::to_string(j) + "%");
        } else {
            pattern->append("????%");
        }
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        if (l % 2 == 0) {
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_FALSE(v->get_data()[l]);
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, rowsNullablePatternLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "test" + std::to_string(j));

        if (j % 2 == 0) {
            pattern->append("%test" + std::to_string(j) + "%");
            null->append(0);
        } else {
            pattern->append("????%");
            null->append(1);
        }
    }

    columns.push_back(str);
    columns.push_back(NullableColumn::create(pattern, null));

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();

    ASSERT_TRUE(result->is_nullable());
    ASSERT_FALSE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int l = 0; l < 20; ++l) {
        if (l % 2 == 0) {
            ASSERT_TRUE(v->get_data()[l]);
            ASSERT_FALSE(result->is_null(l));
        } else {
            ASSERT_FALSE(v->get_data()[l]);
            ASSERT_TRUE(result->is_null(l));
        }
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, rowsPatternRegex) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "test" + std::to_string(j));
        pattern->append(".+\\d\\d");
    }

    columns.push_back(str);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::regex_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::regex(context, columns).value();

    ASSERT_TRUE(result->is_numeric());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int l = 0; l < 20; ++l) {
        if (l > 9) {
            ASSERT_TRUE(v->get_data()[l]);
        } else {
            ASSERT_FALSE(v->get_data()[l]);
        }
    }

    ASSERT_TRUE(LikePredicate::regex_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, constValueLike) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);

    const int num_rows = 10;

    auto value_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("abcd", num_rows);
    auto pattern_col = BinaryColumn::create();
    pattern_col->append("abc");
    pattern_col->append("ab%");
    pattern_col->append("abc_");
    pattern_col->append("%cd");
    pattern_col->append("_bcd");
    pattern_col->append("a%d");
    pattern_col->append("ab_d");
    pattern_col->append("abcd");
    pattern_col->append("abcm");
    pattern_col->append("abcd_");

    bool expected[num_rows] = {false, true, true, true, true, true, true, true, false, false};

    Columns columns;
    columns.emplace_back(std::move(value_col));
    columns.emplace_back(std::move(pattern_col));
    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::like_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::like(context, columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_EQ(num_rows, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    for (int i = 0; i < num_rows; ++i) {
        ASSERT_EQ(expected[i], v->get_data()[i]);
    }

    ASSERT_TRUE(LikePredicate::like_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, constValueRegexp) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);

    const int num_rows = 4;

    auto value_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("abcd", num_rows);
    auto pattern_col = BinaryColumn::create();
    pattern_col->append("abc");
    pattern_col->append("ab.*");
    pattern_col->append("abcd");
    pattern_col->append("abcm");

    bool expected[num_rows] = {true, true, true, false};

    Columns columns;
    columns.emplace_back(std::move(value_col));
    columns.emplace_back(std::move(pattern_col));
    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::regex_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::regex(context, columns).value();
    ASSERT_TRUE(result->is_numeric());
    ASSERT_EQ(num_rows, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);
    for (int i = 0; i < num_rows; ++i) {
        ASSERT_EQ(expected[i], v->get_data()[i]);
    }

    ASSERT_TRUE(LikePredicate::regex_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, constValueRegexpLargerThanHyperscan) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto haystack = ColumnHelper::create_const_column<TYPE_VARCHAR>("CHINA", 1);

#define LONG_PATTERN_LEN 16384
    char large_pattern[LONG_PATTERN_LEN];
    memset(large_pattern, 'a', LONG_PATTERN_LEN);
    large_pattern[LONG_PATTERN_LEN - 1] = 0;
    large_pattern[2] = '.';
#undef LONG_PATTERN_LEN

    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(large_pattern, 1);

    columns.push_back(haystack);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::regex_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::regex(context, columns).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));

    ASSERT_TRUE(LikePredicate::regex_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

TEST_F(LikeTest, constValueLikeComplicateForHyperscan) {
    auto context = FunctionContext::create_test_context();
    std::unique_ptr<FunctionContext> ctx(context);
    Columns columns;

    auto haystack = ColumnHelper::create_const_column<TYPE_VARCHAR>("CHINA", 1);

    std::string large_pattern =
            "(肉|便|便|胃|补钙|不喝奶|心|胖|服|食|不.{0,3}(想|要|愿|喜欢|爱|肯|好好|怎么).{0,3}(吃|喝)|擦鼻涕|肠梗阻|("
            "肠|胃).{0,2}(敏感|弱|好|适应|舒服|难受|差|问题|炎)|吃不下饭|吃撑了|吃(的|得).{0,2}(少|多|饱|吐)|吃奶|打嗝|"
            "低血糖|肚子饿|肚子疼|断奶|断食|饿肚子|饿坏了|发胖|发腮|肥胖|疯狂吃|腹泻|感冒|干呕|过敏|咳嗽|没吃饱|(好|很|"
            "太|有点|有些)胖|(好|很|太|有点|有些)瘦|怀孕|换口味|减肥|焦虑|戒奶|拉肚子|拉稀|精神不好|拒绝吃|口臭|口炎|"
            "狂吃|拉屎|cccccccccc|细小|毛囊炎|没精神|磨牙|奶.{0,2}不(够|足)|没有奶|奶癣|呕吐|胖不起来|皮肤病|偏瘦|缺钙|"
            "缺维耐|便|火|好|竭|生|振|了|病|吃|病|(吃|喝)奶|不(好|了|良)|牙.{0,2}不好|牙(疼|痛)|腺|抑|(好|够|足|良)|越|"
            "增|毛|胖|牙|奶|肉|毒|暑|总).{0,5}(永)";
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(large_pattern, 1);

    columns.push_back(haystack);
    columns.push_back(pattern);

    context->impl()->set_constant_columns(columns);

    ASSERT_TRUE(LikePredicate::regex_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = LikePredicate::regex(context, columns).value();

    ASSERT_TRUE(result->is_constant());
    ASSERT_FALSE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));

    ASSERT_TRUE(LikePredicate::regex_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                        .ok());
}

} // namespace starrocks::vectorized
