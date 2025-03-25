#include "udf/java/java_data_converter.h"

#include <gtest/gtest.h>

#include <vector>

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"

namespace starrocks {
class DataConverterTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DataConverterTest, cast_to_jval) {
    std::vector<LogicalType> types = {TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR};

    for (auto type : types) {
        TypeDescriptor tdesc(type);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }

    for (auto type : types) {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(type);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }

    for (auto type2 : types) {
        for (auto type1 : types) {
            TypeDescriptor tdesc(TYPE_MAP);
            tdesc.children.emplace_back(type1);
            tdesc.children.emplace_back(type2);
            auto c1 = ColumnHelper::create_column(tdesc, true);
            c1->append_default();
            auto c2 = ColumnHelper::create_column(tdesc, false);
            c2->append_default();
            ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
            jobject obj1 = val1.l;
            LOCAL_REF_GUARD(obj1);
            ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
            jobject obj2 = val2.l;
            LOCAL_REF_GUARD(obj2);
        }
    }

    {
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_ARRAY);
        ta.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }
}

TEST_F(DataConverterTest, convert_to_boxed_array) {
    std::vector<LogicalType> types = {TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR};
    FunctionContext context;
    std::vector<FunctionContext::TypeDesc> args;
    for (auto type : types) {
        args.clear();
        args.emplace_back(type);
        context._arg_types = args;
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_ARRAY);
        ta.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);

        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();

        std::vector<jobject> res;
        std::vector<const Column*> columns;
        columns.resize(1);
        columns[0] = c1.get();
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(&context, columns.data(), 1, 1, &res));

        res.clear();
        columns[0] = c2.get();
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(&context, columns.data(), 1, 1, &res));
    }
}
} // namespace starrocks