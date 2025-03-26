#include "udf/java/java_data_converter.h"

#include <gtest/gtest.h>

#include <vector>

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"
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

    {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(TYPE_INT);
        auto i32c = Int32Column::create();
        auto& elements_data = i32c->get_data();
        elements_data.resize(20);
        for (size_t i = 0; i < elements_data.size(); ++i) {
            elements_data[i] = i;
        }
        auto nullable = NullableColumn::wrap_if_necessary(std::move(i32c));
        auto offsets = UInt32Column::create();
        auto& offsets_data = offsets->get_data();
        offsets_data.emplace_back(0);
        offsets_data.emplace_back(2);
        offsets_data.emplace_back(10);
        offsets_data.emplace_back(20);
        std::string target;
        auto arr_col = ArrayColumn::create(std::move(nullable), std::move(offsets));
        auto& instance = JVMFunctionHelper::getInstance();
        for (size_t i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(jvalue v, cast_to_jvalue(tdesc, true, arr_col.get(), i));
            jobject obj = v.l;
            target = target + instance.to_string(obj);
            LOCAL_REF_GUARD(obj);
        }
        ASSERT_EQ(target, "[0, 1][2, 3, 4, 5, 6, 7, 8, 9][10, 11, 12, 13, 14, 15, 16, 17, 18, 19]");
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

    {
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);

        auto keys = Int32Column::create();
        auto& elements_data = keys->get_data();
        elements_data.resize(20);
        for (size_t i = 0; i < elements_data.size(); ++i) {
            elements_data[i] = i;
        }
        auto nullable = NullableColumn::wrap_if_necessary(std::move(keys));
        auto offsets = UInt32Column::create();
        auto& offsets_data = offsets->get_data();

        offsets_data.emplace_back(0);
        offsets_data.emplace_back(2);
        offsets_data.emplace_back(10);
        offsets_data.emplace_back(20);

        auto values = Int32Column::create();
        auto& values_data = values->get_data();
        values_data.resize(elements_data.size());
        for (size_t i = 0; i < elements_data.size(); ++i) {
            values_data[i] = elements_data.size() - i;
        }
        auto vnullable = NullableColumn::wrap_if_necessary(std::move(values));
        auto map_column = MapColumn::create(std::move(nullable), std::move(vnullable), std::move(offsets));
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(tdesc, true, map_column.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        auto& instance = JVMFunctionHelper::getInstance();
        std::string result = instance.to_string(obj);
        ASSERT_EQ("{0=20, 1=19}", result);
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

TEST_F(DataConverterTest, append_jvalue) {
    {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(TYPE_INT);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        auto c2 = ColumnHelper::create_column(tdesc, true);
        Datum datum = std::vector<Datum>{1, 2, 3};
        c2->append_datum(datum);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj = val2.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_OK(append_jvalue(tdesc, true, c1.get(), val2));
    }
    {
        TypeDescriptor tdesc(TYPE_MAP);
        tdesc.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(TYPE_INT);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        auto c2 = ColumnHelper::create_column(tdesc, true);
        c2->append_datum(DatumMap{{1, 34}});
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj = val2.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_OK(append_jvalue(tdesc, true, c1.get(), val2));
        ASSERT_EQ(c1->debug_item(0), "{1:34}");
    }
}
} // namespace starrocks