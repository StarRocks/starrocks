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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <paimon/bucket/bucket_id_calculator.h>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "connector/hive/paimon/paimon_write_column_converter.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "exprs/paimon_functions.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
std::string configuration(const std::string& fields, const std::string& algorithm = "default", bool pk = false) {
    return "{\"version\":1,\"bucket_count\":17,\"is_pk\":" + std::string(pk ? "true" : "false") + ",\"algorithm\":\"" +
           algorithm + "\",\"time_zone\":\"UTC\",\"memory_limit\":\"268435456\",\"fields\":" + fields + "}";
}

void compare_sdk(const std::string& config, const ColumnPtr& keys, const TypeDescriptor& type,
                 const std::shared_ptr<arrow::Schema>& schema, std::unique_ptr<paimon::BucketIdCalculator> sdk) {
    auto calculator = starrocks_paimon_bucket_calculator_v1(config, type, nullptr);
    ASSERT_TRUE(calculator.ok()) << calculator.status();
    auto expanded = ColumnHelper::unpack_and_duplicate_const_column(keys->size(), keys);
    const auto* structure = down_cast<const StructColumn*>(expanded.get());
    auto batch = paimon_write_columns_to_arrow(structure->fields(), type.children, schema, "UTC",
                                               arrow::default_memory_pool());
    ASSERT_TRUE(batch.ok()) << batch.status();
    ArrowArray array{};
    ArrowSchema c_schema{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch.value(), &array, &c_schema).ok());
    std::vector<int32_t> expected(keys->size());
    ASSERT_TRUE(sdk->CalculateBucketIds(&array, &c_schema, expected.data()).ok());
    // Reuse the same calculator across batches, including after the first Arrow batch is released.
    for (int batch = 0; batch < 3; ++batch) {
        auto result = calculator.value()->calculate(keys);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(expected.size(), result.value()->size());
        for (size_t i = 0; i < expected.size(); ++i) EXPECT_EQ(expected[i], result.value()->get(i).get_int32());
    }
}

class PaimonBucketFunctionTest : public ::testing::Test {
protected:
    void SetUp() override {
        SyncPoint::GetInstance()->SetCallBack("PaimonFunctions::load_calculator", [](void* arg) {
            *static_cast<CreatePaimonBucketCalculator*>(arg) = starrocks_paimon_bucket_calculator_v1;
        });
        SyncPoint::GetInstance()->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("PaimonFunctions::load_calculator");
        SyncPoint::GetInstance()->ClearTrace();
    }
};

TEST_F(PaimonBucketFunctionTest, CompositeNullableKeysMatchSdkAcrossBatches) {
    auto ids = Int32Column::create();
    ids->append(1);
    ids->append(-5);
    ids->append(42);
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);
    auto names = BinaryColumn::create();
    names->append("x");
    names->append("y");
    names->append("z");
    ColumnPtr keys = StructColumn::create(Columns{NullableColumn::create(ids, nulls), names}, {"id", "name"});
    auto type = TypeDescriptor::create_struct_type({"id", "name"},
                                                   {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)});
    auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    for (bool pk : {false, true}) {
        auto sdk = paimon::BucketIdCalculator::Create(pk, 17, paimon::GetDefaultPool());
        ASSERT_TRUE(sdk.ok());
        compare_sdk(configuration(R"([{"name":"id","type":"INT"},{"name":"name","type":"STRING"}])", "default", pk),
                    keys, type, schema, std::move(sdk).value());
    }
}

TEST_F(PaimonBucketFunctionTest, ModHiveAndConstantKeysMatchSdk) {
    auto id = Int32Column::create();
    id->append(-42);
    ColumnPtr keys = ConstColumn::create(StructColumn::create(Columns{id}, {"id"}), 8);
    auto type = TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)});
    auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    auto mod = paimon::BucketIdCalculator::CreateMod(true, 17, paimon::FieldType::INT, paimon::GetDefaultPool());
    ASSERT_TRUE(mod.ok());
    compare_sdk(configuration(R"([{"name":"id","type":"INT"}])", "mod", true), keys, type, schema,
                std::move(mod).value());
    auto hive = paimon::BucketIdCalculator::CreateHive(false, 17, {paimon::HiveFieldInfo(paimon::FieldType::INT)},
                                                       paimon::GetDefaultPool());
    ASSERT_TRUE(hive.ok());
    compare_sdk(configuration(R"([{"name":"id","type":"INT"}])", "hive"), keys, type, schema, std::move(hive).value());
}

TEST_F(PaimonBucketFunctionTest, FunctionLifecycleUsesLinkedCalculatorAndReturnsActualBuckets) {
    auto type = TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)});
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARCHAR), type}, TypeDescriptor(TYPE_INT)));
    auto text = BinaryColumn::create();
    text->append(configuration(R"([{"name":"id","type":"INT"}])", "mod", true));
    ColumnPtr config = ConstColumn::create(text, 1);
    context->set_constant_columns({config, nullptr});
    ASSERT_OK(PaimonFunctions::prepare(context.get(), FunctionContext::FRAGMENT_LOCAL));
    auto ids = Int32Column::create();
    for (int32_t id : {0, 1, 16, 17, 18, 35}) ids->append(id);
    const std::vector<int32_t> expected = {0, 1, 16, 0, 1, 1};
    ColumnPtr keys = StructColumn::create(Columns{ids}, {"id"});
    for (int batch = 0; batch < 3; ++batch) {
        auto result = PaimonFunctions::bucket(context.get(), {config, keys});
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(expected.size(), result.value()->size());
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(expected[row], result.value()->get(row).get_int32());
        }
    }
    ASSERT_OK(PaimonFunctions::close(context.get(), FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(PaimonBucketFunctionTest, StandardFunctionExpressionBuildsAndEvaluatesBucketProjection) {
    for (bool with_profile : {false, true}) {
        auto runtime =
                with_profile ? std::make_unique<RuntimeState>(TQueryGlobals{}) : std::make_unique<RuntimeState>();
        auto& state = *runtime;
        state.init_instance_mem_tracker();
        ObjectPool pool;
        auto type = TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)});
        auto node = [](TExprNodeType::type kind, const TypeDescriptor& type, int children) {
            TExprNode result;
            result.__set_node_type(kind);
            result.__set_type(type.to_thrift());
            result.__set_num_children(children);
            result.__set_is_nullable(false);
            result.__set_output_scale(0);
            return result;
        };
        auto function = [](const std::string& name, int64_t id, const std::vector<TTypeDesc>& args,
                           const TTypeDesc& result) {
            TFunction fn;
            TFunctionName n;
            n.__set_function_name(name);
            fn.__set_name(n);
            fn.__set_fid(id);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            fn.__set_arg_types(args);
            fn.__set_ret_type(result);
            fn.__set_has_var_args(false);
            return fn;
        };
        auto string_type = TypeDescriptor(TYPE_VARCHAR);
        auto int_type = TypeDescriptor(TYPE_INT);
        auto bucket = node(TExprNodeType::FUNCTION_CALL, int_type, 2);
        bucket.__set_fn(
                function("__paimon_bucket", 10360, {string_type.to_thrift(), type.to_thrift()}, int_type.to_thrift()));
        auto literal = node(TExprNodeType::STRING_LITERAL, string_type, 0);
        TStringLiteral text;
        text.__set_value(configuration(R"([{"name":"id","type":"INT"}])", "mod", true));
        literal.__set_string_literal(text);
        auto row = node(TExprNodeType::FUNCTION_CALL, type, 1);
        row.__set_fn(function("row", 170500, {int_type.to_thrift()}, type.to_thrift()));
        auto ref = node(TExprNodeType::SLOT_REF, int_type, 0);
        TSlotRef slot;
        slot.__set_slot_id(0);
        slot.__set_tuple_id(0);
        ref.__set_slot_ref(slot);
        TExpr serialized;
        serialized.__set_nodes({bucket, literal, row, ref});
        ExprContext* expression = nullptr;
        ASSERT_OK(ExprFactory::create_expr_tree(&pool, serialized, &expression, &state));
        ASSERT_OK(ExprExecutor::prepare(expression, &state));
        ASSERT_OK(ExprExecutor::open(expression, &state));
        auto ids = Int32Column::create();
        ids->append(1);
        ids->append(18);
        ids->append(19);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ids, 0);
        auto result = expression->evaluate(chunk.get());
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(3, result.value()->size());
        EXPECT_EQ(1, result.value()->get(0).get_int32());
        EXPECT_EQ(1, result.value()->get(1).get_int32());
        EXPECT_EQ(2, result.value()->get(2).get_int32());
        ExprExecutor::close(expression, &state);
    }
}

TEST_F(PaimonBucketFunctionTest, FunctionPrepareRejectsNonconstantConfiguration) {
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)})},
            TypeDescriptor(TYPE_INT)));
    context->set_constant_columns({nullptr, nullptr});
    EXPECT_FALSE(PaimonFunctions::prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(PaimonBucketFunctionTest, RejectsMalformedConfigurationAndUnsupportedAlgorithm) {
    auto type = TypeDescriptor::create_struct_type({"id"}, {TypeDescriptor(TYPE_INT)});
    for (const auto& config :
         {std::string("{}"), configuration("[]", "unknown"), configuration(R"([{"name":"id","type":"INT"}])", "mod")}) {
        auto wrong_type = type;
        if (config.find("mod") != std::string::npos) wrong_type.children.clear();
        EXPECT_FALSE(starrocks_paimon_bucket_calculator_v1(config, wrong_type, nullptr).ok());
    }
}
} // namespace
} // namespace starrocks
