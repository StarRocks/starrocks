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

#include <gtest/gtest.h>

#include <filesystem>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "exec/hdfs_scanner.h"
#include "exprs/binary_predicate.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_reader.h"
#include "formats/parquet/parquet_ut_base.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats{};

class IcebergSchemaEvolutionTest : public testing::Test {
public:
    void SetUp() override { _runtime_state = _pool.add(new RuntimeState(TQueryGlobals())); }
    void TearDown() override {}

protected:
    // Created by: parquet-mr version 1.12.3 (build f8dced182c4c1fbdec6ccb3185537b5a01e6ed6b)
    // Properties:
    //   iceberg.schema: {"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"col","required":true,"type":{"type":"struct","fields":[{"id":3,"name":"a","required":false,"type":"int"},{"id":4,"name":"b","required":false,"type":"int"},{"id":5,"name":"c","required":false,"type":"int"}]}}]}
    // Schema:
    // message table {
    //   required int64 id = 1;
    //   required group col = 2 {
    //     optional int32 a = 3;
    //     optional int32 b = 4;
    //     optional int32 c = 5;
    //   }
    // }
    const std::string add_struct_subfield_file_path =
            "./be/test/formats/parquet/test_data/iceberg_schema_evolution/add_struct_subfield.parquet";

    /**
    * File path:  00000-0-bcfe7a47-24d9-4ecc-a228-e5eeebaba356-00001.parquet
      Created by: parquet-mr version 1.13.1 (build db4183109d5b734ec5930d870cdae161e408ddba)
      Properties:
      iceberg.schema: {"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"int"},{"id":2,"name":"col","required":true,"type":{"type":"struct","fields":[{"id":3,"name":"age","required":true,"type":"int"},{"id":4,"name":"detail","required":true,"type":{"type":"struct","fields":[{"id":5,"name":"height","required":true,"type":"int"},{"id":6,"name":"sex","required":true,"type":"int"}]}}]}}]}
      Schema:
        message table {
            required int32 id = 1;
            required group col = 2 {
                required int32 age = 3;
                required group detail = 4 {
                    required int32 height = 5;
                    required int32 sex = 6;
                }
            }
        }
     */
    const std::string struct_map_array_file_path =
            "./be/test/formats/parquet/test_data/iceberg_schema_evolution/struct_map_array.parquet";

    // message hive_schema {
    //   optional int32 c1;
    //   optional int64 c2;
    //   optional binary c3 (STRING);
    //   optional int96 c4;
    // }
    const std::string no_field_id_file_path = "./be/test/exec/test_data/parquet_scanner/file_reader_test.parquet2";

    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path) {
        return *FileSystem::Default()->new_random_access_file(file_path);
    }

    HdfsScannerContext* _create_scan_context() {
        auto* ctx = _pool.add(new HdfsScannerContext());
        auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
        ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
        ctx->stats = &g_hdfs_scan_stats;
        return ctx;
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0) {
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = file_path;
        scan_range->file_length = std::filesystem::file_size(file_path);
        scan_range->offset = 4;
        scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;
        return scan_range;
    }

    std::shared_ptr<RowDescriptor> _row_desc = nullptr;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
};

TEST_F(IcebergSchemaEvolutionTest, TestStructAddSubfield) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    TIcebergSchemaField field_col_b{};
    field_col_b.__set_field_id(4);
    field_col_b.__set_name("b");

    TIcebergSchemaField field_col_c{};
    field_col_c.__set_field_id(5);
    field_col_c.__set_name("c");

    TIcebergSchemaField field_col_d{};
    field_col_d.__set_field_id(6);
    field_col_d.__set_name("d");

    std::vector<TIcebergSchemaField> subfields{field_col_a, field_col_b, field_col_c, field_col_d};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("a");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("b");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("c");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("d");

    Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

    TupleDescriptor* tuple_desc = parquet::Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, {a:2,b:3,c:4,d:NULL}]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestStructEvolutionPadNull) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    TIcebergSchemaField field_col_b{};
    field_col_b.__set_field_id(4);
    field_col_b.__set_name("b");

    TIcebergSchemaField field_col_c{};
    field_col_c.__set_field_id(5);
    field_col_c.__set_name("c");

    TIcebergSchemaField field_col_d{};
    field_col_d.__set_field_id(6);
    field_col_d.__set_name("d");

    std::vector<TIcebergSchemaField> subfields{field_col_a, field_col_d};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("d");

    Utils::SlotDesc slot_descs[] = {{"col", col}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestStructDropSubfield) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    TIcebergSchemaField field_col_b{};
    field_col_b.__set_field_id(4);
    field_col_b.__set_name("b");

    std::vector<TIcebergSchemaField> subfields{field_col_a, field_col_b};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("a");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("b");

    Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, {a:2,b:3}]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestStructReorderSubfield) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_b{};
    field_col_b.__set_field_id(4);
    field_col_b.__set_name("b");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    std::vector<TIcebergSchemaField> subfields{field_col_a, field_col_b, field_col_b, field_col_a};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("b");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("a");

    Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, {b:3,a:2}]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestStructRenameSubfield) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a_rename");

    TIcebergSchemaField field_col_b{};
    field_col_b.__set_field_id(4);
    field_col_b.__set_name("b_rename");

    TIcebergSchemaField field_col_c{};
    field_col_c.__set_field_id(5);
    field_col_c.__set_name("c_rename");

    TIcebergSchemaField field_col_d{};
    field_col_d.__set_field_id(6);
    field_col_d.__set_name("d_rename");

    std::vector<TIcebergSchemaField> subfields{field_col_a, field_col_b, field_col_c, field_col_d};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("a_rename");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("b_rename");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("c_rename");

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("d_rename");

    Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, {a_rename:2,b_rename:3,c_rename:4,d_rename:NULL}]", chunk->debug_row(0));
}

static void _create_null_conjunct_ctxs(SlotId slot_id, std::vector<ExprContext*>* conjunct_ctxs, ObjectPool& pool,
                                       RuntimeState* runtime_state) {
    std::vector<TExpr> t_conjuncts;
    std::vector<TExprNode> nodes;

    // create "is_null_pred" FunctionCall
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(1);
    node.__set_has_nullable_child(true);

    {
        // FunctionCall's type
        TScalarType booleanType;
        booleanType.__set_type(TPrimitiveType::BOOLEAN);
        TTypeNode typeNode;
        typeNode.__set_scalar_type(booleanType);
        TTypeDesc typeDesc;
        typeDesc.__set_types({typeNode});
        node.__set_type(typeDesc);
    }

    {
        // create "is_null_pred" Function
        TFunctionName name;
        name.__set_function_name("is_null_pred");
        TFunction func;
        func.__set_name(name);
        func.__set_binary_type(TFunctionBinaryType::BUILTIN);

        {
            // Function arg type
            TScalarType argType;
            argType.__set_type(TPrimitiveType::INVALID_TYPE);
            TTypeNode typeNode;
            typeNode.__set_scalar_type(argType);
            TTypeDesc typeDesc;
            typeDesc.__set_types({typeNode});
            func.__set_arg_types({typeDesc});
        }

        {
            // Function ret type
            TScalarType booleanType;
            booleanType.__set_type(TPrimitiveType::BOOLEAN);
            TTypeNode typeNode;
            typeNode.__set_scalar_type(booleanType);
            TTypeDesc typeDesc;
            typeDesc.__set_types({typeNode});
            func.__set_ret_type(typeDesc);
        }
        func.__set_id(0);
        func.__set_fid(0);
        node.__set_fn(func);
    }
    nodes.emplace_back(node);

    // create "is_null_pred" FunctionCall's child
    TExprNode child;
    child.node_type = TExprNodeType::SLOT_REF;
    child.type = gen_type_desc(TPrimitiveType::INT);
    child.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    child.__set_slot_ref(t_slot_ref);
    child.is_nullable = true;
    nodes.emplace_back(child);

    TExpr t_expr;
    t_expr.nodes = nodes;
    t_conjuncts.emplace_back(t_expr);

    ASSERT_OK(Expr::create_expr_trees(&pool, t_conjuncts, conjunct_ctxs, nullptr));
    ASSERT_OK(Expr::prepare(*conjunct_ctxs, runtime_state));
    ASSERT_OK(Expr::open(*conjunct_ctxs, runtime_state));
}

TEST_F(IcebergSchemaEvolutionTest, TestAddColumn) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(7);
    field_col.__set_name("new_column");

    TIcebergSchemaField field_new_conjunct{};
    field_col.__set_field_id(8);
    field_col.__set_name("new_conjunct");

    std::vector<TIcebergSchemaField> fields{field_id, field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    TypeDescriptor new_conjunct = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);

    Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {"new_conjunct", new_conjunct}, {""}};

    {
        Utils::SlotDesc min_max_slots[] = {
                {"new_conjunct", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT), 2},
                {""},
        };
        ctx->min_max_tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, min_max_slots);

        // create min max conjuncts
        // new_conjunct is null
        _create_null_conjunct_ctxs(2, &ctx->min_max_conjunct_ctxs, _pool, _runtime_state);
    }

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(new_conjunct, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, NULL, NULL]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestDropColumn) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));
    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    std::vector<TIcebergSchemaField> fields{field_id};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    Utils::SlotDesc slot_descs[] = {{"id", id}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestRenameColumn) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("rename_id");

    std::vector<TIcebergSchemaField> fields{field_id};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor rename_id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    Utils::SlotDesc slot_descs[] = {{"rename_id", rename_id}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(rename_id, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestReorderColumn) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    std::vector<TIcebergSchemaField> subfields{field_col_a};
    field_col.__set_children(subfields);

    TIcebergSchemaField field_id{};
    field_id.__set_field_id(1);
    field_id.__set_name("id");

    std::vector<TIcebergSchemaField> fields{field_col, field_id};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
    col.field_names.emplace_back("a");

    TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    Utils::SlotDesc slot_descs[] = {{"col", col}, {"id", id}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[{a:2}, 1]", chunk->debug_row(0));
}

TEST_F(IcebergSchemaEvolutionTest, TestWidenColumnType) {
    auto file = _create_file(add_struct_subfield_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(add_struct_subfield_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    TIcebergSchemaField field_col{};
    field_col.__set_field_id(2);
    field_col.__set_name("col");

    TIcebergSchemaField field_col_a{};
    field_col_a.__set_field_id(3);
    field_col_a.__set_name("a");

    std::vector<TIcebergSchemaField> subfields{field_col_a};
    field_col.__set_children(subfields);

    std::vector<TIcebergSchemaField> fields{field_col};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

    col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    col.field_names.emplace_back("a");

    Utils::SlotDesc slot_descs[] = {{"col", col}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[{a:2}]", chunk->debug_row(0));
}

// Test iceberg table's parquet file don't have field id
TEST_F(IcebergSchemaEvolutionTest, TestWithoutFieldId) {
    auto file = _create_file(no_field_id_file_path);
    auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                    std::filesystem::file_size(no_field_id_file_path));

    // --------------init context---------------
    auto ctx = _create_scan_context();
    TIcebergSchema schema = TIcebergSchema{};

    // Should read as null
    TIcebergSchemaField field_c1{};
    field_c1.__set_field_id(1);
    field_c1.__set_name("rename_c1");

    // Can read values
    TIcebergSchemaField field_c2{};
    field_c2.__set_field_id(2);
    field_c2.__set_name("c2");

    // Should read as null
    TIcebergSchemaField field_c3{};
    field_c3.__set_field_id(3);
    field_c3.__set_name("rename_c3");

    std::vector<TIcebergSchemaField> fields{field_c1, field_c2, field_c3};
    schema.__set_fields(fields);
    ctx->lake_schema = &schema;

    TypeDescriptor rename_c1 = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
    TypeDescriptor c2 = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
    TypeDescriptor rename_c3 = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);

    Utils::SlotDesc slot_descs[] = {{"rename_c1", rename_c1}, {"c2", c2}, {"rename_c3", rename_c3}, {""}};

    TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
    Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
    ctx->slot_descs = tuple_desc->slots();
    ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
    // --------------finish init context---------------

    Status status = file_reader->init(ctx);
    if (!status.ok()) {
        std::cout << status.message() << std::endl;
    }
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(ColumnHelper::create_column(rename_c1, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(c2, true), chunk->num_columns());
    chunk->append_column(ColumnHelper::create_column(rename_c3, true), chunk->num_columns());

    status = file_reader->get_next(&chunk);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(11, chunk->num_rows());
    EXPECT_EQ("[NULL, 10, NULL]", chunk->debug_row(0));
}

// Test iceberg table's parquet file, struct without subfield
TEST_F(IcebergSchemaEvolutionTest, TestWithoutSubfield) {
    // has one valid column
    {
        auto file = _create_file(add_struct_subfield_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(add_struct_subfield_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TIcebergSchema schema = TIcebergSchema{};

        TIcebergSchemaField field_id{};
        field_id.__set_field_id(1);
        field_id.__set_name("id");

        TIcebergSchemaField field_col{};
        field_col.__set_field_id(2);
        field_col.__set_name("col");

        TIcebergSchemaField field_col_not_existed{};
        field_col_not_existed.__set_field_id(10);
        field_col_not_existed.__set_name("not_existed");

        std::vector<TIcebergSchemaField> subfields{field_col_not_existed};
        field_col.__set_children(subfields);

        std::vector<TIcebergSchemaField> fields{field_id, field_col};
        schema.__set_fields(fields);
        ctx->lake_schema = &schema;

        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

        col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        col.field_names.emplace_back("not_existed");

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    }

    // has no valid column
    {
        auto file = _create_file(add_struct_subfield_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(add_struct_subfield_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TIcebergSchema schema = TIcebergSchema{};

        TIcebergSchemaField field_col{};
        field_col.__set_field_id(2);
        field_col.__set_name("col");

        TIcebergSchemaField field_col_not_existed{};
        field_col_not_existed.__set_field_id(10);
        field_col_not_existed.__set_name("not_existed");

        std::vector<TIcebergSchemaField> subfields{field_col_not_existed};
        field_col.__set_children(subfields);

        std::vector<TIcebergSchemaField> fields{field_col};
        schema.__set_fields(fields);
        ctx->lake_schema = &schema;

        TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

        col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        col.field_names.emplace_back("not_existed");

        Utils::SlotDesc slot_descs[] = {{"col", col}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());
        EXPECT_EQ("[NULL]", chunk->debug_row(0));
    }
}

// Test hive table's parquet file, struct without subfield
TEST_F(IcebergSchemaEvolutionTest, TestHiveWithoutSubfield) {
    // with one valid column
    {
        auto file = _create_file(add_struct_subfield_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(add_struct_subfield_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();

        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

        col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        col.field_names.emplace_back("not_existed");

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col", col}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    }

    // without valid column
    {
        auto file = _create_file(add_struct_subfield_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(add_struct_subfield_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TypeDescriptor col = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);

        col.children.emplace_back(TypeDescriptor::from_logical_type(LogicalType::TYPE_INT));
        col.field_names.emplace_back("not_existed");

        Utils::SlotDesc slot_descs[] = {{"col", col}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(col, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[NULL]", chunk->debug_row(0));
    }
}

// Test inner struct without subfield
TEST_F(IcebergSchemaEvolutionTest, TestInnerStructWithoutSubfield) {
    // test with iceberg schema change
    {
        auto file = _create_file(struct_map_array_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(struct_map_array_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TIcebergSchema schema = TIcebergSchema{};

        TIcebergSchemaField field_id{};
        field_id.__set_field_id(1);
        field_id.__set_name("id");

        TIcebergSchemaField field_col{};
        field_col.__set_field_id(2);
        field_col.__set_name("col_struct");

        TIcebergSchemaField field_col_age{};
        field_col_age.__set_field_id(5);
        field_col_age.__set_name("age");

        TIcebergSchemaField field_col_detail{};
        field_col_detail.__set_field_id(6);
        field_col_detail.__set_name("detail");

        TIcebergSchemaField field_col_detail_not_existed{};
        field_col_detail_not_existed.__set_field_id(10);
        field_col_detail_not_existed.__set_name("not_existed");

        std::vector<TIcebergSchemaField> detail_subfields{field_col_detail_not_existed};
        field_col_detail.__set_children(detail_subfields);

        std::vector<TIcebergSchemaField> col_subfields{field_col_age, field_col_detail};
        field_col.__set_children(col_subfields);

        std::vector<TIcebergSchemaField> fields{field_id, field_col};
        schema.__set_fields(fields);
        ctx->lake_schema = &schema;

        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_struct.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_struct.field_names.emplace_back("age");

        TypeDescriptor col_detail = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_detail.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_detail.field_names.emplace_back("not_existed");

        col_struct.children.emplace_back(col_detail);
        col_struct.field_names.emplace_back("detail");

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col_struct", col_struct}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col_struct, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, {age:10,detail:NULL}]", chunk->debug_row(0));
    }
    {
        auto file = _create_file(struct_map_array_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(struct_map_array_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TIcebergSchema schema = TIcebergSchema{};

        TIcebergSchemaField field_id{};
        field_id.__set_field_id(1);
        field_id.__set_name("id");

        TIcebergSchemaField field_col_map{};
        field_col_map.__set_field_id(3);
        field_col_map.__set_name("col_map");

        TIcebergSchemaField field_col_map_key{};
        field_col_map_key.__set_field_id(9);
        field_col_map_key.__set_name("key");

        TIcebergSchemaField field_col_map_value{};
        field_col_map_value.__set_field_id(10);
        field_col_map_value.__set_name("value");

        TIcebergSchemaField field_col_map_value_not_existed{};
        field_col_map_value_not_existed.__set_field_id(13);
        field_col_map_value_not_existed.__set_name("not_existed");

        std::vector<TIcebergSchemaField> map_value_subfields{field_col_map_value_not_existed};
        field_col_map_value.__set_children(map_value_subfields);

        std::vector<TIcebergSchemaField> col_map_subfields{field_col_map_key, field_col_map_value};
        field_col_map.__set_children(col_map_subfields);

        std::vector<TIcebergSchemaField> fields{field_id, field_col_map};
        schema.__set_fields(fields);
        ctx->lake_schema = &schema;

        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col_map = TypeDescriptor::from_logical_type(LogicalType::TYPE_MAP);
        col_map.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));

        TypeDescriptor col_map_value = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_map_value.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_map_value.field_names.emplace_back("not_existed");

        col_map.children.emplace_back(col_map_value);

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col_map", col_map}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col_map, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, {1:NULL}]", chunk->debug_row(0));
    }

    // test with hive
    {
        auto file = _create_file(struct_map_array_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(struct_map_array_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_struct.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_struct.field_names.emplace_back("age");

        TypeDescriptor col_detail = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_detail.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_detail.field_names.emplace_back("not_existed");

        col_struct.children.emplace_back(col_detail);
        col_struct.field_names.emplace_back("detail");

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col_Struct", col_struct}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col_struct, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, {age:10,detail:NULL}]", chunk->debug_row(0));
    }
    {
        auto file = _create_file(struct_map_array_file_path);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(struct_map_array_file_path));

        // --------------init context---------------
        auto ctx = _create_scan_context();
        TypeDescriptor id = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

        TypeDescriptor col_array = TypeDescriptor::from_logical_type(LogicalType::TYPE_ARRAY);

        TypeDescriptor col_array_struct = TypeDescriptor::from_logical_type(LogicalType::TYPE_STRUCT);
        col_array_struct.children.emplace_back(TypeDescriptor::from_logical_type(TYPE_INT));
        col_array_struct.field_names.emplace_back("not_existed");

        col_array.children.emplace_back(col_array_struct);

        Utils::SlotDesc slot_descs[] = {{"id", id}, {"col_ARRAY", col_array}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(add_struct_subfield_file_path));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(id, true), chunk->num_columns());
        chunk->append_column(ColumnHelper::create_column(col_array, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(1, chunk->num_rows());

        EXPECT_EQ("[1, NULL]", chunk->debug_row(0));
    }
}

} // namespace starrocks::parquet
