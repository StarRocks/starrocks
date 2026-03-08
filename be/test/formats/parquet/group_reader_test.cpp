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

#include "formats/parquet/group_reader.h"

#include <base/testutil/assert.h>
#include <formats/parquet/scalar_column_reader.h>
#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "common/config_exec_fwd.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/utils.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "storage/column_expr_predicate.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks::parquet {

class MockInputStream : public io::SeekableInputStream {
public:
    StatusOr<int64_t> read(void* data, int64_t size) override { return size; }
    StatusOr<int64_t> position() override { return 0; }
    StatusOr<int64_t> get_size() override { return 0; }
    Status seek(int64_t offset) override { return Status::OK(); }
};

class MockColumnReader : public ColumnReader {
public:
    explicit MockColumnReader(tparquet::Type::type type) : ColumnReader(nullptr), _type(type) {}
    ~MockColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        size_t num_rows = static_cast<size_t>(range.span_size());
        if (_step > 1) {
            return Status::EndOfFile("");
        }
        size_t start = 0;
        if (_step == 0) {
            start = 0;
            num_rows = 8;
        } else if (_step == 1) {
            start = 8;
            num_rows = 4;
        }

        auto dst = dst_col->as_mutable_ptr();
        if (_type == tparquet::Type::type::INT32) {
            _append_int32_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::INT64) {
            _append_int64_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::INT96) {
            _append_int96_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::BYTE_ARRAY) {
            _append_binary_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::FLOAT) {
            _append_float_column(dst.get(), start, num_rows);
        } else if (_type == tparquet::Type::type::DOUBLE) {
            _append_double_column(dst.get(), start, num_rows);
        }

        _step++;
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override{};

    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

private:
    static void _append_int32_column(Column* column, size_t start, size_t num_rows) {
        for (int i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int32_t>(start));
        }
    }

    static void _append_int64_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int64_t>(start));
        }
    }

    static void _append_int96_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(i + static_cast<int64_t>(start));
        }
    }

    static void _append_binary_column(Column* column, size_t start, size_t num_rows) {
        for (size_t i = 0; i < num_rows; i++) {
            std::string str = std::string("str") + std::to_string(i + start);
            Slice slice;
            slice.data = str.data();
            slice.size = str.length();
            (*column).append_datum(slice);
        }
    }

    static void _append_float_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(0.5f * (i + start));
        }
    }

    static void _append_double_column(Column* column, size_t start, size_t num_rows) {
        for (int64_t i = 0; i < num_rows; i++) {
            (*column).append_datum(1.5 * (i + start));
        }
    }

    int _step = 0;
    tparquet::Type::type _type = tparquet::Type::type::INT32;
};

class MockPredicateErrorReader : public ColumnReader {
public:
    explicit MockPredicateErrorReader(const ParquetField* field) : ColumnReader(field) {}
    ~MockPredicateErrorReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        dst_col->as_mutable_ptr()->append_default(range.span_size());
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return Status::InternalError("mock row_group_zone_map_filter error");
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return Status::InternalError("mock page_index_zone_map_filter error");
    }
};

class GroupReaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

private:
    RandomAccessFile* _create_file();
    tparquet::ColumnChunk* _create_t_column_chunk(const std::string& file_path);
    tparquet::RowGroup* _create_t_row_group(GroupReaderParam* param);
    tparquet::FileMetaData* _create_t_filemeta(GroupReaderParam* param);
    tparquet::SchemaElement* _create_root_schema_element(GroupReaderParam* param);
    tparquet::SchemaElement* _create_schema_element(const std::string& col_name, tparquet::Type::type type);
    Status _create_filemeta(FileMetaData** file_meta, GroupReaderParam* param);
    GroupReaderParam* _create_group_reader_param();
    static ChunkPtr _create_chunk(GroupReaderParam* param);

    static void _check_int32_column(Column* column, size_t start, size_t count);
    static void _check_int64_column(Column* column, size_t start, size_t count);
    static void _check_int96_column(Column* column, size_t start, size_t count);
    static void _check_binary_column(Column* column, size_t start, size_t count);
    static void _check_float_column(Column* column, size_t start, size_t count);
    static void _check_double_column(Column* column, size_t start, size_t count);
    static void _check_chunk(GroupReaderParam* param, const ChunkPtr& chunk, size_t start, size_t count);

    ObjectPool _pool;
};

static StatusOr<const ColumnPredicate*> create_struct_subfield_eq_predicate(ObjectPool* pool, RuntimeState* state,
                                                                            ColumnId column_id, SlotId slot_id,
                                                                            const std::vector<std::string>& subfield,
                                                                            const std::string& value) {
    std::vector<TExprNode> nodes;

    TExprNode node0 = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, TExprOpcode::EQ);
    node0.__set_is_monotonic(true);
    nodes.emplace_back(node0);

    TExprNode node1;
    node1.node_type = TExprNodeType::SUBFIELD_EXPR;
    node1.is_nullable = true;
    node1.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node1.num_children = 1;
    node1.used_subfield_names = subfield;
    node1.__isset.used_subfield_names = true;
    node1.__set_is_monotonic(true);
    nodes.emplace_back(node1);

    TExprNode node2;
    node2.node_type = TExprNodeType::SLOT_REF;
    node2.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node2.num_children = 0;
    TSlotRef t_slot_ref;
    t_slot_ref.slot_id = slot_id;
    t_slot_ref.tuple_id = 0;
    node2.__set_slot_ref(t_slot_ref);
    node2.is_nullable = true;
    node2.__set_is_monotonic(true);
    nodes.emplace_back(node2);

    TExprNode node3;
    node3.node_type = TExprNodeType::STRING_LITERAL;
    node3.type = gen_type_desc(TPrimitiveType::VARCHAR);
    node3.num_children = 0;
    TStringLiteral string_literal;
    string_literal.value = value;
    node3.__set_string_literal(string_literal);
    node3.is_nullable = false;
    node3.__set_is_monotonic(true);
    nodes.emplace_back(node3);

    TExpr t_expr;
    t_expr.nodes = std::move(nodes);
    std::vector<TExpr> t_conjuncts{t_expr};
    std::vector<ExprContext*> conjunct_ctxs;
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(pool, t_conjuncts, &conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(ExprExecutor::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(conjunct_ctxs, state));
    ASSIGN_OR_RETURN(auto pred,
                     ColumnExprPredicate::make_column_expr_predicate(get_type_info(LogicalType::TYPE_VARCHAR),
                                                                     column_id, state, conjunct_ctxs[0], nullptr));
    return pred;
}

ChunkPtr GroupReaderTest::_create_chunk(GroupReaderParam* param) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto& column : param->read_cols) {
        auto c = ColumnHelper::create_column(column.slot_type(), true);
        chunk->append_column(std::move(c), column.slot_id());
    }
    return chunk;
}

void GroupReaderTest::_check_int32_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int32(), static_cast<int32_t>(start + i));
    }
}

void GroupReaderTest::_check_int64_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);

    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int64(), static_cast<int64_t>(start + i));
    }
}

void GroupReaderTest::_check_int96_column(Column* column, size_t start, size_t count) {
    ASSERT_EQ(column->size(), count);

    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(column->get(i).get_int64(), static_cast<int64_t>(start + i));
    }
}

void GroupReaderTest::_check_binary_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto check_slice = column->get(i).get_slice();
        std::string str = std::string("str") + std::to_string(i + start);
        Slice slice;
        slice.data = str.data();
        slice.size = str.length();
        ASSERT_TRUE(slice == check_slice);
    }
}

void GroupReaderTest::_check_float_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        float value = column->get(i).get_float();
        float exp = 0.5f * (start + i);
        ASSERT_FLOAT_EQ(exp, value);
    }
}

void GroupReaderTest::_check_double_column(Column* column, size_t start, size_t count) {
    for (size_t i = 0; i < count; i++) {
        double value = column->get(i).get_double();
        double exp = 1.5 * (start + i);
        ASSERT_DOUBLE_EQ(exp, value);
    }
}

void GroupReaderTest::_check_chunk(GroupReaderParam* param, const ChunkPtr& chunk, size_t start, size_t count) {
    ASSERT_EQ(param->read_cols.size(), chunk->num_columns());
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto column = chunk->mutable_columns()[i].get();
        auto _type = param->read_cols[i].type_in_parquet;
        size_t num_rows = count;

        if (_type == tparquet::Type::type::INT32) {
            _check_int32_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT64) {
            _check_int64_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT96) {
            _check_int96_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::BYTE_ARRAY) {
            _check_binary_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::FLOAT) {
            _check_float_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::DOUBLE) {
            _check_double_column(column, start, num_rows);
        }
    }
}

RandomAccessFile* GroupReaderTest::_create_file() {
    return _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock-random-access-file"));
}

tparquet::ColumnChunk* GroupReaderTest::_create_t_column_chunk(const std::string& file_path) {
    auto* column = _pool.add(new tparquet::ColumnChunk());

    column->__set_file_path(file_path);
    column->file_offset = 0;
    column->meta_data.data_page_offset = 4;

    return column;
}

tparquet::RowGroup* GroupReaderTest::_create_t_row_group(GroupReaderParam* param) {
    // create column chunks
    std::vector<tparquet::ColumnChunk> cols;
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto* col = _create_t_column_chunk("c" + std::to_string(i));
        cols.emplace_back(*col);
    }

    // create row group
    auto* row_group = _pool.add(new tparquet::RowGroup());
    row_group->__set_columns(cols);
    row_group->__set_num_rows(12);

    return row_group;
}

tparquet::SchemaElement* GroupReaderTest::_create_root_schema_element(GroupReaderParam* param) {
    auto* element = _pool.add(new tparquet::SchemaElement());

    element->__set_num_children(param->read_cols.size());
    return element;
}

tparquet::SchemaElement* GroupReaderTest::_create_schema_element(const std::string& col_name,
                                                                 tparquet::Type::type type) {
    auto* element = _pool.add(new tparquet::SchemaElement());

    element->__set_type(type);
    element->__set_name(col_name);
    element->__set_num_children(0);

    return element;
}

tparquet::FileMetaData* GroupReaderTest::_create_t_filemeta(GroupReaderParam* param) {
    auto* file_meta = _pool.add(new tparquet::FileMetaData());

    // row group
    std::vector<tparquet::RowGroup> row_groups;
    for (size_t i = 0; i < 2; i++) {
        row_groups.emplace_back(*_create_t_row_group(param));
    }

    // schema elements
    std::vector<tparquet::SchemaElement> schema_elements;
    schema_elements.emplace_back(*_create_root_schema_element(param));
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        std::string name = "c" + std::to_string(i);
        auto type = param->read_cols[i].type_in_parquet;
        schema_elements.emplace_back(*_create_schema_element(name, type));
    }

    // create file meta
    file_meta->__set_version(0);
    file_meta->__set_row_groups(row_groups);
    file_meta->__set_schema(schema_elements);

    return file_meta;
}

Status GroupReaderTest::_create_filemeta(FileMetaData** file_meta, GroupReaderParam* param) {
    auto* t_file_meta = _create_t_filemeta(param);

    *file_meta = _pool.add(new FileMetaData());
    return (*file_meta)->init(*t_file_meta, true);
}

static GroupReaderParam::Column _create_group_reader_param_of_column(ObjectPool* pool, int idx,
                                                                     tparquet::Type::type par_type,
                                                                     LogicalType prim_type) {
    SlotDescriptor* slot =
            pool->add(new SlotDescriptor(idx, fmt::format("col{}", idx), TypeDescriptor::from_logical_type(prim_type)));
    GroupReaderParam::Column c;
    c.idx_in_parquet = idx;
    c.type_in_parquet = par_type;
    c.slot_desc = slot;
    return c;
}

static HdfsScanStats g_hdfs_scan_stats;
GroupReaderParam* GroupReaderTest::_create_group_reader_param() {
    GroupReaderParam::Column c1 =
            _create_group_reader_param_of_column(&_pool, 0, tparquet::Type::type::INT32, LogicalType::TYPE_INT);
    GroupReaderParam::Column c2 =
            _create_group_reader_param_of_column(&_pool, 1, tparquet::Type::type::INT64, LogicalType::TYPE_BIGINT);
    GroupReaderParam::Column c3 = _create_group_reader_param_of_column(&_pool, 2, tparquet::Type::type::BYTE_ARRAY,
                                                                       LogicalType::TYPE_VARCHAR);
    GroupReaderParam::Column c4 =
            _create_group_reader_param_of_column(&_pool, 3, tparquet::Type::type::INT96, LogicalType::TYPE_DATETIME);
    GroupReaderParam::Column c5 =
            _create_group_reader_param_of_column(&_pool, 4, tparquet::Type::type::FLOAT, LogicalType::TYPE_FLOAT);
    GroupReaderParam::Column c6 =
            _create_group_reader_param_of_column(&_pool, 5, tparquet::Type::type::DOUBLE, LogicalType::TYPE_DOUBLE);

    auto* param = _pool.add(new GroupReaderParam());
    param->read_cols.emplace_back(c1);
    param->read_cols.emplace_back(c2);
    param->read_cols.emplace_back(c3);
    param->read_cols.emplace_back(c4);
    param->read_cols.emplace_back(c5);
    param->read_cols.emplace_back(c6);
    param->stats = &g_hdfs_scan_stats;
    return param;
}

TEST_F(GroupReaderTest, TestInit) {
    // create file
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    // create file meta
    FileMetaData* file_meta;
    Status status = _create_filemeta(&file_meta, param);
    ASSERT_TRUE(status.ok());

    // create row group reader
    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;
    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // init row group reader
    status = group_reader->init();
    ASSERT_TRUE(status.ok());
    status = group_reader->prepare();
    // timezone is empty
    ASSERT_FALSE(status.ok());
    //ASSERT_TRUE(status.is_end_of_file());
}

static void replace_column_readers(GroupReader* group_reader, GroupReaderParam* param) {
    group_reader->_column_readers.clear();
    group_reader->_active_column_indices.clear();
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto r = std::make_unique<MockColumnReader>(param->read_cols[i].type_in_parquet);
        group_reader->_column_readers[i] = std::move(r);
        group_reader->_active_column_indices.push_back(i);
    }
}

static void prepare_row_range(GroupReader* group_reader) {
    group_reader->_range =
            SparseRange<uint64_t>(group_reader->_row_group_first_row,
                                  group_reader->_row_group_first_row + group_reader->_row_group_metadata->num_rows);
    group_reader->_range_iter = group_reader->_range.new_iterator();
}

TEST_F(GroupReaderTest, TestGetNext) {
    // create file
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    // create file meta
    FileMetaData* file_meta;
    Status status = _create_filemeta(&file_meta, param);
    ASSERT_TRUE(status.ok());

    // create row group reader
    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;
    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // init row group reader
    status = group_reader->init();
    ASSERT_TRUE(status.ok());
    status = group_reader->prepare();
    ASSERT_FALSE(status.ok());

    // replace column readers
    replace_column_readers(group_reader, param);
    // create chunk
    group_reader->_read_chunk = _create_chunk(param);

    auto chunk = _create_chunk(param);

    prepare_row_range(group_reader);
    // get next
    size_t row_count = 8;
    status = group_reader->get_next(&chunk, &row_count);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(row_count, 8);
    _check_chunk(param, chunk, 0, 8);

    // reset and get next.
    chunk = _create_chunk(param);
    row_count = 8;
    status = group_reader->get_next(&chunk, &row_count);
    ASSERT_TRUE(status.is_end_of_file());
    ASSERT_EQ(row_count, 4);
    _check_chunk(param, chunk, 8, 4);
}

TEST_F(GroupReaderTest, ColumnReaderCreateTypeMismatch) {
    ParquetField field;
    field.name = "col0";
    field.type = ColumnType::ARRAY;

    TypeDescriptor col_type;
    col_type.type = LogicalType::TYPE_VARCHAR;

    ColumnReaderOptions options;
    auto st = ColumnReaderFactory::create(options, &field, col_type, nullptr);
    ASSERT_FALSE(st.ok()) << st;
    std::cout << st.status().message() << "\n";
}

TEST_F(GroupReaderTest, VariantColumnReader) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    // Create metadata and value children for variant
    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    field.children.push_back(metadata_field);
    field.children.push_back(value_field);

    TypeDescriptor col_type;
    col_type.type = LogicalType::TYPE_VARIANT;

    // Create minimal row group metadata with column chunks
    tparquet::ColumnChunk metadata_chunk;
    metadata_chunk.__set_file_path("metadata");
    metadata_chunk.file_offset = 0;
    metadata_chunk.meta_data.data_page_offset = 4;

    tparquet::ColumnChunk value_chunk;
    value_chunk.__set_file_path("value");
    value_chunk.file_offset = 0;
    value_chunk.meta_data.data_page_offset = 4;

    tparquet::RowGroup row_group;
    row_group.columns.push_back(metadata_chunk);
    row_group.columns.push_back(value_chunk);
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;
    TIcebergSchemaField lake_schema_field;
    lake_schema_field.name = "col_variant";
    lake_schema_field.field_id = 1;
    auto st = ColumnReaderFactory::create(options, &field, col_type, &lake_schema_field);
    ASSERT_TRUE(st.ok()) << st.status().message();
}

TEST_F(GroupReaderTest, VariantColumnReaderWithTypedShreddedFields) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // typed_value.age.{value,typed_value}
    ParquetField age_value_field;
    age_value_field.name = "value";
    age_value_field.type = ColumnType::SCALAR;
    age_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    age_value_field.physical_column_index = 2;

    ParquetField age_typed_field;
    age_typed_field.name = "typed_value";
    age_typed_field.type = ColumnType::SCALAR;
    age_typed_field.physical_type = tparquet::Type::INT32;
    age_typed_field.physical_column_index = 3;
    {
        tparquet::IntType int_type;
        int_type.bitWidth = 32;
        int_type.isSigned = false;
        tparquet::LogicalType logical_type;
        logical_type.__set_INTEGER(int_type);
        age_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField age_node;
    age_node.name = "age";
    age_node.type = ColumnType::STRUCT;
    age_node.children = {age_value_field, age_typed_field};

    // typed_value.created.{value,typed_value}
    ParquetField created_value_field;
    created_value_field.name = "value";
    created_value_field.type = ColumnType::SCALAR;
    created_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    created_value_field.physical_column_index = 4;

    ParquetField created_typed_field;
    created_typed_field.name = "typed_value";
    created_typed_field.type = ColumnType::SCALAR;
    created_typed_field.physical_type = tparquet::Type::INT32;
    created_typed_field.physical_column_index = 5;
    created_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::DATE);

    ParquetField created_node;
    created_node.name = "created";
    created_node.type = ColumnType::STRUCT;
    created_node.children = {created_value_field, created_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {age_node, created_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 5; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithRootTypedValue) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // Root typed_value as SCALAR (non-STRUCT). This shape is valid for shredded
    // variant rows whose payload is stored directly in typed_value.
    ParquetField typed_value_scalar;
    typed_value_scalar.name = "typed_value";
    typed_value_scalar.type = ColumnType::SCALAR;
    typed_value_scalar.physical_type = tparquet::Type::INT64;
    typed_value_scalar.physical_column_index = 2;

    field.children = {metadata_field, value_field, typed_value_scalar};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 2; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, FixedValueColumnReaderTest) {
    auto col1 = std::make_unique<FixedValueColumnReader>(kNullDatum);
    ASSERT_OK(col1->prepare());
    col1->get_levels(nullptr, nullptr, nullptr);
    col1->set_need_parse_levels(false);
    col1->collect_column_io_range(nullptr, nullptr, ColumnIOType::PAGES, true);
    SparseRange<uint64_t> sparse_range;
    col1->select_offset_index(sparse_range, 100);
    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(100), true);
    Range<uint64_t> range(0, 100);
    ASSERT_TRUE(col1->read_range(range, nullptr, column).ok());

    TypeInfoPtr type_info = get_type_info(LogicalType::TYPE_INT);
    ColumnPredicate* is_null_predicate = _pool.add(new_column_null_predicate(type_info, 1, true));
    ColumnPredicate* is_not_null_predicate = _pool.add(new_column_null_predicate(type_info, 1, false));

    std::vector<const ColumnPredicate*> predicates;
    predicates.push_back(is_null_predicate);
    predicates.push_back(is_not_null_predicate);

    ASSERT_TRUE(col1->row_group_zone_map_filter(predicates, CompoundNodeType::AND, 1, 100).value());
    ASSERT_FALSE(col1->row_group_zone_map_filter(predicates, CompoundNodeType::OR, 1, 100).value());
}

TEST_F(GroupReaderTest, ParquetUtilsGetNonNullDataColumnAndRowConstNullable) {
    auto data = Int32Column::create();
    data->append(123);
    auto nulls = NullColumn::create();
    nulls->append(0);
    auto nullable = NullableColumn::create(std::move(data), std::move(nulls));
    auto const_nullable = ConstColumn::create(nullable, 4);

    const Column* out_column = nullptr;
    size_t out_row = 999;
    ASSERT_TRUE(ParquetUtils::get_non_null_data_column_and_row(const_nullable.get(), 3, &out_column, &out_row));
    ASSERT_NE(out_column, nullptr);
    ASSERT_TRUE(out_column->is_numeric());
    ASSERT_EQ(0, out_row);

    const Column* null_out_column = nullptr;
    size_t null_out_row = 0;
    auto null_data = Int32Column::create();
    null_data->append(7);
    auto all_nulls = NullColumn::create();
    all_nulls->append(1);
    auto all_null_nullable = NullableColumn::create(std::move(null_data), std::move(all_nulls));
    auto const_all_null = ConstColumn::create(all_null_nullable, 2);
    ASSERT_FALSE(
            ParquetUtils::get_non_null_data_column_and_row(const_all_null.get(), 1, &null_out_column, &null_out_row));
}

TEST_F(GroupReaderTest, ParquetUtilsHasNonNullValueConstNullable) {
    auto data = Int32Column::create();
    data->append(10);
    auto nulls = NullColumn::create();
    nulls->append(0);
    auto nullable = NullableColumn::create(std::move(data), std::move(nulls));
    auto const_nullable = ConstColumn::create(nullable, 8);
    ASSERT_TRUE(ParquetUtils::has_non_null_value(const_nullable.get(), 8));

    auto null_data = Int32Column::create();
    null_data->append(10);
    auto all_nulls = NullColumn::create();
    all_nulls->append(1);
    auto all_null_nullable = NullableColumn::create(std::move(null_data), std::move(all_nulls));
    auto const_all_null = ConstColumn::create(all_null_nullable, 8);
    ASSERT_FALSE(ParquetUtils::has_non_null_value(const_all_null.get(), 8));
}

TEST_F(GroupReaderTest, ParquetUtilsHasNonNullBinaryValueBranches) {
    auto str_data = BinaryColumn::create();
    str_data->append(Slice("abc"));
    auto str_nulls = NullColumn::create();
    str_nulls->append(0);
    auto str_nullable = NullableColumn::create(std::move(str_data), std::move(str_nulls));
    auto const_binary = ConstColumn::create(str_nullable, 3);
    ASSERT_TRUE(ParquetUtils::has_non_null_binary_value(const_binary.get(), 3));

    auto null_str_data = BinaryColumn::create();
    null_str_data->append(Slice("abc"));
    auto null_str_nulls = NullColumn::create();
    null_str_nulls->append(1);
    auto null_str_nullable = NullableColumn::create(std::move(null_str_data), std::move(null_str_nulls));
    auto const_null_binary = ConstColumn::create(null_str_nullable, 3);
    ASSERT_FALSE(ParquetUtils::has_non_null_binary_value(const_null_binary.get(), 3));

    auto int_data = Int32Column::create();
    int_data->append(1);
    auto int_nulls = NullColumn::create();
    int_nulls->append(0);
    auto int_nullable = NullableColumn::create(std::move(int_data), std::move(int_nulls));
    auto const_non_binary = ConstColumn::create(int_nullable, 3);
    ASSERT_FALSE(ParquetUtils::has_non_null_binary_value(const_non_binary.get(), 3));

    auto plain_binary = BinaryColumn::create();
    plain_binary->append(Slice("v"));
    ASSERT_TRUE(ParquetUtils::has_non_null_binary_value(plain_binary.get(), 1));
}

TEST_F(GroupReaderTest, VariantColumnReaderWithScalarTypes) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // BOOLEAN field
    ParquetField bool_value_field;
    bool_value_field.name = "value";
    bool_value_field.type = ColumnType::SCALAR;
    bool_value_field.physical_type = tparquet::Type::BOOLEAN;
    bool_value_field.physical_column_index = 2;

    ParquetField bool_typed_field;
    bool_typed_field.name = "typed_value";
    bool_typed_field.type = ColumnType::SCALAR;
    bool_typed_field.physical_type = tparquet::Type::BOOLEAN;
    bool_typed_field.physical_column_index = 3;

    ParquetField bool_node;
    bool_node.name = "isActive";
    bool_node.type = ColumnType::STRUCT;
    bool_node.children = {bool_value_field, bool_typed_field};

    // INT64 field
    ParquetField int64_value_field;
    int64_value_field.name = "value";
    int64_value_field.type = ColumnType::SCALAR;
    int64_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int64_value_field.physical_column_index = 4;

    ParquetField int64_typed_field;
    int64_typed_field.name = "typed_value";
    int64_typed_field.type = ColumnType::SCALAR;
    int64_typed_field.physical_type = tparquet::Type::INT64;
    int64_typed_field.physical_column_index = 5;

    ParquetField int64_node;
    int64_node.name = "count";
    int64_node.type = ColumnType::STRUCT;
    int64_node.children = {int64_value_field, int64_typed_field};

    // FLOAT field
    ParquetField float_value_field;
    float_value_field.name = "value";
    float_value_field.type = ColumnType::SCALAR;
    float_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    float_value_field.physical_column_index = 6;

    ParquetField float_typed_field;
    float_typed_field.name = "typed_value";
    float_typed_field.type = ColumnType::SCALAR;
    float_typed_field.physical_type = tparquet::Type::FLOAT;
    float_typed_field.physical_column_index = 7;

    ParquetField float_node;
    float_node.name = "score";
    float_node.type = ColumnType::STRUCT;
    float_node.children = {float_value_field, float_typed_field};

    // DOUBLE field
    ParquetField double_value_field;
    double_value_field.name = "value";
    double_value_field.type = ColumnType::SCALAR;
    double_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    double_value_field.physical_column_index = 8;

    ParquetField double_typed_field;
    double_typed_field.name = "typed_value";
    double_typed_field.type = ColumnType::SCALAR;
    double_typed_field.physical_type = tparquet::Type::DOUBLE;
    double_typed_field.physical_column_index = 9;

    ParquetField double_node;
    double_node.name = "amount";
    double_node.type = ColumnType::STRUCT;
    double_node.children = {double_value_field, double_typed_field};

    // INT64 with logicalType (TIMESTAMP)
    ParquetField ts_value_field;
    ts_value_field.name = "value";
    ts_value_field.type = ColumnType::SCALAR;
    ts_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    ts_value_field.physical_column_index = 10;

    ParquetField ts_typed_field;
    ts_typed_field.name = "typed_value";
    ts_typed_field.type = ColumnType::SCALAR;
    ts_typed_field.physical_type = tparquet::Type::INT64;
    ts_typed_field.physical_column_index = 11;
    ts_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::TIMESTAMP_MICROS);

    ParquetField ts_node;
    ts_node.name = "created";
    ts_node.type = ColumnType::STRUCT;
    ts_node.children = {ts_value_field, ts_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {bool_node, int64_node, float_node, double_node, ts_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 11; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithLogicalTypes) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // STRING using logicalType
    ParquetField str_value_field;
    str_value_field.name = "value";
    str_value_field.type = ColumnType::SCALAR;
    str_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    str_value_field.physical_column_index = 2;

    ParquetField str_typed_field;
    str_typed_field.name = "typed_value";
    str_typed_field.type = ColumnType::SCALAR;
    str_typed_field.physical_type = tparquet::Type::BYTE_ARRAY;
    str_typed_field.physical_column_index = 3;
    {
        tparquet::StringType str_type;
        tparquet::LogicalType logical_type;
        logical_type.__set_STRING(str_type);
        str_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField str_node;
    str_node.name = "name";
    str_node.type = ColumnType::STRUCT;
    str_node.children = {str_value_field, str_typed_field};

    // TIME using logicalType
    ParquetField time_value_field;
    time_value_field.name = "value";
    time_value_field.type = ColumnType::SCALAR;
    time_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    time_value_field.physical_column_index = 4;

    ParquetField time_typed_field;
    time_typed_field.name = "typed_value";
    time_typed_field.type = ColumnType::SCALAR;
    time_typed_field.physical_type = tparquet::Type::INT64;
    time_typed_field.physical_column_index = 5;
    {
        tparquet::TimeType time_type;
        time_type.unit.__set_MICROS(tparquet::MicroSeconds());
        time_type.isAdjustedToUTC = false;
        tparquet::LogicalType logical_type;
        logical_type.__set_TIME(time_type);
        time_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField time_node;
    time_node.name = "timestamp";
    time_node.type = ColumnType::STRUCT;
    time_node.children = {time_value_field, time_typed_field};

    // INT8 using INTEGER logicalType
    ParquetField int8_value_field;
    int8_value_field.name = "value";
    int8_value_field.type = ColumnType::SCALAR;
    int8_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int8_value_field.physical_column_index = 6;

    ParquetField int8_typed_field;
    int8_typed_field.name = "typed_value";
    int8_typed_field.type = ColumnType::SCALAR;
    int8_typed_field.physical_type = tparquet::Type::INT32;
    int8_typed_field.physical_column_index = 7;
    {
        tparquet::IntType int_type;
        int_type.bitWidth = 8;
        int_type.isSigned = true;
        tparquet::LogicalType logical_type;
        logical_type.__set_INTEGER(int_type);
        int8_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField int8_node;
    int8_node.name = "byte_val";
    int8_node.type = ColumnType::STRUCT;
    int8_node.children = {int8_value_field, int8_typed_field};

    // INT16 using INTEGER logicalType
    ParquetField int16_value_field;
    int16_value_field.name = "value";
    int16_value_field.type = ColumnType::SCALAR;
    int16_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int16_value_field.physical_column_index = 8;

    ParquetField int16_typed_field;
    int16_typed_field.name = "typed_value";
    int16_typed_field.type = ColumnType::SCALAR;
    int16_typed_field.physical_type = tparquet::Type::INT32;
    int16_typed_field.physical_column_index = 9;
    {
        tparquet::IntType int_type;
        int_type.bitWidth = 16;
        int_type.isSigned = true;
        tparquet::LogicalType logical_type;
        logical_type.__set_INTEGER(int_type);
        int16_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField int16_node;
    int16_node.name = "short_val";
    int16_node.type = ColumnType::STRUCT;
    int16_node.children = {int16_value_field, int16_typed_field};

    // UINT32 using INTEGER logicalType
    ParquetField uint32_value_field;
    uint32_value_field.name = "value";
    uint32_value_field.type = ColumnType::SCALAR;
    uint32_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    uint32_value_field.physical_column_index = 10;

    ParquetField uint32_typed_field;
    uint32_typed_field.name = "typed_value";
    uint32_typed_field.type = ColumnType::SCALAR;
    uint32_typed_field.physical_type = tparquet::Type::INT64;
    uint32_typed_field.physical_column_index = 11;
    {
        tparquet::IntType int_type;
        int_type.bitWidth = 32;
        int_type.isSigned = false;
        tparquet::LogicalType logical_type;
        logical_type.__set_INTEGER(int_type);
        uint32_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField uint32_node;
    uint32_node.name = "u32";
    uint32_node.type = ColumnType::STRUCT;
    uint32_node.children = {uint32_value_field, uint32_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {str_node, time_node, int8_node, int16_node, uint32_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 11; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithConvertedTypes) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // INT_8 using converted_type
    ParquetField int8_value_field;
    int8_value_field.name = "value";
    int8_value_field.type = ColumnType::SCALAR;
    int8_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int8_value_field.physical_column_index = 2;

    ParquetField int8_typed_field;
    int8_typed_field.name = "typed_value";
    int8_typed_field.type = ColumnType::SCALAR;
    int8_typed_field.physical_type = tparquet::Type::INT32;
    int8_typed_field.physical_column_index = 3;
    int8_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::INT_8);

    ParquetField int8_node;
    int8_node.name = "tiny";
    int8_node.type = ColumnType::STRUCT;
    int8_node.children = {int8_value_field, int8_typed_field};

    // INT_16 using converted_type
    ParquetField int16_value_field;
    int16_value_field.name = "value";
    int16_value_field.type = ColumnType::SCALAR;
    int16_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int16_value_field.physical_column_index = 4;

    ParquetField int16_typed_field;
    int16_typed_field.name = "typed_value";
    int16_typed_field.type = ColumnType::SCALAR;
    int16_typed_field.physical_type = tparquet::Type::INT32;
    int16_typed_field.physical_column_index = 5;
    int16_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::INT_16);

    ParquetField int16_node;
    int16_node.name = "small";
    int16_node.type = ColumnType::STRUCT;
    int16_node.children = {int16_value_field, int16_typed_field};

    // INT_32 using converted_type
    ParquetField int32_value_field;
    int32_value_field.name = "value";
    int32_value_field.type = ColumnType::SCALAR;
    int32_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int32_value_field.physical_column_index = 6;

    ParquetField int32_typed_field;
    int32_typed_field.name = "typed_value";
    int32_typed_field.type = ColumnType::SCALAR;
    int32_typed_field.physical_type = tparquet::Type::INT32;
    int32_typed_field.physical_column_index = 7;
    int32_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::INT_32);

    ParquetField int32_node;
    int32_node.name = "int";
    int32_node.type = ColumnType::STRUCT;
    int32_node.children = {int32_value_field, int32_typed_field};

    // INT_64 using converted_type
    ParquetField int64_value_field;
    int64_value_field.name = "value";
    int64_value_field.type = ColumnType::SCALAR;
    int64_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    int64_value_field.physical_column_index = 8;

    ParquetField int64_typed_field;
    int64_typed_field.name = "typed_value";
    int64_typed_field.type = ColumnType::SCALAR;
    int64_typed_field.physical_type = tparquet::Type::INT64;
    int64_typed_field.physical_column_index = 9;
    int64_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::INT_64);

    ParquetField int64_node;
    int64_node.name = "big";
    int64_node.type = ColumnType::STRUCT;
    int64_node.children = {int64_value_field, int64_typed_field};

    // UINT_8 using converted_type (should widen to INT16)
    ParquetField uint8_value_field;
    uint8_value_field.name = "value";
    uint8_value_field.type = ColumnType::SCALAR;
    uint8_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    uint8_value_field.physical_column_index = 10;

    ParquetField uint8_typed_field;
    uint8_typed_field.name = "typed_value";
    uint8_typed_field.type = ColumnType::SCALAR;
    uint8_typed_field.physical_type = tparquet::Type::INT32;
    uint8_typed_field.physical_column_index = 11;
    uint8_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::UINT_8);

    ParquetField uint8_node;
    uint8_node.name = "utiny";
    uint8_node.type = ColumnType::STRUCT;
    uint8_node.children = {uint8_value_field, uint8_typed_field};

    // UINT_16 using converted_type (should widen to INT32)
    ParquetField uint16_value_field;
    uint16_value_field.name = "value";
    uint16_value_field.type = ColumnType::SCALAR;
    uint16_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    uint16_value_field.physical_column_index = 12;

    ParquetField uint16_typed_field;
    uint16_typed_field.name = "typed_value";
    uint16_typed_field.type = ColumnType::SCALAR;
    uint16_typed_field.physical_type = tparquet::Type::INT32;
    uint16_typed_field.physical_column_index = 13;
    uint16_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::UINT_16);

    ParquetField uint16_node;
    uint16_node.name = "usmall";
    uint16_node.type = ColumnType::STRUCT;
    uint16_node.children = {uint16_value_field, uint16_typed_field};

    // UINT_32 using converted_type (should widen to INT64)
    ParquetField uint32_value_field;
    uint32_value_field.name = "value";
    uint32_value_field.type = ColumnType::SCALAR;
    uint32_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    uint32_value_field.physical_column_index = 14;

    ParquetField uint32_typed_field;
    uint32_typed_field.name = "typed_value";
    uint32_typed_field.type = ColumnType::SCALAR;
    uint32_typed_field.physical_type = tparquet::Type::INT64;
    uint32_typed_field.physical_column_index = 15;
    uint32_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::UINT_32);

    ParquetField uint32_node;
    uint32_node.name = "u32";
    uint32_node.type = ColumnType::STRUCT;
    uint32_node.children = {uint32_value_field, uint32_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {int8_node, int16_node, int32_node, int64_node, uint8_node, uint16_node, uint32_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 15; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderFallbackOnly) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // Field with only fallback binary (no typed_value)
    ParquetField fallback_value_field;
    fallback_value_field.name = "value";
    fallback_value_field.type = ColumnType::SCALAR;
    fallback_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    fallback_value_field.physical_column_index = 2;

    ParquetField fallback_node;
    fallback_node.name = "fallback_field";
    fallback_node.type = ColumnType::STRUCT;
    fallback_node.children = {fallback_value_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {fallback_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 2; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithArrayShredding) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // Array element: list.element.value (binary fallback)
    ParquetField element_value_field;
    element_value_field.name = "value";
    element_value_field.type = ColumnType::SCALAR;
    element_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    element_value_field.physical_column_index = 2;

    // Array element: list.element.typed_value (int32)
    ParquetField element_typed_value_field;
    element_typed_value_field.name = "typed_value";
    element_typed_value_field.type = ColumnType::SCALAR;
    element_typed_value_field.physical_type = tparquet::Type::INT32;
    element_typed_value_field.physical_column_index = 3;

    // Array element struct: list.element {value, typed_value}
    ParquetField element_struct;
    element_struct.name = "element";
    element_struct.type = ColumnType::STRUCT;
    element_struct.children = {element_value_field, element_typed_value_field};

    // Array list: list {element}
    ParquetField list_struct;
    list_struct.name = "list";
    list_struct.type = ColumnType::STRUCT;
    list_struct.children = {element_struct};

    // typed_value.tags: ARRAY<INT32>
    ParquetField tags_value_field;
    tags_value_field.name = "value";
    tags_value_field.type = ColumnType::SCALAR;
    tags_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    tags_value_field.physical_column_index = 4;

    ParquetField tags_typed_field;
    tags_typed_field.name = "typed_value";
    tags_typed_field.type = ColumnType::ARRAY;
    tags_typed_field.children = {list_struct};

    ParquetField tags_node;
    tags_node.name = "tags";
    tags_node.type = ColumnType::STRUCT;
    tags_node.children = {tags_value_field, tags_typed_field};

    // typed_value.scores: ARRAY<DOUBLE>
    ParquetField score_elem_value;
    score_elem_value.name = "value";
    score_elem_value.type = ColumnType::SCALAR;
    score_elem_value.physical_type = tparquet::Type::BYTE_ARRAY;
    score_elem_value.physical_column_index = 5;

    ParquetField score_elem_typed;
    score_elem_typed.name = "typed_value";
    score_elem_typed.type = ColumnType::SCALAR;
    score_elem_typed.physical_type = tparquet::Type::DOUBLE;
    score_elem_typed.physical_column_index = 6;

    ParquetField score_elem_struct;
    score_elem_struct.name = "element";
    score_elem_struct.type = ColumnType::STRUCT;
    score_elem_struct.children = {score_elem_value, score_elem_typed};

    ParquetField score_list;
    score_list.name = "list";
    score_list.type = ColumnType::STRUCT;
    score_list.children = {score_elem_struct};

    ParquetField scores_value;
    scores_value.name = "value";
    scores_value.type = ColumnType::SCALAR;
    scores_value.physical_type = tparquet::Type::BYTE_ARRAY;
    scores_value.physical_column_index = 7;

    ParquetField scores_typed;
    scores_typed.name = "typed_value";
    scores_typed.type = ColumnType::ARRAY;
    scores_typed.children = {score_list};

    ParquetField scores_node;
    scores_node.name = "scores";
    scores_node.type = ColumnType::STRUCT;
    scores_node.children = {scores_value, scores_typed};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {tags_node, scores_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 7; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithDecimalTypes) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // DECIMAL with converted_type
    ParquetField dec_value_field;
    dec_value_field.name = "value";
    dec_value_field.type = ColumnType::SCALAR;
    dec_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    dec_value_field.physical_column_index = 2;

    ParquetField dec_typed_field;
    dec_typed_field.name = "typed_value";
    dec_typed_field.type = ColumnType::SCALAR;
    dec_typed_field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    dec_typed_field.precision = 10;
    dec_typed_field.scale = 2;
    dec_typed_field.physical_column_index = 3;
    dec_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::DECIMAL);

    ParquetField dec_node;
    dec_node.name = "price";
    dec_node.type = ColumnType::STRUCT;
    dec_node.children = {dec_value_field, dec_typed_field};

    // DECIMAL with logicalType
    ParquetField dec2_value_field;
    dec2_value_field.name = "value";
    dec2_value_field.type = ColumnType::SCALAR;
    dec2_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    dec2_value_field.physical_column_index = 4;

    ParquetField dec2_typed_field;
    dec2_typed_field.name = "typed_value";
    dec2_typed_field.type = ColumnType::SCALAR;
    dec2_typed_field.physical_type = tparquet::Type::INT64;
    dec2_typed_field.precision = 18;
    dec2_typed_field.scale = 4;
    dec2_typed_field.physical_column_index = 5;
    {
        tparquet::DecimalType decimal_type;
        decimal_type.scale = 4;
        decimal_type.precision = 18;
        tparquet::LogicalType logical_type;
        logical_type.__set_DECIMAL(decimal_type);
        dec2_typed_field.schema_element.__set_logicalType(logical_type);
    }

    ParquetField dec2_node;
    dec2_node.name = "amount";
    dec2_node.type = ColumnType::STRUCT;
    dec2_node.children = {dec2_value_field, dec2_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {dec_node, dec2_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 5; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, VariantColumnReaderWithTimeMillis) {
    ParquetField field;
    field.name = "col_variant";
    field.type = ColumnType::STRUCT;

    ParquetField metadata_field;
    metadata_field.name = "metadata";
    metadata_field.type = ColumnType::SCALAR;
    metadata_field.physical_type = tparquet::Type::BYTE_ARRAY;
    metadata_field.physical_column_index = 0;

    ParquetField value_field;
    value_field.name = "value";
    value_field.type = ColumnType::SCALAR;
    value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    value_field.physical_column_index = 1;

    // TIME_MILLIS using converted_type
    ParquetField time_value_field;
    time_value_field.name = "value";
    time_value_field.type = ColumnType::SCALAR;
    time_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    time_value_field.physical_column_index = 2;

    ParquetField time_typed_field;
    time_typed_field.name = "typed_value";
    time_typed_field.type = ColumnType::SCALAR;
    time_typed_field.physical_type = tparquet::Type::INT32;
    time_typed_field.physical_column_index = 3;
    time_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::TIME_MILLIS);

    ParquetField time_node;
    time_node.name = "time_millis";
    time_node.type = ColumnType::STRUCT;
    time_node.children = {time_value_field, time_typed_field};

    // TIMESTAMP_MILLIS using converted_type
    ParquetField ts_value_field;
    ts_value_field.name = "value";
    ts_value_field.type = ColumnType::SCALAR;
    ts_value_field.physical_type = tparquet::Type::BYTE_ARRAY;
    ts_value_field.physical_column_index = 4;

    ParquetField ts_typed_field;
    ts_typed_field.name = "typed_value";
    ts_typed_field.type = ColumnType::SCALAR;
    ts_typed_field.physical_type = tparquet::Type::INT64;
    ts_typed_field.physical_column_index = 5;
    ts_typed_field.schema_element.__set_converted_type(tparquet::ConvertedType::TIMESTAMP_MILLIS);

    ParquetField ts_node;
    ts_node.name = "ts_millis";
    ts_node.type = ColumnType::STRUCT;
    ts_node.children = {ts_value_field, ts_typed_field};

    ParquetField typed_value_field;
    typed_value_field.name = "typed_value";
    typed_value_field.type = ColumnType::STRUCT;
    typed_value_field.children = {time_node, ts_node};

    field.children = {metadata_field, value_field, typed_value_field};

    tparquet::RowGroup row_group;
    for (int i = 0; i <= 5; ++i) {
        tparquet::ColumnChunk chunk;
        chunk.__set_file_path("col" + std::to_string(i));
        chunk.file_offset = 0;
        chunk.meta_data.data_page_offset = 4;
        row_group.columns.emplace_back(std::move(chunk));
    }
    row_group.__set_num_rows(0);

    ColumnReaderOptions options;
    options.row_group_meta = &row_group;

    TypeDescriptor variant_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT);
    auto st = ColumnReaderFactory::create(options, &field, variant_type);
    ASSERT_TRUE(st.ok()) << st.status().message();
    ASSERT_NE(st.value(), nullptr);
}

TEST_F(GroupReaderTest, StructColumnReaderRowGroupZoneMapFilterChildError) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    ParquetField child_field;
    child_field.name = "a";
    child_field.type = ColumnType::SCALAR;
    child_field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.children.push_back(child_field);

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockPredicateErrorReader>(&field.children[0]));
    StructColumnReader reader(&field, std::move(child_readers));

    RuntimeState runtime_state{TQueryGlobals()};
    ASSIGN_OR_ABORT(auto* pred, create_struct_subfield_eq_predicate(&_pool, &runtime_state, 0, 0, {"a"}, "x"));
    _pool.add(const_cast<ColumnPredicate*>(pred));

    auto res = reader.row_group_zone_map_filter({pred}, CompoundNodeType::AND, 0, 10);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(res.value());
}

TEST_F(GroupReaderTest, StructColumnReaderPageIndexZoneMapFilterChildError) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    ParquetField child_field;
    child_field.name = "a";
    child_field.type = ColumnType::SCALAR;
    child_field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.children.push_back(child_field);

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockPredicateErrorReader>(&field.children[0]));
    StructColumnReader reader(&field, std::move(child_readers));

    RuntimeState runtime_state{TQueryGlobals()};
    ASSIGN_OR_ABORT(auto* pred, create_struct_subfield_eq_predicate(&_pool, &runtime_state, 0, 0, {"a"}, "x"));
    _pool.add(const_cast<ColumnPredicate*>(pred));

    SparseRange<uint64_t> row_ranges;
    auto res = reader.page_index_zone_map_filter({pred}, &row_ranges, CompoundNodeType::AND, 0, 10);
    ASSERT_TRUE(res.ok());
    ASSERT_FALSE(res.value());
    ASSERT_EQ(10, row_ranges.span_size());
}

} // namespace starrocks::parquet
