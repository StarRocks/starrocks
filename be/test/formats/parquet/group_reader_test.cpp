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

#include <formats/parquet/scalar_column_reader.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <memory>

#include "column/column_helper.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"

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
        auto column = chunk->columns()[i]->as_mutable_ptr().get();
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

    // Minimal scanner context so GroupReader::_create_column_readers() can
    // dereference scanner_ctx->params->options without crashing.
    auto* hdfs_params = _pool.add(new HdfsScannerParams());
    auto* scanner_ctx = _pool.add(new HdfsScannerContext());
    scanner_ctx->params = hdfs_params;

    auto* param = _pool.add(new GroupReaderParam());
    param->read_cols.emplace_back(c1);
    param->read_cols.emplace_back(c2);
    param->read_cols.emplace_back(c3);
    param->read_cols.emplace_back(c4);
    param->read_cols.emplace_back(c5);
    param->read_cols.emplace_back(c6);
    param->stats = &g_hdfs_scan_stats;
    param->scanner_ctx = scanner_ctx;
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
    group_reader->_column_materializer->clear_classification();
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto r = std::make_unique<MockColumnReader>(param->read_cols[i].type_in_parquet);
        group_reader->_column_readers[i] = std::move(r);
        group_reader->_column_materializer->add_active_column(i);
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
    group_reader->_column_materializer->mutable_read_chunk() = _create_chunk(param);

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

// ==================== Iceberg v3 Row Lineage Tests ====================

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueNullScanRange) {
    auto* param = _create_group_reader_param();
    param->scan_range = nullptr;

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueNoExtendedColumns) {
    auto* param = _create_group_reader_param();
    param->scan_range = _pool.add(new THdfsScanRange());

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueSlotNotFound) {
    auto* param = _create_group_reader_param();
    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(std::map<int32_t, TExpr>());
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(999);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueEmptyNodes) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    expr.nodes = std::vector<TExprNode>();
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_invalid_argument());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueWrongNodeType) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::BOOL_LITERAL;
    expr.nodes.push_back(node);
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_invalid_argument());
}

TEST_F(GroupReaderTest, TestGetExtendedBigIntValueSuccess) {
    auto* param = _create_group_reader_param();

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::INT_LITERAL;
    TIntLiteral int_literal;
    int_literal.value = 42;
    node.__set_int_literal(int_literal);
    expr.nodes.push_back(node);
    extended_columns[0] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    auto result = group_reader->_get_extended_bigint_value(0);
    ASSERT_OK(result);
    ASSERT_EQ(result.value().get_int64(), 42);
}

TEST_F(GroupReaderTest, TestCreateReservedIcebergColumnReaderNotFound) {
    auto* param = _create_group_reader_param();

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    SlotDescriptor slot(0, "_row_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, HdfsScanner::ICEBERG_ROW_ID_COLUMN_ID);
    ASSERT_OK(result);
    ASSERT_EQ(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, HdfsScanner::ICEBERG_ROW_ID,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdWithoutFirstRowIdReturnsNull) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, HdfsScanner::ICEBERG_ROW_ID,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(0, 4), nullptr, column));
    ASSERT_EQ(4, column->size());
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(column->get(i).is_null());
    }
}

TEST_F(GroupReaderTest, TestIcebergRowIdWithoutFirstRowIdUsesRowPositionForLookupPath) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, HdfsScanner::ICEBERG_ROW_ID,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* scan_range_id_slot = _pool.add(
            new SlotDescriptor(101, "_scan_range_id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)));
    reserved_slots->push_back(row_id_slot);
    reserved_slots->push_back(scan_range_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    auto* scan_range = new THdfsScanRange();
    param->scan_range = _pool.add(scan_range);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 7, 7);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(0, 4), nullptr, column));
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(7, column->get(0).get_int64());
    ASSERT_EQ(8, column->get(1).get_int64());
    ASSERT_EQ(9, column->get(2).get_int64());
    ASSERT_EQ(10, column->get(3).get_int64());
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSequenceNumberColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::INT_LITERAL;
    TIntLiteral int_literal;
    int_literal.value = 100;
    node.__set_int_literal(int_literal);
    expr.nodes.push_back(node);
    extended_columns[101] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSequenceNumberNullExtendedLiteralReturnsNull) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    std::map<int32_t, TExpr> extended_columns;
    TExpr expr;
    TExprNode node;
    node.node_type = TExprNodeType::NULL_LITERAL;
    expr.nodes.push_back(node);
    extended_columns[101] = expr;

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_extended_columns(extended_columns);
    param->scan_range = _pool.add(scan_range);

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));

    auto* file = _create_file();
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[101]->read_range(Range<uint64_t>(0, 4), nullptr, column));
    ASSERT_EQ(4, column->size());
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(column->get(i).is_null());
    }
}

// Helper to build a tparquet::FileMetaData that includes the standard 6 read_cols
// PLUS extra iceberg reserved columns (_row_id and/or _last_updated_sequence_number)
// with proper field_ids so that get_field_idx_by_field_id returns a valid index.
static tparquet::FileMetaData build_t_filemeta_with_iceberg_columns(ObjectPool* pool, const GroupReaderParam* param,
                                                                    bool include_row_id, bool include_seq_num) {
    tparquet::FileMetaData t_file_meta;

    // Count total columns
    size_t num_read_cols = param->read_cols.size();
    size_t extra_cols = (include_row_id ? 1 : 0) + (include_seq_num ? 1 : 0);
    size_t total_cols = num_read_cols + extra_cols;

    // Build schema elements
    std::vector<tparquet::SchemaElement> schema_elements;

    // Root element
    tparquet::SchemaElement root;
    root.__set_num_children(static_cast<int32_t>(total_cols));
    schema_elements.push_back(root);

    // Standard read_cols
    for (size_t i = 0; i < num_read_cols; i++) {
        tparquet::SchemaElement elem;
        elem.__set_type(param->read_cols[i].type_in_parquet);
        elem.__set_name("c" + std::to_string(i));
        elem.__set_num_children(0);
        schema_elements.push_back(elem);
    }

    // _row_id column with ICEBERG_ROW_ID_COLUMN_ID field_id
    if (include_row_id) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(HdfsScanner::ICEBERG_ROW_ID);
        elem.__set_num_children(0);
        elem.__set_field_id(HdfsScanner::ICEBERG_ROW_ID_COLUMN_ID);
        schema_elements.push_back(elem);
    }

    // _last_updated_sequence_number column
    if (include_seq_num) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER);
        elem.__set_num_children(0);
        elem.__set_field_id(HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_ID);
        schema_elements.push_back(elem);
    }

    // Build row groups (2 row groups, each with column chunks for all columns)
    std::vector<tparquet::RowGroup> row_groups;
    for (size_t rg = 0; rg < 2; rg++) {
        tparquet::RowGroup row_group;
        std::vector<tparquet::ColumnChunk> cols;
        for (size_t i = 0; i < total_cols; i++) {
            tparquet::ColumnChunk col;
            col.__set_file_path("c" + std::to_string(i));
            col.file_offset = 0;
            col.meta_data.data_page_offset = 4;
            cols.push_back(col);
        }
        row_group.__set_columns(cols);
        row_group.__set_num_rows(12);
        row_groups.push_back(row_group);
    }

    t_file_meta.__set_version(0);
    t_file_meta.__set_row_groups(row_groups);
    t_file_meta.__set_schema(schema_elements);
    return t_file_meta;
}

TEST_F(GroupReaderTest, TestCreateReservedIcebergColumnReaderFound) {
    auto* param = _create_group_reader_param();

    // Build file metadata with _row_id physical column
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/false);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;
    param->scanner_ctx->timezone = "UTC";

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    SlotDescriptor slot(200, HdfsScanner::ICEBERG_ROW_ID, TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, HdfsScanner::ICEBERG_ROW_ID_COLUMN_ID);
    ASSERT_OK(result);
    // Physical column found -> non-null reader
    ASSERT_NE(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdPhysicalColumnReaderCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with _row_id
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, HdfsScanner::ICEBERG_ROW_ID,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata WITH physical _row_id column (field_id matches)
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/false);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // The _row_id column reader should be created from the physical column (not IcebergRowIdReader)
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSeqNumPhysicalColumnReaderCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with _last_updated_sequence_number
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata WITH physical _last_updated_sequence_number column (field_id matches)
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/false, /*include_seq_num=*/true);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // The _last_updated_sequence_number reader should be created from the physical column
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

TEST_F(GroupReaderTest, TestIcebergBothPhysicalColumnsCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with both _row_id and _last_updated_sequence_number
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, HdfsScanner::ICEBERG_ROW_ID,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* seq_slot = _pool.add(new SlotDescriptor(101, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER,
                                                  TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    reserved_slots->push_back(row_id_slot);
    reserved_slots->push_back(seq_slot);
    param->scanner_ctx->reserved_field_slots = *_pool.add(reserved_slots);
    param->scanner_ctx->timezone = "UTC";

    // Build file metadata with both physical iceberg columns
    auto t_file_meta = build_t_filemeta_with_iceberg_columns(&_pool, param,
                                                             /*include_row_id=*/true, /*include_seq_num=*/true);
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    auto* file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->file = file;
    param->file_metadata = file_meta;
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 0);
    ASSERT_OK(group_reader->init());
    // Both physical column readers should be created
    ASSERT_NE(group_reader->_column_readers[100], nullptr);
    ASSERT_NE(group_reader->_column_readers[101], nullptr);
}

<<<<<<< HEAD
=======
TEST_F(GroupReaderTest, CreateColumnReadersRegistersVirtualZoneMapReaderForPhysicalSource) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* source_slot =
            _pool.add(new SlotDescriptor(180, "data", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT)));
    auto* virtual_slot =
            _pool.add(new SlotDescriptor(181, "data.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    GroupReaderParam::Column source_col{};
    source_col.slot_desc = source_slot;
    source_col.idx_in_parquet = 0;
    GroupReaderParam::Column virtual_col{};
    virtual_col.slot_desc = virtual_slot;
    virtual_col.idx_in_parquet = 0;
    virtual_col.is_extended_variant_virtual = true;
    virtual_col.source_variant_column_name = "data";
    virtual_col.variant_virtual_leaf_path = "a";

    param->read_cols.emplace_back(source_col);
    param->read_cols.emplace_back(virtual_col);

    auto t_file_meta = build_t_filemeta_with_variant_column("data");
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    param->file_metadata = file_meta;
    param->file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSERT_OK(group_reader->init());
    auto it = group_reader->_column_readers.find(virtual_slot->id());
    ASSERT_NE(it, group_reader->_column_readers.end());
    EXPECT_NE(nullptr, down_cast<VariantVirtualZoneMapReader*>(it->second.get()));
}

TEST(GroupReaderBloomFilterTest, DecimalBloomFilterApplicabilityRequiresExactLayoutMatch) {
    MockColumnReader reader(tparquet::Type::INT32);

    ParquetField decimal32_field;
    decimal32_field.physical_type = tparquet::Type::INT32;
    decimal32_field.precision = 5;
    decimal32_field.scale = 2;
    EXPECT_TRUE(reader.check_type_can_apply_bloom_filter(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2),
                                                         decimal32_field));
    EXPECT_FALSE(reader.check_type_can_apply_bloom_filter(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 6, 2),
                                                          decimal32_field));

    ParquetField decimal64_field;
    decimal64_field.physical_type = tparquet::Type::INT64;
    decimal64_field.precision = 16;
    decimal64_field.scale = 2;
    EXPECT_TRUE(reader.check_type_can_apply_bloom_filter(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 16, 2),
                                                         decimal64_field));

    ParquetField decimal128_field;
    decimal128_field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    decimal128_field.precision = 22;
    decimal128_field.scale = 4;
    EXPECT_FALSE(reader.check_type_can_apply_bloom_filter(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 22, 4),
                                                          decimal128_field));
}

// ── Hidden variant source active/lazy classification ─────────────────────────

// Covers: _lazy_hidden_slot_ids.push_back (lazy hidden source path in Step 4)
//         _active_slot_ids.push_back for active hidden (Step 5)
TEST_F(GroupReaderTest, ProcessColumnsClassifiesHiddenSourcesAsActiveOrLazy) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    // Two virtual columns: slot 120 has a conjunct (→ active source),
    //                      slot 121 is projection-only (→ lazy source).
    auto* vslot_active =
            _pool.add(new SlotDescriptor(120, "v1.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* vslot_lazy =
            _pool.add(new SlotDescriptor(121, "v2.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column vc1{};
    vc1.slot_desc = vslot_active;
    vc1.is_extended_variant_virtual = true;
    vc1.source_variant_column_name = "v1";
    vc1.variant_virtual_leaf_path = "a";
    GroupReaderParam::Column vc2{};
    vc2.slot_desc = vslot_lazy;
    vc2.is_extended_variant_virtual = true;
    vc2.source_variant_column_name = "v2";
    vc2.variant_virtual_leaf_path = "a";
    param->read_cols.emplace_back(vc1);
    param->read_cols.emplace_back(vc2);

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, vslot_active->id(), 11, &conjunct_ctxs));
    param->conjunct_ctxs_by_slot[vslot_active->id()] = conjunct_ctxs;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // Manually wire projections and hidden sources (normally done by _create_column_readers).
    const SlotId active_src_id = SlotId(-10);
    const SlotId lazy_src_id = SlotId(-11);
    ASSIGN_OR_ABORT(auto proj_active, make_virtual_projection_for_test("a", vslot_active->type(), active_src_id));
    ASSIGN_OR_ABORT(auto proj_lazy, make_virtual_projection_for_test("a", vslot_lazy->type(), lazy_src_id));
    group_reader->_variant->_projections.emplace(vslot_active->id(), std::move(proj_active));
    group_reader->_variant->_projections.emplace(vslot_lazy->id(), std::move(proj_lazy));

    auto& active_src =
            group_reader->_variant->_hidden_sources
                    .emplace("v1", VariantProjectionHandler::HiddenSource{.slot_id = active_src_id, .reader = nullptr})
                    .first->second;
    auto& lazy_src =
            group_reader->_variant->_hidden_sources
                    .emplace("v2", VariantProjectionHandler::HiddenSource{.slot_id = lazy_src_id, .reader = nullptr})
                    .first->second;
    group_reader->_variant->_hidden_slot_index[active_src_id] = &active_src;
    group_reader->_variant->_hidden_slot_index[lazy_src_id] = &lazy_src;

    group_reader->_process_columns_and_conjunct_ctxs();

    // active_src_id should be in _active_hidden_slot_ids (backed by conjunct)
    EXPECT_TRUE(std::find(group_reader->_variant->_active_hidden_slot_ids.begin(),
                          group_reader->_variant->_active_hidden_slot_ids.end(),
                          active_src_id) != group_reader->_variant->_active_hidden_slot_ids.end());
    // lazy_src_id should be in _lazy_hidden_slot_ids (projection-only)
    EXPECT_TRUE(std::find(group_reader->_variant->_lazy_hidden_slot_ids.begin(),
                          group_reader->_variant->_lazy_hidden_slot_ids.end(),
                          lazy_src_id) != group_reader->_variant->_lazy_hidden_slot_ids.end());
    // active_src_id should also appear in unified _active_slot_ids
    const auto& active_slot_ids = group_reader->_column_materializer->active_slot_ids();
    EXPECT_TRUE(std::find(active_slot_ids.begin(), active_slot_ids.end(), active_src_id) != active_slot_ids.end());
    // _deferred_conjunct_slot_ids must contain vslot_active
    EXPECT_EQ(1u, group_reader->_variant->_deferred_conjunct_slot_ids.count(vslot_active->id()));
}

TEST_F(GroupReaderTest, VariantVirtualPromotionSkipsDuplicateTypedLeafReaders) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* vslot1 = _pool.add(new SlotDescriptor(220, "data.a1", TypeDescriptor::from_logical_type(TYPE_INT)));
    auto* vslot2 = _pool.add(new SlotDescriptor(221, "data.a2", TypeDescriptor::from_logical_type(TYPE_INT)));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot1, "a"));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot2, "a"));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId source_slot_id = SlotId(-20);
    ASSIGN_OR_ABORT(auto proj1, make_virtual_projection_for_test("a", vslot1->type(), source_slot_id));
    ASSIGN_OR_ABORT(auto proj2, make_virtual_projection_for_test("a", vslot2->type(), source_slot_id));
    group_reader->_variant->_projections.emplace(vslot1->id(), std::move(proj1));
    group_reader->_variant->_projections.emplace(vslot2->id(), std::move(proj2));

    ASSIGN_OR_ABORT(auto reader, make_variant_promotion_reader(&_pool, {"a"}, 12, 12, 12));
    attach_hidden_variant_source_for_promotion_test(group_reader, source_slot_id, std::move(reader));
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    EXPECT_TRUE(group_reader->_variant->_promoted_virtual_slots.empty());
    EXPECT_EQ(2, group_reader->_variant->_projections.size());
    EXPECT_FALSE(group_reader->_variant->_hidden_slot_index.at(source_slot_id)->fully_promoted);
    EXPECT_EQ(group_reader->_column_readers.end(), group_reader->_column_readers.find(vslot1->id()));
    EXPECT_EQ(group_reader->_column_readers.end(), group_reader->_column_readers.find(vslot2->id()));
}

TEST_F(GroupReaderTest, VariantVirtualPromotionPromotesDifferentTypedLeafReaders) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* vslot1 = _pool.add(new SlotDescriptor(222, "data.a", TypeDescriptor::from_logical_type(TYPE_INT)));
    auto* vslot2 = _pool.add(new SlotDescriptor(223, "data.b", TypeDescriptor::from_logical_type(TYPE_INT)));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot1, "a"));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot2, "b"));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId source_slot_id = SlotId(-21);
    ASSIGN_OR_ABORT(auto proj1, make_virtual_projection_for_test("a", vslot1->type(), source_slot_id));
    ASSIGN_OR_ABORT(auto proj2, make_virtual_projection_for_test("b", vslot2->type(), source_slot_id));
    group_reader->_variant->_projections.emplace(vslot1->id(), std::move(proj1));
    group_reader->_variant->_projections.emplace(vslot2->id(), std::move(proj2));

    ASSIGN_OR_ABORT(auto reader, make_variant_promotion_reader(&_pool, {"a", "b"}, 12, 12, 12));
    attach_hidden_variant_source_for_promotion_test(group_reader, source_slot_id, std::move(reader));
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    EXPECT_EQ(2, group_reader->_variant->_promoted_virtual_slots.size());
    EXPECT_EQ(1u, group_reader->_variant->_promoted_virtual_slots.count(vslot1->id()));
    EXPECT_EQ(1u, group_reader->_variant->_promoted_virtual_slots.count(vslot2->id()));
    EXPECT_TRUE(group_reader->_variant->_projections.empty());
    EXPECT_TRUE(group_reader->_variant->_hidden_slot_index.at(source_slot_id)->fully_promoted);
    ASSERT_NE(group_reader->_column_readers.end(), group_reader->_column_readers.find(vslot1->id()));
    ASSERT_NE(group_reader->_column_readers.end(), group_reader->_column_readers.find(vslot2->id()));
    EXPECT_NE(nullptr, down_cast<VariantTypedValueProxy*>(group_reader->_column_readers.at(vslot1->id()).get()));
    EXPECT_NE(nullptr, down_cast<VariantTypedValueProxy*>(group_reader->_column_readers.at(vslot2->id()).get()));
}

TEST_F(GroupReaderTest, VariantVirtualPromotionClassifiesDictFilterConjuncts) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* dict_slot = _pool.add(new SlotDescriptor(228, "data.a", TypeDescriptor::from_logical_type(TYPE_VARCHAR)));
    auto* expr_slot = _pool.add(new SlotDescriptor(229, "data.b", TypeDescriptor::from_logical_type(TYPE_INT)));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(dict_slot, "a"));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(expr_slot, "b"));

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> dict_conjuncts;
    ASSERT_OK(create_varchar_eq_conjunct_ctxs(&_pool, &runtime_state, dict_slot->id(), "x", &dict_conjuncts));
    param->conjunct_ctxs_by_slot[dict_slot->id()] = dict_conjuncts;
    std::vector<ExprContext*> expr_conjuncts;
    ASSERT_OK(create_int_eq_conjunct_ctxs(&_pool, &runtime_state, expr_slot->id(), 7, &expr_conjuncts));
    param->conjunct_ctxs_by_slot[expr_slot->id()] = expr_conjuncts;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId source_slot_id = SlotId(-26);
    ASSIGN_OR_ABORT(auto dict_proj, make_virtual_projection_for_test("a", dict_slot->type(), source_slot_id));
    ASSIGN_OR_ABORT(auto expr_proj, make_virtual_projection_for_test("b", expr_slot->type(), source_slot_id));
    group_reader->_variant->_projections.emplace(dict_slot->id(), std::move(dict_proj));
    group_reader->_variant->_projections.emplace(expr_slot->id(), std::move(expr_proj));

    ASSIGN_OR_ABORT(auto reader,
                    make_variant_promotion_reader(&_pool, {"a", "b"}, 12, 12, 12, tparquet::Type::BYTE_ARRAY,
                                                  tparquet::Type::INT32, true, false));
    attach_hidden_variant_source_for_promotion_test(group_reader, source_slot_id, std::move(reader));
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    EXPECT_EQ(2, group_reader->_variant->_promoted_virtual_slots.size());
    const auto& dict_column_indices = group_reader->_column_materializer->dict_column_indices();
    const auto& dict_column_sub_field_paths = group_reader->_column_materializer->dict_column_sub_field_paths();
    const auto& post_read_conjuncts = group_reader->_column_materializer->post_read_conjuncts_by_slot();
    ASSERT_EQ(1, dict_column_indices.size());
    EXPECT_EQ(0, dict_column_indices[0]);
    EXPECT_EQ(1u, dict_column_sub_field_paths.count(0));
    ASSERT_EQ(1u, post_read_conjuncts.count(expr_slot->id()));
    EXPECT_EQ(1, post_read_conjuncts.at(expr_slot->id()).size());
    EXPECT_EQ(0u, post_read_conjuncts.count(dict_slot->id()));
}

TEST_F(GroupReaderTest, VariantVirtualPromotionSkipsNonAllNullFallback) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* vslot = _pool.add(new SlotDescriptor(224, "data.a", TypeDescriptor::from_logical_type(TYPE_INT)));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot, "a"));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId source_slot_id = SlotId(-22);
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a", vslot->type(), source_slot_id));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));

    ASSIGN_OR_ABORT(auto reader, make_variant_promotion_reader(&_pool, {"a"}, 12, 11, 12));
    attach_hidden_variant_source_for_promotion_test(group_reader, source_slot_id, std::move(reader));
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    EXPECT_TRUE(group_reader->_variant->_promoted_virtual_slots.empty());
    EXPECT_EQ(1, group_reader->_variant->_projections.size());
    EXPECT_FALSE(group_reader->_variant->_hidden_slot_index.at(source_slot_id)->fully_promoted);
}

TEST_F(GroupReaderTest, VariantVirtualPromotionSkipsIncompatibleTargetType) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* vslot = _pool.add(new SlotDescriptor(225, "data.a", TypeDescriptor::from_logical_type(TYPE_BIGINT)));
    param->read_cols.emplace_back(make_variant_virtual_column_for_promotion_test(vslot, "a"));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId source_slot_id = SlotId(-23);
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a", vslot->type(), source_slot_id));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));

    ASSIGN_OR_ABORT(auto reader, make_variant_promotion_reader(&_pool, {"a"}, 12, 12, 12));
    attach_hidden_variant_source_for_promotion_test(group_reader, source_slot_id, std::move(reader));
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    EXPECT_TRUE(group_reader->_variant->_promoted_virtual_slots.empty());
    EXPECT_EQ(1, group_reader->_variant->_projections.size());
    EXPECT_FALSE(group_reader->_variant->_hidden_slot_index.at(source_slot_id)->fully_promoted);
}

TEST_F(GroupReaderTest, VariantVirtualPromotionKeepsUnpromotedMixedSourceActiveAndRebuildsReadOrder) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    auto* promoted_slot = _pool.add(new SlotDescriptor(226, "data1.a", TypeDescriptor::from_logical_type(TYPE_INT)));
    auto* unpromoted_slot =
            _pool.add(new SlotDescriptor(227, "data2.a", TypeDescriptor::from_logical_type(TYPE_BIGINT)));
    auto promoted_col = make_variant_virtual_column_for_promotion_test(promoted_slot, "a");
    promoted_col.source_variant_column_name = "data1";
    auto unpromoted_col = make_variant_virtual_column_for_promotion_test(unpromoted_slot, "a");
    unpromoted_col.source_variant_column_name = "data2";
    param->read_cols.emplace_back(std::move(promoted_col));
    param->read_cols.emplace_back(std::move(unpromoted_col));

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, unpromoted_slot->id(), 11, &conjunct_ctxs));
    param->conjunct_ctxs_by_slot[unpromoted_slot->id()] = conjunct_ctxs;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    const SlotId promoted_source_slot_id = SlotId(-24);
    const SlotId unpromoted_source_slot_id = SlotId(-25);
    ASSIGN_OR_ABORT(auto promoted_proj,
                    make_virtual_projection_for_test("a", promoted_slot->type(), promoted_source_slot_id));
    ASSIGN_OR_ABORT(auto unpromoted_proj,
                    make_virtual_projection_for_test("a", unpromoted_slot->type(), unpromoted_source_slot_id));
    group_reader->_variant->_projections.emplace(promoted_slot->id(), std::move(promoted_proj));
    group_reader->_variant->_projections.emplace(unpromoted_slot->id(), std::move(unpromoted_proj));

    ASSIGN_OR_ABORT(auto promoted_reader, make_variant_promotion_reader(&_pool, {"a"}, 12, 12, 12));
    ASSIGN_OR_ABORT(auto unpromoted_reader, make_variant_promotion_reader(&_pool, {"a"}, 12, 12, 12));
    auto& promoted_source =
            group_reader->_variant->_hidden_sources
                    .emplace("data1", VariantProjectionHandler::HiddenSource{.slot_id = promoted_source_slot_id,
                                                                             .reader = std::move(promoted_reader)})
                    .first->second;
    auto& unpromoted_source =
            group_reader->_variant->_hidden_sources
                    .emplace("data2", VariantProjectionHandler::HiddenSource{.slot_id = unpromoted_source_slot_id,
                                                                             .reader = std::move(unpromoted_reader)})
                    .first->second;
    group_reader->_variant->_hidden_slot_index[promoted_source_slot_id] = &promoted_source;
    group_reader->_variant->_hidden_slot_index[unpromoted_source_slot_id] = &unpromoted_source;
    group_reader->_process_columns_and_conjunct_ctxs();

    group_reader->_variant->try_promote();

    group_reader->_column_materializer->rebuild_read_order_ctx();

    EXPECT_EQ(1u, group_reader->_variant->_promoted_virtual_slots.count(promoted_slot->id()));
    EXPECT_EQ(0u, group_reader->_variant->_promoted_virtual_slots.count(unpromoted_slot->id()));
    EXPECT_TRUE(group_reader->_variant->_hidden_slot_index.at(promoted_source_slot_id)->fully_promoted);
    EXPECT_FALSE(group_reader->_variant->_hidden_slot_index.at(unpromoted_source_slot_id)->fully_promoted);
    EXPECT_EQ(1u, group_reader->_variant->_projections.count(unpromoted_slot->id()));
    EXPECT_EQ(0u, group_reader->_variant->_projections.count(promoted_slot->id()));
    EXPECT_NE(group_reader->_variant->_active_hidden_slot_ids.end(),
              std::find(group_reader->_variant->_active_hidden_slot_ids.begin(),
                        group_reader->_variant->_active_hidden_slot_ids.end(), unpromoted_source_slot_id));
    EXPECT_EQ(group_reader->_variant->_active_hidden_slot_ids.end(),
              std::find(group_reader->_variant->_active_hidden_slot_ids.begin(),
                        group_reader->_variant->_active_hidden_slot_ids.end(), promoted_source_slot_id));
    auto* read_order_ctx = group_reader->_column_materializer->read_order_ctx();
    ASSERT_NE(nullptr, read_order_ctx);
    EXPECT_EQ(1u, read_order_ctx->get_column_read_order().size());
    EXPECT_EQ(0, read_order_ctx->get_column_read_order()[0]);
}

TEST_F(GroupReaderTest, CreateColumnReadersRegistersVirtualZoneMapReaderForHiddenSource) {
    auto* param = _create_group_reader_param();
    param->read_cols.clear();

    auto* virtual_slot =
            _pool.add(new SlotDescriptor(190, "data.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    GroupReaderParam::Column virtual_col{};
    virtual_col.slot_desc = virtual_slot;
    virtual_col.idx_in_parquet = 0;
    virtual_col.is_extended_variant_virtual = true;
    virtual_col.source_variant_column_name = "data";
    virtual_col.variant_virtual_leaf_path = "a";
    param->read_cols.emplace_back(virtual_col);

    auto t_file_meta = build_t_filemeta_with_variant_column("data");
    auto* file_meta = _pool.add(new FileMetaData());
    ASSERT_OK(file_meta->init(t_file_meta, true));

    param->file_metadata = file_meta;
    param->file = _pool.add(new RandomAccessFile(std::make_shared<MockInputStream>(), "mock"));
    param->chunk_size = config::vector_chunk_size;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSERT_OK(group_reader->init());
    ASSERT_FALSE(group_reader->_variant->_hidden_slot_index.empty());
    auto it = group_reader->_column_readers.find(virtual_slot->id());
    ASSERT_NE(it, group_reader->_column_readers.end());
    EXPECT_NE(nullptr, down_cast<VariantVirtualZoneMapReader*>(it->second.get()));
}

// Covers: _lazy_slot_ids.push_back (physical lazy column path in Step 5)
TEST_F(GroupReaderTest, ProcessColumnsPopulatesLazySlotIdsForPhysicalColumnsWithoutPredicates) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    // Two physical variant columns: slot 130 has a predicate (active), 131 has none (lazy).
    auto* active_slot =
            _pool.add(new SlotDescriptor(130, "col_a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* lazy_slot =
            _pool.add(new SlotDescriptor(131, "col_b", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column ca{};
    ca.slot_desc = active_slot;
    GroupReaderParam::Column cb{};
    cb.slot_desc = lazy_slot;
    param->read_cols.emplace_back(ca);
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, active_slot->id(), 42, &conjunct_ctxs));
    param->conjunct_ctxs_by_slot[active_slot->id()] = conjunct_ctxs;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto variant_col = make_typed_only_variant_column_for_virtual_column_test();
    group_reader->_column_readers.emplace(active_slot->id(),
                                          std::make_unique<MockVariantSourceColumnReader>(variant_col->clone()));
    group_reader->_column_readers.emplace(lazy_slot->id(),
                                          std::make_unique<MockVariantSourceColumnReader>(variant_col->clone()));

    // parquet_late_materialization_enable defaults to true; the no-predicate
    // column is expected to land in _lazy_column_indices / _lazy_slot_ids.
    group_reader->_process_columns_and_conjunct_ctxs();

    const auto& active_slot_ids = group_reader->_column_materializer->active_slot_ids();
    const auto& lazy_slot_ids = group_reader->_column_materializer->lazy_slot_ids();
    EXPECT_TRUE(std::find(active_slot_ids.begin(), active_slot_ids.end(), active_slot->id()) != active_slot_ids.end());
    EXPECT_TRUE(std::find(lazy_slot_ids.begin(), lazy_slot_ids.end(), lazy_slot->id()) != lazy_slot_ids.end());
}

// Covers: physical VARIANT column with no direct conjuncts must be promoted to
//         active when it is the source_slot_id for a deferred virtual conjunct.
//         Without the pre-pass fix this column would be lazified and Phase 4
//         would fail to find it in active_chunk.
TEST_F(GroupReaderTest, ProcessColumnsPromotesPhysicalVariantSourceForDeferredConjunct) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    // Physical VARIANT column — no direct conjunct, should normally be lazy.
    auto* phys_slot =
            _pool.add(new SlotDescriptor(170, "v", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARIANT)));
    // Virtual slot with a deferred conjunct whose source is the physical slot.
    auto* virt_slot =
            _pool.add(new SlotDescriptor(171, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column pc{};
    pc.slot_desc = phys_slot;
    GroupReaderParam::Column vc{};
    vc.slot_desc = virt_slot;
    vc.is_extended_variant_virtual = true;
    param->read_cols.emplace_back(pc);
    param->read_cols.emplace_back(vc);

    // Conjunct on the virtual slot.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, virt_slot->id(), 42, &conjunct_ctxs));
    param->conjunct_ctxs_by_slot[virt_slot->id()] = conjunct_ctxs;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto variant_col = make_typed_only_variant_column_for_virtual_column_test();
    group_reader->_column_readers.emplace(phys_slot->id(),
                                          std::make_unique<MockVariantSourceColumnReader>(variant_col->clone()));

    // Wire projection: virtual slot 171 → source is physical slot 170 (non-negative).
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a", virt_slot->type(), phys_slot->id()));
    group_reader->_variant->_projections.emplace(virt_slot->id(), std::move(proj));

    group_reader->_process_columns_and_conjunct_ctxs();

    // Physical slot must be active (not lazy) because a deferred conjunct needs it.
    const auto& active_slot_ids = group_reader->_column_materializer->active_slot_ids();
    const auto& lazy_slot_ids = group_reader->_column_materializer->lazy_slot_ids();
    EXPECT_TRUE(std::find(active_slot_ids.begin(), active_slot_ids.end(), phys_slot->id()) != active_slot_ids.end());
    EXPECT_TRUE(std::find(lazy_slot_ids.begin(), lazy_slot_ids.end(), phys_slot->id()) == lazy_slot_ids.end());
    // The virtual conjunct slot must be registered.
    EXPECT_EQ(1u, group_reader->_variant->_deferred_conjunct_slot_ids.count(virt_slot->id()));
}

// ── _apply_deferred_variant_conjuncts ────────────────────────────────────────

// Covers: Status::InternalError when a deferred-conjunct slot's source is absent
//         from active_chunk (invariant violation path, lines ~1289-1293).
TEST_F(GroupReaderTest, ApplyDeferredVariantConjunctsReturnsErrorWhenConjunctSourceMissing) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto* vslot =
            _pool.add(new SlotDescriptor(140, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // Wire projection for vslot pointing to source slot -20 (absent from active_chunk).
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a.b.c", vslot->type(), SlotId(-20)));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));
    // Mark this slot as having a deferred conjunct (simulating Step 1 output).
    group_reader->_variant->_deferred_conjunct_slot_ids.insert(vslot->id());
    // Push a dummy conjunct so the early-exit guard doesn't fire.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, vslot->id(), 11, &conjunct_ctxs));
    for (auto* ctx : conjunct_ctxs) {
        group_reader->_variant->_deferred_variant_virtual_conjunct_ctxs.push_back(ctx);
    }

    // active_chunk is empty — source slot -20 is absent.
    auto active_chunk = std::make_shared<Chunk>();
    auto projected_chunk = std::make_shared<Chunk>();
    auto status = group_reader->_variant->filter_subfields(active_chunk, 2, nullptr,
                                                           group_reader->_variant->projection_timezone());
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.status().is_internal_error());
}

// Covers: eval_chunk->append_column (line ~1300) — the success path where source
//         IS in active_chunk and the filter correctly selects matching rows.
TEST_F(GroupReaderTest, ApplyDeferredVariantConjunctsProjectsSourceAndFilters) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto* vslot =
            _pool.add(new SlotDescriptor(150, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // Source slot -21 will be present in active_chunk.
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a.b.c", vslot->type(), SlotId(-21)));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));
    group_reader->_variant->_deferred_conjunct_slot_ids.insert(vslot->id());

    // Conjunct: v.a == 22  (row 0 has 11, row 1 has 22 → only row 1 passes).
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> conjunct_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, vslot->id(), 22, &conjunct_ctxs));
    for (auto* ctx : conjunct_ctxs) {
        group_reader->_variant->_deferred_variant_virtual_conjunct_ctxs.push_back(ctx);
    }

    // active_chunk contains the source variant column with typed path "a.b.c" → [11, 22].
    auto active_chunk = std::make_shared<Chunk>();
    active_chunk->append_column(make_typed_only_variant_column_for_virtual_column_test(), SlotId(-21));

    auto projected_chunk = std::make_shared<Chunk>();
    ASSIGN_OR_ABORT(auto filter, group_reader->_variant->filter_subfields(
                                         active_chunk, 2, nullptr, group_reader->_variant->projection_timezone()));
    ASSERT_EQ(2u, filter.size());
    EXPECT_EQ(0, filter[0]); // row 0 (value=11) rejected
    EXPECT_EQ(1, filter[1]); // row 1 (value=22) passed
    // active_chunk is NOT filtered here; the caller merges and applies the combined filter.
    EXPECT_EQ(2u, active_chunk->num_rows());
}

TEST_F(GroupReaderTest, AlignDeferredProjectedChunkAfterFilterReturnsErrorWhenRowCountMismatch) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto active_chunk = std::make_shared<Chunk>();
    auto active_col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    active_col->append_datum(Datum(int64_t{1}));
    active_col->append_datum(Datum(int64_t{2}));
    active_chunk->append_column(active_col, SlotId(1));

    auto projected_chunk = std::make_shared<Chunk>();
    auto projected_col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    projected_col->append_datum(Datum(int64_t{99}));
    projected_chunk->append_column(projected_col, SlotId(2));

    Filter filter = {1, 0};
    group_reader->_variant->_deferred_projected_chunk = projected_chunk;
    auto st = group_reader->_variant->align_after_combined_filter(active_chunk, filter, 2);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(GroupReaderTest, AlignDeferredProjectedChunkAfterFilterAppliesFilterForPreFilterRows) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto active_chunk = std::make_shared<Chunk>();
    auto active_col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    active_col->append_datum(Datum(int64_t{1}));
    active_col->append_datum(Datum(int64_t{2}));
    active_chunk->append_column(active_col, SlotId(1));

    auto projected_chunk = std::make_shared<Chunk>();
    auto projected_col = ColumnHelper::create_column(TYPE_BIGINT_DESC, true);
    projected_col->append_datum(Datum(int64_t{11}));
    projected_col->append_datum(Datum(int64_t{22}));
    projected_chunk->append_column(projected_col, SlotId(2));

    Filter filter = {1, 0};
    group_reader->_variant->_deferred_projected_chunk = projected_chunk;
    ASSERT_OK(group_reader->_variant->align_after_combined_filter(active_chunk, filter, 2));
    ASSERT_EQ(1u, projected_chunk->num_rows());
    EXPECT_EQ(11, projected_chunk->get_column_by_slot_id(SlotId(2))->get(0).get_int64());
}

// ── _fill_dst_chunk error path ────────────────────────────────────────────────

// Covers: Status::InternalError in _fill_dst_chunk when source slot absent
//         from active_chunk (lines ~1340-1342).
TEST_F(GroupReaderTest, FillDstChunkReturnsErrorWhenSourceSlotMissingFromActiveChunk) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto* vslot =
            _pool.add(new SlotDescriptor(160, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column vc{};
    vc.slot_desc = vslot;
    param->read_cols.emplace_back(vc);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // Wire projection pointing to source slot -30 — NOT present in active_chunk.
    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a.b.c", vslot->type(), SlotId(-30)));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));

    auto active_chunk = std::make_shared<Chunk>(); // source slot -30 absent
    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(vslot->type(), true), vslot->id());

    auto status = fill_dst_chunk_without_projected(group_reader, active_chunk, &dst_chunk);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is_internal_error());
}

TEST_F(GroupReaderTest, FillDstChunkReturnsErrorWhenDeferredProjectedColumnIsNull) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto* vslot =
            _pool.add(new SlotDescriptor(260, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column vc{};
    vc.slot_desc = vslot;
    param->read_cols.emplace_back(vc);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a.b.c", vslot->type(), SlotId(-31)));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));

    auto active_chunk = std::make_shared<Chunk>();
    active_chunk->append_column(make_typed_only_variant_column_for_virtual_column_test(), SlotId(-31));

    auto projected_chunk = std::make_shared<Chunk>();
    projected_chunk->append_column(ColumnHelper::create_column(vslot->type(), true), vslot->id());
    projected_chunk->get_column_by_slot_id(vslot->id()).reset();

    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(vslot->type(), true), vslot->id());

    group_reader->_variant->_deferred_projected_chunk = projected_chunk;
    auto status = group_reader->_variant->emit_projections(active_chunk, &dst_chunk,
                                                           group_reader->_variant->projection_timezone());
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is_internal_error());
}

TEST_F(GroupReaderTest, FillDstChunkReturnsErrorWhenDeferredProjectedColumnRowCountMismatch) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto* vslot =
            _pool.add(new SlotDescriptor(261, "v.a", TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));

    param->read_cols.clear();
    GroupReaderParam::Column vc{};
    vc.slot_desc = vslot;
    param->read_cols.emplace_back(vc);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSIGN_OR_ABORT(auto proj, make_virtual_projection_for_test("a.b.c", vslot->type(), SlotId(-32)));
    group_reader->_variant->_projections.emplace(vslot->id(), std::move(proj));

    auto active_chunk = std::make_shared<Chunk>();
    active_chunk->append_column(make_typed_only_variant_column_for_virtual_column_test(), SlotId(-32));

    auto projected_chunk = std::make_shared<Chunk>();
    auto projected_col = ColumnHelper::create_column(vslot->type(), true);
    projected_col->append_datum(Datum(int64_t{1}));
    projected_chunk->append_column(projected_col, vslot->id());

    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(vslot->type(), true), vslot->id());

    group_reader->_variant->_deferred_projected_chunk = projected_chunk;
    auto status = group_reader->_variant->emit_projections(active_chunk, &dst_chunk,
                                                           group_reader->_variant->projection_timezone());
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.is_internal_error());
}

TEST_F(GroupReaderTest, GetVariantShreddedHintsReturnsEmptyOnInvalidAccessPath) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    std::vector<ColumnAccessPathPtr> column_access_paths;
    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::ROOT, "data", 0));
    ASSIGN_OR_ABORT(auto field_arr, ColumnAccessPath::create(TAccessPathType::FIELD, "arr", 0, root->absolute_path()));
    ASSIGN_OR_ABORT(auto offset_0,
                    ColumnAccessPath::create(TAccessPathType::OFFSET, "0", 0, field_arr->absolute_path()));
    field_arr->children().emplace_back(std::move(offset_0));
    root->children().emplace_back(std::move(field_arr));
    column_access_paths.emplace_back(std::move(root));
    const_cast<HdfsScannerParams*>(param->scanner_ctx->params)->column_access_paths = &column_access_paths;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    auto hints = group_reader->_variant->_build_shredded_hints("data");
    EXPECT_TRUE(hints.shredded_paths.empty());
    EXPECT_TRUE(hints.parsed_shredded_paths.empty());
}

TEST_F(GroupReaderTest, GetVariantShreddedHintsReturnsEmptyWhenFullColumnAccessIsPresent) {
    std::vector<ColumnAccessPathPtr> column_access_paths;

    ASSIGN_OR_ABORT(auto full_root, ColumnAccessPath::create(TAccessPathType::ROOT, "data", 0));
    column_access_paths.emplace_back(std::move(full_root));

    ASSIGN_OR_ABORT(auto subfield_root, ColumnAccessPath::create(TAccessPathType::ROOT, "data", 0));
    ASSIGN_OR_ABORT(auto field_a,
                    ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0, subfield_root->absolute_path()));
    ASSIGN_OR_ABORT(auto field_b, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0, field_a->absolute_path()));
    subfield_root->children().emplace_back(std::move(field_a));
    subfield_root->children()[0]->children().emplace_back(std::move(field_b));
    column_access_paths.emplace_back(std::move(subfield_root));

    auto hints = build_variant_shredded_hints(&column_access_paths, "data");
    EXPECT_TRUE(hints.shredded_paths.empty());
    EXPECT_TRUE(hints.parsed_shredded_paths.empty());
}

// ── Decimal virtual column fallback tests ──────────────────────────────────────
//
// These tests exercise the row-by-row decimal fallback path that is reached when
// neither build_exact_typed_variant_projection nor build_decimal_typed_variant_projection
// succeeds (i.e. the shredded leaf is absent or is a non-decimal type).
//
// Covered lines in group_reader.cpp (build_decimal_typed_variant_projection early
// returns + build_decimal_variant_projection_column body):
//   line 219: !reader.is_typed_exact() → no shredded leaf at path, decimal target
//   line 224: source leaf is not decimal (e.g. INT64) with decimal target
//   lines 262-300: build_decimal_variant_projection_column (normal + overflow rows)
//   lines 346-351: TYPE_DECIMAL32/64/128 switch cases in project_variant_leaf_column

// Test: raw JSON variant data (no typed shredded leaf) with a DECIMAL32 target.
// build_decimal_typed_variant_projection returns NotFound at line 219 (no typed exact
// leaf), then build_decimal_variant_projection_column is invoked for the row-by-row cast.
TEST_F(GroupReaderTest, FillDstChunkProjectsDecimalVirtualColumnFromRawVariantFallback) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto decimal_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 2);
    auto* virtual_slot = _pool.add(new SlotDescriptor(400, "data.price", decimal_type));
    param->read_cols.clear();
    GroupReaderParam::Column virtual_col{};
    virtual_col.slot_desc = virtual_slot;
    param->read_cols.emplace_back(virtual_col);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    // Project path "a.b.c" → DECIMAL32(5,2). The raw variant rows hold integer values
    // at that path; they must be cast row-by-row via cast_variant_to_decimal.
    ASSIGN_OR_ABORT(auto projection, make_virtual_projection_for_test("a.b.c", virtual_slot->type(), SlotId(401)));
    group_reader->_variant->_projections.emplace(virtual_slot->id(), std::move(projection));

    // Raw variant: {"a":{"b":{"c":10}}} and {"a":{"b":{"c":20}}} — no typed shredded leaf.
    auto variant_src = make_raw_json_variant_column_for_virtual_column_test(
            {R"({"a":{"b":{"c":10}}})", R"({"a":{"b":{"c":20}}})"});

    auto read_chunk = std::make_shared<Chunk>();
    read_chunk->append_column(variant_src, SlotId(401));

    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(virtual_slot->type(), true), virtual_slot->id());

    ASSERT_OK(fill_dst_chunk_without_projected(group_reader, read_chunk, &dst_chunk));
    const auto& result = dst_chunk->get_column_by_slot_id(virtual_slot->id());
    ASSERT_EQ(2, result->size());
    // 10 → 10.00 in DECIMAL32(5,2) stored as 1000; 20 → 2000.
    EXPECT_EQ(1000, result->get(0).get_int32());
    EXPECT_EQ(2000, result->get(1).get_int32());
}

// Test: variant with an INT64 typed shredded leaf at "a.b.c" but a DECIMAL32 target.
// build_decimal_typed_variant_projection returns NotFound at line 224 (source is INT64,
// not a decimal type), then build_decimal_variant_projection_column does the row-by-row
// cast reading from the typed INT64 column.
TEST_F(GroupReaderTest, FillDstChunkProjectsDecimalVirtualColumnFromBigintTypedLeafFallback) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto decimal_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 7, 2);
    auto* virtual_slot = _pool.add(new SlotDescriptor(402, "data.price", decimal_type));
    param->read_cols.clear();
    GroupReaderParam::Column virtual_col{};
    virtual_col.slot_desc = virtual_slot;
    param->read_cols.emplace_back(virtual_col);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSIGN_OR_ABORT(auto projection, make_virtual_projection_for_test("a.b.c", virtual_slot->type(), SlotId(403)));
    group_reader->_variant->_projections.emplace(virtual_slot->id(), std::move(projection));

    // Typed INT64 leaf at "a.b.c" (values 11 and 22).
    // Target DECIMAL32(7,2): exact match fails (INT64 ≠ DECIMAL32); decimal typed match
    // fails at line 224 (INT64 is not a decimal type); row-by-row cast follows.
    auto variant_src = make_typed_only_variant_column_for_virtual_column_test(); // INT64 values 11, 22

    auto read_chunk = std::make_shared<Chunk>();
    read_chunk->append_column(variant_src, SlotId(403));

    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(virtual_slot->type(), true), virtual_slot->id());

    ASSERT_OK(fill_dst_chunk_without_projected(group_reader, read_chunk, &dst_chunk));
    const auto& result = dst_chunk->get_column_by_slot_id(virtual_slot->id());
    ASSERT_EQ(2, result->size());
    // 11 → 11.00 stored as 1100; 22 → 2200.
    EXPECT_EQ(1100, result->get(0).get_int32());
    EXPECT_EQ(2200, result->get(1).get_int32());
}

// Test: raw JSON variant with a value that overflows DECIMAL32 storage.
// cast_variant_to_decimal uses DecimalV3Cast::from_integer which checks for int32_t overflow
// (value × scale_factor > INT32_MAX).  For scale=2, scale_factor=100; integer 21474837 ×
// 100 = 2147483700 exceeds INT32_MAX (2147483647), so overflow=true and the row is NULL
// (lines 292-293 in build_decimal_variant_projection_column).
TEST_F(GroupReaderTest, FillDstChunkProjectsDecimalVirtualColumnOverflowBecomesNull) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    auto decimal_type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2);
    auto* virtual_slot = _pool.add(new SlotDescriptor(404, "data.price", decimal_type));
    param->read_cols.clear();
    GroupReaderParam::Column virtual_col{};
    virtual_col.slot_desc = virtual_slot;
    param->read_cols.emplace_back(virtual_col);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSIGN_OR_ABORT(auto projection, make_virtual_projection_for_test("a.b.c", virtual_slot->type(), SlotId(405)));
    group_reader->_variant->_projections.emplace(virtual_slot->id(), std::move(projection));

    // Row 0: integer 5 → 5 × 100 = 500, fits in int32_t → non-null.
    // Row 1: integer 21474837 → 21474837 × 100 = 2147483700 > INT32_MAX → overflow → NULL.
    auto variant_src = make_raw_json_variant_column_for_virtual_column_test(
            {R"({"a":{"b":{"c":5}}})", R"({"a":{"b":{"c":21474837}}})"});

    auto read_chunk = std::make_shared<Chunk>();
    read_chunk->append_column(variant_src, SlotId(405));

    auto dst_chunk = std::make_shared<Chunk>();
    dst_chunk->append_column(ColumnHelper::create_column(virtual_slot->type(), true), virtual_slot->id());

    ASSERT_OK(fill_dst_chunk_without_projected(group_reader, read_chunk, &dst_chunk));
    const auto& result = dst_chunk->get_column_by_slot_id(virtual_slot->id());
    ASSERT_EQ(2, result->size());
    EXPECT_EQ(500, result->get(0).get_int32()); // 5 × 100 = 500
    EXPECT_TRUE(result->is_null(1));            // 21474837 × 100 overflows int32_t → NULL
}

>>>>>>> 4e0fe034f9 ([Refactor] Consolidate scanner options and conjuncts into shared structs, unify predicate evaluation in base class (#74559))
} // namespace starrocks::parquet
