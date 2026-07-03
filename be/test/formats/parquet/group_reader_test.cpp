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
#include "exprs/expr.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/lazy_materialization_context.h"
#include "formats/parquet/read_range_planner.h"
#include "formats/parquet/utils.h"
#include "formats/reserved_columns.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/global_dict/parser.h"
#include "runtime/runtime_state.h"
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

class MockIORangeColumnReader : public ColumnReader {
public:
    explicit MockIORangeColumnReader(std::vector<io::SharedBufferedInputStream::IORange> ranges)
            : ColumnReader(nullptr), _ranges(std::move(ranges)) {}
    ~MockIORangeColumnReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst_col) override {
        dst_col->as_mutable_ptr()->append_default(range.span_size());
        return Status::OK();
    }

    void set_need_parse_levels(bool need_parse_levels) override {}
    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}
    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {}

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        if (ranges == nullptr) {
            return;
        }
        for (const auto& range : _ranges) {
            ranges->emplace_back(range.offset, range.size, active);
            if (end_offset != nullptr) {
                *end_offset = std::max(*end_offset, range.offset + range.size);
            }
        }
    }

private:
    std::vector<io::SharedBufferedInputStream::IORange> _ranges;
};

class MockTempColumnReader : public ColumnReader {
public:
    explicit MockTempColumnReader() : ColumnReader(nullptr) {}

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter*, ColumnPtr& dst) override {
        if (_temp_col == nullptr) {
            _temp_col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        }
        dst = _temp_col;
        _temp_col->as_mutable_raw_ptr()->append_datum(Datum(int32_t(42)));
        return Status::OK();
    }

    Status finalize_lazy_state(ColumnPtr& col) override {
        if (col.get() == _temp_col.get()) {
            _finalize_called = true;
            size_t n = _temp_col->size();
            auto dest = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            dest->append_default(n);
            _temp_col->as_mutable_raw_ptr()->reset_column();
            col = std::move(dest);
        }
        return Status::OK();
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override {
        dst = src;
        return Status::OK();
    }

    void set_need_parse_levels(bool) override {}
    void get_levels(level_t**, level_t**, size_t*) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

    ColumnPtr _temp_col = nullptr;
    bool _finalize_called = false;
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

static HdfsScannerStats g_hdfs_stats;
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
    // dereference scanner_ctx->options without crashing.
    auto* scanner_ctx = _pool.add(new HdfsScannerContext());

    auto* param = _pool.add(new GroupReaderParam());
    param->read_cols.emplace_back(c1);
    param->read_cols.emplace_back(c2);
    param->read_cols.emplace_back(c3);
    param->read_cols.emplace_back(c4);
    param->read_cols.emplace_back(c5);
    param->read_cols.emplace_back(c6);
    param->stats = &g_hdfs_stats;
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
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, t_conjuncts, &conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));
    ASSIGN_OR_RETURN(auto pred,
                     ColumnExprPredicate::make_column_expr_predicate(get_type_info(LogicalType::TYPE_VARCHAR),
                                                                     column_id, state, conjunct_ctxs[0], nullptr));
    return pred;
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

TEST_F(GroupReaderTest, StructColumnReaderReadRangeMissingSubfieldReader) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockColumnReader>(tparquet::Type::INT32));
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"b"}, {TYPE_INT_DESC});
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.read_range(Range<uint64_t>(0, 1), nullptr, dst);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(GroupReaderTest, StructColumnReaderReadRangeAllSubfieldsMissingInFile) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", nullptr);
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"a"}, {TYPE_INT_DESC});
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.read_range(Range<uint64_t>(0, 1), nullptr, dst);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
}

TEST_F(GroupReaderTest, StructColumnReaderFillDstColumnMissingSubfieldReader) {
    ParquetField field;
    field.name = "col_struct";
    field.type = ColumnType::STRUCT;

    std::map<std::string, ColumnReaderPtr> child_readers;
    child_readers.emplace("a", std::make_unique<MockColumnReader>(tparquet::Type::INT32));
    StructColumnReader reader(&field, std::move(child_readers));

    TypeDescriptor struct_type = TypeDescriptor::create_struct_type({"b"}, {TYPE_INT_DESC});
    ColumnPtr src = ColumnHelper::create_column(struct_type, true);
    ColumnPtr dst = ColumnHelper::create_column(struct_type, true);
    auto st = reader.fill_dst_column(dst, src);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
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
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, formats::kIcebergRowIdColumnId);
    ASSERT_OK(result);
    ASSERT_EQ(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
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
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
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
    // Legacy (non-lineage) GLM lookup: _row_id is the file-local row position. The read range is
    // file-local, so a row group starting at row 7 must emit 7..10 for its first four rows.
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
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
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 7);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(7, 11), nullptr, column));
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(7, column->get(0).get_int64());
    ASSERT_EQ(8, column->get(1).get_int64());
    ASSERT_EQ(9, column->get(2).get_int64());
    ASSERT_EQ(10, column->get(3).get_int64());
}

TEST_F(GroupReaderTest, TestIcebergRowIdSecondRowGroupUsesFileLevelFirstRowId) {
    // Row lineage (v3): _row_id = scan_range->first_row_id + file-local position. For a row group
    // that does not start the file (first row 7 here) the base must stay the file-level
    // first_row_id; a row-group-level base would double-count the row-group start and emit
    // 1014..1017 instead of 1007..1010.
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
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

    auto* scan_range = new THdfsScanRange();
    scan_range->__set_first_row_id(1000);
    param->scan_range = _pool.add(scan_range);

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto group_reader = std::make_unique<GroupReader>(*param, 0, skip_rows_ctx, 7);
    ASSERT_OK(group_reader->init());

    ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT), true);
    ASSERT_OK(group_reader->_column_readers[100]->read_range(Range<uint64_t>(7, 11), nullptr, column));
    ASSERT_EQ(4, column->size());
    ASSERT_EQ(1007, column->get(0).get_int64());
    ASSERT_EQ(1008, column->get(1).get_int64());
    ASSERT_EQ(1009, column->get(2).get_int64());
    ASSERT_EQ(1010, column->get(3).get_int64());
}

TEST_F(GroupReaderTest, TestIcebergLastUpdatedSequenceNumberColumnReaderCreation) {
    auto* param = _create_group_reader_param();
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
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

static tparquet::FileMetaData build_t_filemeta_with_iceberg_columns(ObjectPool* pool, const GroupReaderParam* param,
                                                                    bool include_row_id, bool include_seq_num) {
    tparquet::FileMetaData t_file_meta;

    size_t num_read_cols = param->read_cols.size();
    size_t extra_cols = (include_row_id ? 1 : 0) + (include_seq_num ? 1 : 0);
    size_t total_cols = num_read_cols + extra_cols;

    std::vector<tparquet::SchemaElement> schema_elements;

    tparquet::SchemaElement root;
    root.__set_num_children(static_cast<int32_t>(total_cols));
    schema_elements.push_back(root);

    for (size_t i = 0; i < num_read_cols; i++) {
        tparquet::SchemaElement elem;
        elem.__set_type(param->read_cols[i].type_in_parquet);
        elem.__set_name("c" + std::to_string(i));
        elem.__set_num_children(0);
        schema_elements.push_back(elem);
    }

    if (include_row_id) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(formats::kIcebergRowIdColumnName);
        elem.__set_num_children(0);
        elem.__set_field_id(formats::kIcebergRowIdColumnId);
        schema_elements.push_back(elem);
    }

    if (include_seq_num) {
        tparquet::SchemaElement elem;
        elem.__set_type(tparquet::Type::INT64);
        elem.__set_name(formats::kIcebergLastUpdatedSequenceNumberColumnName);
        elem.__set_num_children(0);
        elem.__set_field_id(formats::kIcebergLastUpdatedSequenceNumberColumnId);
        schema_elements.push_back(elem);
    }

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

    SlotDescriptor slot(200, formats::kIcebergRowIdColumnName,
                        TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT));
    auto result = group_reader->_create_reserved_iceberg_column_reader(&slot, formats::kIcebergRowIdColumnId);
    ASSERT_OK(result);
    // Physical column found -> non-null reader
    ASSERT_NE(result.value(), nullptr);
}

TEST_F(GroupReaderTest, TestIcebergRowIdPhysicalColumnReaderCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with _row_id
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
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
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
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

TEST_F(GroupReaderTest, CollectIORangesDeduplicatesIdenticalRangesAcrossReaders) {
    auto* file = _create_file();
    auto* param = _create_group_reader_param();

    FileMetaData* file_meta;
    ASSERT_TRUE(_create_filemeta(&file_meta, param).ok());

    param->chunk_size = config::vector_chunk_size;
    param->file = file;
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));
    ASSERT_OK(group_reader->init());

    group_reader->_column_readers.clear();
    group_reader->_column_materializer->clear_classification();
    group_reader->_column_materializer->add_active_column(0);
    group_reader->_column_materializer->add_lazy_column(1);
    group_reader->_column_readers.emplace(0, std::make_unique<MockIORangeColumnReader>(
                                                     std::vector<io::SharedBufferedInputStream::IORange>{{100, 20}}));
    group_reader->_column_readers.emplace(
            1, std::make_unique<MockIORangeColumnReader>(
                       std::vector<io::SharedBufferedInputStream::IORange>{{100, 20}, {200, 10}}));

    std::vector<io::SharedBufferedInputStream::IORange> ranges;
    int64_t end_offset = 0;
    group_reader->collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);

    ASSERT_EQ(2, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(20, ranges[0].size);
    EXPECT_TRUE(ranges[0].is_active);
    EXPECT_EQ(200, ranges[1].offset);
    EXPECT_EQ(10, ranges[1].size);
    EXPECT_FALSE(ranges[1].is_active);
    EXPECT_EQ(210, end_offset);
}

TEST_F(GroupReaderTest, TestIcebergBothPhysicalColumnsCreation) {
    auto* param = _create_group_reader_param();

    // Set up reserved_field_slots with both _row_id and _last_updated_sequence_number
    auto* reserved_slots = new std::vector<SlotDescriptor*>();
    auto* row_id_slot = _pool.add(new SlotDescriptor(100, formats::kIcebergRowIdColumnName,
                                                     TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT)));
    auto* seq_slot = _pool.add(new SlotDescriptor(101, formats::kIcebergLastUpdatedSequenceNumberColumnName,
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

TEST_F(GroupReaderTest, ReadRangePlannerDeduplicateMergesIdenticalRanges) {
    using IORange = io::SharedBufferedInputStream::IORange;
    std::vector<IORange> ranges;
    ranges.emplace_back(100, 50, true);
    ranges.emplace_back(100, 50, false);
    ranges.emplace_back(200, 30, true);
    ranges.emplace_back(200, 30, true);

    ReadRangePlanner::deduplicate(&ranges);

    // Two duplicates: (100,50) merged → is_active = true, (200,30) merged → is_active = true.
    ASSERT_EQ(2u, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(50, ranges[0].size);
    EXPECT_TRUE(ranges[0].is_active);
    EXPECT_EQ(200, ranges[1].offset);
    EXPECT_EQ(30, ranges[1].size);
    EXPECT_TRUE(ranges[1].is_active);
}

// Covers: ReadRangePlanner::deduplicate sorts by size when offsets are equal.

TEST_F(GroupReaderTest, ReadRangePlannerDeduplicateSortsBySizeWhenOffsetsEqual) {
    using IORange = io::SharedBufferedInputStream::IORange;
    std::vector<IORange> ranges;
    // Same offset, different sizes → must be ordered by size.
    ranges.emplace_back(100, 50, true);
    ranges.emplace_back(100, 30, true);

    ReadRangePlanner::deduplicate(&ranges);

    ASSERT_EQ(2u, ranges.size());
    EXPECT_EQ(100, ranges[0].offset);
    EXPECT_EQ(30, ranges[0].size);
    EXPECT_EQ(100, ranges[1].offset);
    EXPECT_EQ(50, ranges[1].size);
}

// Covers: ReadRangePlanner::should_coalesce_active_lazy returns true
//         when counter is nullptr (default).

TEST_F(GroupReaderTest, ReadRangePlannerShouldCoalesceReturnsTrueWhenCounterIsNull) {
    auto* param = _create_group_reader_param();
    param->lazy_column_coalesce_counter = nullptr;

    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> readers;
    ReadRangePlanner planner(*param, &readers);

    EXPECT_TRUE(planner.should_coalesce_active_lazy());
}

// Covers: ReadRangePlanner::should_coalesce_active_lazy reads
//         the adaptive counter.

TEST_F(GroupReaderTest, ReadRangePlannerShouldCoalesceReadsCounter) {
    auto* param = _create_group_reader_param();
    std::atomic<int32_t> counter = -1;
    param->lazy_column_coalesce_counter = &counter;

    std::unordered_map<int32_t, std::unique_ptr<ColumnReader>> readers;
    ReadRangePlanner planner(*param, &readers);

    EXPECT_FALSE(planner.should_coalesce_active_lazy());

    counter.store(0, std::memory_order_relaxed);
    EXPECT_TRUE(planner.should_coalesce_active_lazy());
}

// ── Temp Column Lifecycle Tests ──────────────────────────────────────────────

// Covers: ColumnMaterializer::materialize_slot finalizes lazy state
//         before caching, so _slot_cache always holds logical columns.

TEST_F(GroupReaderTest, TempColumnAccretesStaleWithoutReset) {
    MockTempColumnReader reader;

    ColumnPtr col1 = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    ASSERT_OK(reader.read_range(Range<uint64_t>(0, 1), nullptr, col1));
    EXPECT_EQ(1u, col1->size()) << "First read: 1 row";

    // Simulate skipped fill: don't call finalize_lazy_state

    ColumnPtr col2 = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    ASSERT_OK(reader.read_range(Range<uint64_t>(0, 1), nullptr, col2));
    EXPECT_EQ(2u, col2->size()) << "Without reset, second read accretes stale data: 1 stale + 1 new = 2 rows";

    // finalize_lazy_state produces n logical rows where n = temp size.
    // With stale accretion: temp has 2 rows, so finalize yields 2 rows.
    ASSERT_OK(reader.finalize_lazy_state(col2));
    EXPECT_EQ(2u, col2->size()) << "Stale accretion propagates: 2 logical rows from 2 temp rows";
    EXPECT_TRUE(reader._finalize_called);
    EXPECT_EQ(0u, reader._temp_col->size()) << "Temp column is empty after finalize";
}

// Contract test: finalize_lazy_state resets the temp column so that a
// subsequent read_range starts fresh — even when the mock itself does
// not reset before appending.  Verifies the finalize → reset → no-accretion
// lifecycle contract through the ColumnMaterializer pipeline.
//
// Production read_range() reset is exercised by integration tests.

static Status create_bigint_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id, int64_t value,
                                             std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::BIGINT, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_BIGINT>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_BIGINT, int64_t>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

static Status create_int_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id, int32_t value,
                                          std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::INT, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_INT>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_INT, int32_t>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

static Status create_varchar_eq_conjunct_ctxs(ObjectPool* pool, RuntimeState* state, SlotId slot_id,
                                              const std::string& value, std::vector<ExprContext*>* conjunct_ctxs) {
    TExprNode pred_node = ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, TExprOpcode::EQ);
    TExprNode slot_ref = ExprsTestHelper::create_slot_expr_node_t<TYPE_VARCHAR>(0, slot_id, true);
    TExprNode literal = ExprsTestHelper::create_literal<TYPE_VARCHAR, std::string>(value, false);

    TExpr expr;
    expr.nodes.emplace_back(pred_node);
    expr.nodes.emplace_back(slot_ref);
    expr.nodes.emplace_back(literal);

    std::vector<TExpr> conjunct_exprs{expr};
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, conjunct_exprs, conjunct_ctxs, nullptr));
    RETURN_IF_ERROR(Expr::prepare(*conjunct_ctxs, state));
    DictOptimizeParser::disable_open_rewrite(conjunct_ctxs);
    RETURN_IF_ERROR(Expr::open(*conjunct_ctxs, state));
    return Status::OK();
}

class MockGlobalDictColumnReader : public ColumnReader {
public:
    MockGlobalDictColumnReader() : ColumnReader(nullptr) {}
    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter*, ColumnPtr& dst) override {
        if (_code_column == nullptr) {
            _code_column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        }
        _code_column->as_mutable_raw_ptr()->reset_column();
        _code_column->as_mutable_raw_ptr()->append_datum(Datum(int32_t(100)));
        _code_column->as_mutable_raw_ptr()->append_datum(Datum(int32_t(200)));
        dst = _code_column;
        return Status::OK();
    }

    Status finalize_lazy_state(ColumnPtr& col) override {
        if (_finalize_called) return Status::OK();
        if (col.get() == _code_column.get()) {
            _finalize_called = true;
            auto logical = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("hello")));
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("world")));
            col = std::move(logical);
        }
        return Status::OK();
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override {
        if (src.get() == _code_column.get()) {
            auto logical = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARCHAR), true);
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("decoded_hello")));
            logical->as_mutable_raw_ptr()->append_datum(Datum(Slice("decoded_world")));
            dst->as_mutable_raw_ptr()->swap_column(*logical);
            return Status::OK();
        }
        dst->as_mutable_raw_ptr()->swap_column(*(src->as_mutable_raw_ptr()));
        return Status::OK();
    }

    void set_need_parse_levels(bool) override {}
    void get_levels(level_t**, level_t**, size_t*) override {}
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}
    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

    ColumnPtr _code_column = nullptr;
    bool _finalize_called = false;
};

TEST_F(GroupReaderTest, ActiveGlobalDictEmitBypassesFillDstAfterCompoundFinalize) {
    auto* param = _create_group_reader_param();
    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;
    param->read_cols.clear();

    // Two active low-card-like slots, each with a single-slot conjunct.
    auto* slot_a =
            _pool.add(new SlotDescriptor(360, "dict_a", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)));
    auto* slot_b =
            _pool.add(new SlotDescriptor(361, "dict_b", TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR)));

    GroupReaderParam::Column ca{};
    ca.slot_desc = slot_a;
    GroupReaderParam::Column cb{};
    cb.slot_desc = slot_b;
    param->read_cols.emplace_back(ca);
    param->read_cols.emplace_back(cb);

    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> ctx_a, ctx_b;
    ASSERT_OK(create_varchar_eq_conjunct_ctxs(&_pool, &runtime_state, slot_a->id(), "x", &ctx_a));
    ASSERT_OK(create_varchar_eq_conjunct_ctxs(&_pool, &runtime_state, slot_b->id(), "y", &ctx_b));
    param->conjunct_ctxs_by_slot[slot_a->id()] = ctx_a;
    param->conjunct_ctxs_by_slot[slot_b->id()] = ctx_b;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    auto mock_a = std::make_unique<MockGlobalDictColumnReader>();
    auto mock_b = std::make_unique<MockGlobalDictColumnReader>();
    auto* mock_a_ptr = mock_a.get();
    auto* mock_b_ptr = mock_b.get();
    group_reader->_column_readers.emplace(slot_a->id(), std::move(mock_a));
    group_reader->_column_readers.emplace(slot_b->id(), std::move(mock_b));

    group_reader->_process_columns_and_conjunct_ctxs();
    ASSERT_OK(group_reader->_column_materializer->init_read_chunk());

    ASSERT_TRUE(group_reader->_column_materializer->active_slot_ids().size() > 0);

    size_t num_rows = 2;
    Range<uint64_t> full_range(0, num_rows);
    auto active_chunk = group_reader->_column_materializer->create_active_chunk();

    group_reader->_column_materializer->reset_read_chunk();

    // Simulate step 2a: read active columns → PHYSICAL (dict codes).
    Filter filter(num_rows, 1);
    ASSERT_OK(group_reader->_column_materializer->read_active_range(full_range, &filter, &active_chunk));

    // Simulate step 2b compound-conjunct prep: finalize all active columns
    // to LOGICAL.  fill_dst_column() handles already-logical columns via
    // pointer identity fallback (swap), so no _logical_slot_ids mark needed.
    for (int col_idx : group_reader->_column_materializer->active_column_indices()) {
        SlotId slot_id = param->read_cols[col_idx].slot_id();
        ASSERT_OK(group_reader->_column_materializer->finalize_active_slot(slot_id, active_chunk));
    }
    EXPECT_TRUE(mock_a_ptr->_finalize_called);
    EXPECT_TRUE(mock_b_ptr->_finalize_called);

    // Build output chunk and emit.  Before the fix, emit_physical_columns
    // would call fill_dst_column() on the already-logical global-dict columns
    // and fail with "not a dictionary code column".
    std::vector<SlotDescriptor*> dst_slots{slot_a, slot_b};
    auto dst_chunk = std::make_shared<Chunk>();
    for (auto* s : dst_slots) {
        auto col = ColumnHelper::create_column(s->type(), true);
        dst_chunk->append_column(col, s->id());
    }

    ASSERT_OK(group_reader->_column_materializer->emit_physical_columns(active_chunk, &dst_chunk));

    for (auto* s : dst_slots) {
        EXPECT_TRUE(dst_chunk->is_slot_exist(s->id()));
        EXPECT_EQ(2u, dst_chunk->get_column_by_slot_id(s->id())->size());
    }
}

// Covers: GroupReader::get_next Phase 2b late_materialize_skip_rows
//         when compound (scanner_ctxs) conjuncts filter all rows.

TEST_F(GroupReaderTest, LateMaterializeSkipRowsCompoundConjunctFiltersAll) {
    auto* file = _create_file();
    auto* param = _create_group_reader_param();
    param->chunk_size = config::vector_chunk_size;
    param->file = file;

    // Scanner conjunct on col0: INT32 = 999999 → all rows filtered.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> scanner_ctxs;
    ASSERT_OK(
            create_int_eq_conjunct_ctxs(&_pool, &runtime_state, param->read_cols[0].slot_id(), 999999, &scanner_ctxs));
    param->scanner_ctx->conjuncts.scanner_ctxs = scanner_ctxs;

    FileMetaData* file_meta;
    ASSERT_OK(_create_filemeta(&file_meta, param));
    param->file_metadata = file_meta;

    SkipRowsContextPtr skip_rows_ctx = std::make_shared<SkipRowsContext>();
    auto* group_reader = _pool.add(new GroupReader(*param, 0, skip_rows_ctx, 0));

    ASSERT_OK(group_reader->init());
    group_reader->prepare();
    replace_column_readers(group_reader, param);
    group_reader->_column_materializer->mutable_read_chunk() = _create_chunk(param);
    prepare_row_range(group_reader);

    auto chunk = _create_chunk(param);
    size_t row_count = 8;
    auto status = group_reader->get_next(&chunk, &row_count);
    // All rows filtered by scanner_ctxs → get_next consumes all ranges and returns EOF.
    ASSERT_TRUE(status.is_end_of_file());
    // Skip count = first range (8) + second range (4).
    EXPECT_EQ(12, param->stats->late_materialize_skip_rows);
}

// Covers: HdfsScannerContext::evaluate_all_predicates evaluates
//         scanner_ctxs when slot-by-slot conjuncts are absent.

TEST_F(GroupReaderTest, EvaluateAllPredicatesWithScannerCtxs) {
    auto* scanner_ctx = _pool.add(new HdfsScannerContext());
    scanner_ctx->stats = &g_hdfs_stats;

    // Create a BIGINT chunk with 2 rows: values [0, 1].
    auto chunk = std::make_shared<Chunk>();
    auto col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    col->append_datum(Datum(int64_t(0)));
    col->append_datum(Datum(int64_t(1)));
    SlotId slot_id = 600;
    chunk->append_column(col, slot_id);

    // Scanner conjunct: col = 0 → filters row 1, keeps row 0.
    RuntimeState runtime_state{TQueryGlobals()};
    std::vector<ExprContext*> scanner_ctxs;
    ASSERT_OK(create_bigint_eq_conjunct_ctxs(&_pool, &runtime_state, slot_id, 0, &scanner_ctxs));
    scanner_ctx->conjuncts.scanner_ctxs = scanner_ctxs;

    ASSERT_OK(scanner_ctx->evaluate_all_predicates(&chunk));
    EXPECT_EQ(1u, chunk->num_rows());
    EXPECT_EQ(0, chunk->get_column_by_slot_id(slot_id)->get(0).get_int64());

    Expr::close(scanner_ctxs, &runtime_state);
}

} // namespace starrocks::parquet
