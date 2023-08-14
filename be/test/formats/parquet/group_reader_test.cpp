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

#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "exec/hdfs_scanner.h"
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
    MockColumnReader() = default;
    explicit MockColumnReader(tparquet::Type::type type) : _type(type) {}
    ~MockColumnReader() override = default;

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, Column* column) override {
        if (_step > 1) {
            *num_records = 0;
            return Status::EndOfFile("");
        }
        size_t start = 0;
        size_t num_rows = 0;
        if (_step == 0) {
            start = 0;
            num_rows = 8;
        } else if (_step == 1) {
            start = 8;
            num_rows = 4;
        }

        if (_type == tparquet::Type::type::INT32) {
            _append_int32_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT64) {
            _append_int64_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::INT96) {
            _append_int96_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::BYTE_ARRAY) {
            _append_binary_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::FLOAT) {
            _append_float_column(column, start, num_rows);
        } else if (_type == tparquet::Type::type::DOUBLE) {
            _append_double_column(column, start, num_rows);
        }

        _step++;
        *num_records = num_rows;
        return Status::OK();
    }

    Status finish_batch() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override {
        size_t rows = static_cast<size_t>(range.span_size());
        return prepare_batch(&rows, content_type, dst);
    }

    void set_need_parse_levels(bool need_parse_levels) override{};

    void get_levels(int16_t** def_levels, int16_t** rep_levels, size_t* num_levels) override {}

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
        auto c = ColumnHelper::create_column(column.col_type_in_chunk, true);
        chunk->append_column(c, column.col_idx_in_chunk);
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
        auto column = chunk->columns()[i].get();
        auto _type = param->read_cols[i].col_type_in_parquet;
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
        auto type = param->read_cols[i].col_type_in_parquet;
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

static GroupReaderParam::Column _create_group_reader_param_of_column(int idx, tparquet::Type::type par_type,
                                                                     LogicalType prim_type) {
    GroupReaderParam::Column c;
    c.field_idx_in_parquet = idx;
    c.col_idx_in_chunk = idx;
    c.col_type_in_parquet = par_type;
    c.col_type_in_chunk = TypeDescriptor::from_logical_type(prim_type);
    c.slot_id = idx;
    return c;
}

static HdfsScanStats g_hdfs_scan_stats;
GroupReaderParam* GroupReaderTest::_create_group_reader_param() {
    GroupReaderParam::Column c1 =
            _create_group_reader_param_of_column(0, tparquet::Type::type::INT32, LogicalType::TYPE_INT);
    GroupReaderParam::Column c2 =
            _create_group_reader_param_of_column(1, tparquet::Type::type::INT64, LogicalType::TYPE_BIGINT);
    GroupReaderParam::Column c3 =
            _create_group_reader_param_of_column(2, tparquet::Type::type::BYTE_ARRAY, LogicalType::TYPE_VARCHAR);
    GroupReaderParam::Column c4 =
            _create_group_reader_param_of_column(3, tparquet::Type::type::INT96, LogicalType::TYPE_DATETIME);
    GroupReaderParam::Column c5 =
            _create_group_reader_param_of_column(4, tparquet::Type::type::FLOAT, LogicalType::TYPE_FLOAT);
    GroupReaderParam::Column c6 =
            _create_group_reader_param_of_column(5, tparquet::Type::type::DOUBLE, LogicalType::TYPE_DOUBLE);

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
    std::set<int64_t> need_skip_rowids;
    auto* group_reader = _pool.add(new GroupReader(*param, 0, &need_skip_rowids, 0));

    // init row group reader
    status = group_reader->init();
    // timezone is empty
    ASSERT_FALSE(status.ok());
    //ASSERT_TRUE(status.is_end_of_file());
}

static void replace_column_readers(GroupReader* group_reader, GroupReaderParam* param) {
    group_reader->_column_readers.clear();
    group_reader->_active_column_indices.clear();
    group_reader->_dict_filter_ctx.init(param->read_cols.size());
    for (size_t i = 0; i < param->read_cols.size(); i++) {
        auto r = std::make_unique<MockColumnReader>(param->read_cols[i].col_type_in_parquet);
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
    std::set<int64_t> need_skip_rowids;
    auto* group_reader = _pool.add(new GroupReader(*param, 0, &need_skip_rowids, 0));

    // init row group reader
    status = group_reader->init();
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

} // namespace starrocks::parquet
