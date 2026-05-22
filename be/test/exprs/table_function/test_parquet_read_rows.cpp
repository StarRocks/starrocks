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
#include <sys/stat.h>
#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <string>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "common/statusor.h"
#include "exprs/table_function/parquet_read_rows.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "types/json_value.h"

namespace starrocks {

class ParquetReadRowsTableFunctionTest : public ::testing::Test {
public:
    void SetUp() override {
        _runtime_state = std::make_unique<RuntimeState>(TQueryGlobals());
        // Use a per-test-instance subdir under /tmp so parallel test runs do not collide.
        std::random_device rd;
        std::mt19937_64 gen(rd());
        char buf[64];
        std::snprintf(buf, sizeof(buf), "/tmp/parquet_read_rows_ut_%016llx", static_cast<unsigned long long>(gen()));
        _tmp_dir = buf;
        std::filesystem::create_directories(_tmp_dir);
    }

    void TearDown() override {
        if (!_tmp_dir.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(_tmp_dir, ec);
        }
    }

protected:
    // Write a small (id INT, val INT, name VARCHAR) parquet file to `path`. Returns
    // (file_size_bytes, file_mtime_seconds) for anchor construction.
    StatusOr<std::pair<int64_t, int64_t>> _write_fixture(const std::string& path, const std::vector<int32_t>& ids,
                                                         const std::vector<int32_t>& vals,
                                                         const std::vector<std::string>& names) {
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::create_varchar_type(64),
        };

        auto chunk = std::make_shared<Chunk>();
        auto col_id = ColumnHelper::create_column(type_descs[0], true);
        auto col_val = ColumnHelper::create_column(type_descs[1], true);
        auto col_name = ColumnHelper::create_column(type_descs[2], true);
        for (size_t i = 0; i < ids.size(); ++i) {
            int32_t id = ids[i];
            int32_t v = vals[i];
            col_id->append_numbers(&id, sizeof(int32_t));
            col_val->append_numbers(&v, sizeof(int32_t));
            col_name->append_strings(std::vector<Slice>{Slice(names[i])});
        }
        chunk->append_column(std::move(col_id), 0);
        chunk->append_column(std::move(col_val), 1);
        chunk->append_column(std::move(col_name), 2);

        // Use explicit column names so the parquet schema preserves "id" /
        // "val" / "name" — `debug_string()` would emit type descriptors
        // (e.g. "INT") and the two INT columns would collide.
        std::vector<std::string> type_names{"id", "val", "name"};
        auto schema_or = parquet::ParquetBuildHelper::make_schema(
                type_names, type_descs, std::vector<parquet::FileColumnId>(type_descs.size()));
        if (!schema_or.ok()) {
            return Status::InternalError(strings::Substitute("make_schema failed: $0", schema_or.status().ToString()));
        }
        auto schema = schema_or.ValueOrDie();
        ASSIGN_OR_RETURN(auto file, FileSystem::Default()->new_writable_file(path));
        ASSIGN_OR_RETURN(auto properties,
                         parquet::ParquetBuildHelper::make_properties(parquet::ParquetBuilderOptions()));
        auto writer = std::make_shared<parquet::SyncFileWriter>(std::move(file), properties, schema, type_descs,
                                                                _runtime_state.get());
        RETURN_IF_ERROR(writer->init());
        RETURN_IF_ERROR(writer->write(chunk.get()));
        RETURN_IF_ERROR(writer->close());

        struct stat st {};
        if (::stat(path.c_str(), &st) != 0) {
            return Status::IOError("stat failed");
        }
        return std::make_pair(static_cast<int64_t>(st.st_size), static_cast<int64_t>(st.st_mtime));
    }

    // Build a single-row JSON input column with the given source_info text.
    JsonColumn::MutablePtr _make_source_info_column(const std::vector<std::string>& source_infos) {
        auto col = JsonColumn::create();
        for (const auto& s : source_infos) {
            auto jv = JsonValue::parse(Slice(s));
            if (jv.ok()) {
                col->append(std::move(jv).value());
            } else {
                col->append(JsonValue{});
            }
        }
        return col;
    }

    std::string _make_anchor(const std::string& file, int64_t row_in_file, int64_t file_size, int64_t file_mtime_ms) {
        return strings::Substitute(
                R"({"format":"parquet","file":"$0","row_in_file":$1,"file_size":$2,"file_mtime_ms":$3})", file,
                row_in_file, file_size, file_mtime_ms);
    }

    std::unique_ptr<RuntimeState> _runtime_state;
    std::string _tmp_dir;
    ParquetReadRows _tvf;
};

// ---------- happy path: rehydrate the second row of a 3-row file ----------
TEST_F(ParquetReadRowsTableFunctionTest, RehydrateMiddleRow) {
    std::string parquet_path = _tmp_dir + "/fixture.parquet";
    ASSIGN_OR_ABORT(auto meta, _write_fixture(parquet_path, {1, 2, 3}, {100, 200, 300}, {"alice", "bob", "carol"}));
    int64_t file_size = meta.first;
    int64_t file_mtime_ms = meta.second * 1000;

    auto input = _make_source_info_column({_make_anchor(parquet_path, 1, file_size, file_mtime_ms)});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    ASSERT_OK(_tvf.prepare(state));
    ASSERT_OK(_tvf.open(_runtime_state.get(), state));
    state->set_params({input});

    auto [columns, offsets] = _tvf.process(_runtime_state.get(), state);
    ASSERT_OK(state->status());
    ASSERT_EQ(3, columns.size());
    auto* file_col = down_cast<const BinaryColumn*>(columns[0].get());
    auto* row_col = down_cast<const Int64Column*>(columns[1].get());
    auto* raw_col = down_cast<const JsonColumn*>(columns[2].get());

    ASSERT_EQ(1, file_col->size());
    ASSERT_EQ(parquet_path, file_col->get_slice(0).to_string());
    ASSERT_EQ(1, row_col->get_data()[0]);
    // raw_record should be JSON object containing id=2, val=200, name="bob".
    // Use vpack slice API directly so the assertion is independent of whether
    // JsonValue::to_string() pretty-prints with whitespace or not.
    auto* jv = raw_col->get_object(0);
    vpack::Slice obj = jv->to_vslice();
    ASSERT_TRUE(obj.isObject());
    // vpack may store the round-tripped JSON number as either Integer or
    // Double depending on the parser path; isNumber() covers both, and
    // getNumericValue<int64_t> casts safely from either form.
    auto id_field = obj.get("id");
    ASSERT_TRUE(id_field.isNumber());
    EXPECT_EQ(2, id_field.getNumericValue<int64_t>());
    auto val_field = obj.get("val");
    ASSERT_TRUE(val_field.isNumber());
    EXPECT_EQ(200, val_field.getNumericValue<int64_t>());
    auto name_field = obj.get("name");
    ASSERT_TRUE(name_field.isString());
    EXPECT_EQ("bob", name_field.copyString());

    // 1 input → 1 output row.
    ASSERT_EQ(2, offsets->size());
    EXPECT_EQ(0, offsets->get_data()[0]);
    EXPECT_EQ(1, offsets->get_data()[1]);

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- 3 anchors in one process() call to also exercise per-row append ----------
TEST_F(ParquetReadRowsTableFunctionTest, RehydrateMultipleRowsSameFile) {
    std::string parquet_path = _tmp_dir + "/fixture.parquet";
    ASSIGN_OR_ABORT(auto meta, _write_fixture(parquet_path, {1, 2, 3}, {100, 200, 300}, {"alice", "bob", "carol"}));
    int64_t file_size = meta.first;
    int64_t file_mtime_ms = meta.second * 1000;

    auto input = _make_source_info_column({
            _make_anchor(parquet_path, 0, file_size, file_mtime_ms),
            _make_anchor(parquet_path, 2, file_size, file_mtime_ms),
    });

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    auto [columns, offsets] = _tvf.process(_runtime_state.get(), state);
    ASSERT_OK(state->status());

    auto* row_col = down_cast<const Int64Column*>(columns[1].get());
    ASSERT_EQ(2, row_col->size());
    EXPECT_EQ(0, row_col->get_data()[0]);
    EXPECT_EQ(2, row_col->get_data()[1]);

    ASSERT_EQ(3, offsets->size());
    EXPECT_EQ(2, offsets->get_data()[2]);

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- fail-closed: file_size mismatch ----------
TEST_F(ParquetReadRowsTableFunctionTest, SizeMismatchFailsClosed) {
    std::string parquet_path = _tmp_dir + "/fixture.parquet";
    ASSIGN_OR_ABORT(auto meta, _write_fixture(parquet_path, {1, 2, 3}, {100, 200, 300}, {"alice", "bob", "carol"}));
    int64_t actual_size = meta.first;
    int64_t file_mtime_ms = meta.second * 1000;

    // Anchor claims a different size.
    auto input = _make_source_info_column({_make_anchor(parquet_path, 0, actual_size + 7, file_mtime_ms)});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    _tvf.process(_runtime_state.get(), state);
    EXPECT_FALSE(state->status().ok());
    EXPECT_NE(std::string::npos, state->status().to_string().find("size changed"));

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- fail-closed: row_in_file out of range ----------
TEST_F(ParquetReadRowsTableFunctionTest, RowOutOfRangeFailsClosed) {
    std::string parquet_path = _tmp_dir + "/fixture.parquet";
    ASSIGN_OR_ABORT(auto meta, _write_fixture(parquet_path, {1}, {100}, {"alice"}));
    int64_t file_size = meta.first;
    int64_t file_mtime_ms = meta.second * 1000;

    auto input = _make_source_info_column({_make_anchor(parquet_path, 999, file_size, file_mtime_ms)});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    _tvf.process(_runtime_state.get(), state);
    EXPECT_FALSE(state->status().ok());
    EXPECT_NE(std::string::npos, state->status().to_string().find("out of range"));

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- parse error: source_info missing required field ----------
TEST_F(ParquetReadRowsTableFunctionTest, MissingFileFieldRejected) {
    auto input = _make_source_info_column({R"({"row_in_file":0})"});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    _tvf.process(_runtime_state.get(), state);
    EXPECT_FALSE(state->status().ok());
    EXPECT_NE(std::string::npos, state->status().to_string().find("missing required field"));

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- parse error: row_in_file is negative ----------
TEST_F(ParquetReadRowsTableFunctionTest, NegativeRowRejected) {
    auto input =
            _make_source_info_column({R"({"file":"/tmp/x.parquet","row_in_file":-1,"file_size":1,"file_mtime_ms":1})"});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    _tvf.process(_runtime_state.get(), state);
    EXPECT_FALSE(state->status().ok());
    EXPECT_NE(std::string::npos, state->status().to_string().find("non-negative"));

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- parse error: source_info is not a JSON object ----------
TEST_F(ParquetReadRowsTableFunctionTest, NonObjectAnchorRejected) {
    auto input = _make_source_info_column({R"([1,2,3])"});

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    _tvf.process(_runtime_state.get(), state);
    EXPECT_FALSE(state->status().ok());
    EXPECT_NE(std::string::npos, state->status().to_string().find("must be a JSON object"));

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

// ---------- empty input chunk: process() returns 3 empty columns + offset[0]=0 ----------
TEST_F(ParquetReadRowsTableFunctionTest, EmptyInputChunk) {
    auto input = JsonColumn::create();

    TableFunctionState* state = nullptr;
    TFunction fn;
    ASSERT_OK(_tvf.init(fn, &state));
    state->set_params({input});

    auto [columns, offsets] = _tvf.process(_runtime_state.get(), state);
    ASSERT_OK(state->status());
    ASSERT_EQ(3, columns.size());
    EXPECT_EQ(0, columns[0]->size());
    EXPECT_EQ(0, columns[1]->size());
    EXPECT_EQ(0, columns[2]->size());
    ASSERT_EQ(1, offsets->size());
    EXPECT_EQ(0, offsets->get_data()[0]);

    ASSERT_OK(_tvf.close(_runtime_state.get(), state));
}

} // namespace starrocks
