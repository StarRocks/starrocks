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

#include "exprs/table_function/parquet_read_rows.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "types/json_value.h"

namespace starrocks {

namespace {

// Minimal arrow::io::RandomAccessFile that forwards to a StarRocks
// RandomAccessFile. We deliberately do not reuse exec/parquet_reader's
// ParquetChunkFile because that adapter unconditionally dereferences a
// ScannerCounter pointer, which is owned by FileScanNode and not available
// from a TableFunction context.
class ArrowRandomAccessFile final : public arrow::io::RandomAccessFile {
public:
    ArrowRandomAccessFile(std::shared_ptr<RandomAccessFile> file, int64_t size)
            : _file(std::move(file)), _size(size) {}

    arrow::Status Close() override {
        _file.reset();
        return arrow::Status::OK();
    }

    bool closed() const override { return _file == nullptr; }

    arrow::Result<int64_t> Tell() const override { return _pos; }

    arrow::Status Seek(int64_t position) override {
        _pos = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override {
        ARROW_ASSIGN_OR_RAISE(int64_t got, ReadAt(_pos, nbytes, buffer));
        _pos += got;
        return got;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes, arrow::default_memory_pool()));
        ARROW_ASSIGN_OR_RAISE(int64_t got, Read(nbytes, buf->mutable_data()));
        ARROW_RETURN_NOT_OK(buf->Resize(got, /*shrink_to_fit=*/false));
        return std::shared_ptr<arrow::Buffer>(std::move(buf));
    }

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (_file == nullptr) {
            return arrow::Status::IOError("ArrowRandomAccessFile: read after close");
        }
        auto st = _file->read_at_fully(position, out, nbytes);
        if (!st.ok()) {
            return arrow::Status(arrow::StatusCode::IOError, std::string(st.message()));
        }
        return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes, arrow::default_memory_pool()));
        ARROW_ASSIGN_OR_RAISE(int64_t got, ReadAt(position, nbytes, buf->mutable_data()));
        ARROW_RETURN_NOT_OK(buf->Resize(got, /*shrink_to_fit=*/false));
        return std::shared_ptr<arrow::Buffer>(std::move(buf));
    }

    arrow::Result<int64_t> GetSize() override { return _size; }

private:
    std::shared_ptr<RandomAccessFile> _file;
    int64_t _size = 0;
    int64_t _pos = 0;
};

struct Anchor {
    std::string file;
    int64_t row_in_file = 0;
    // -1 sentinel == not provided in source_info. The rejected-records writer
    // omits file_size / file_mtime_ms whenever the upstream scanner did not
    // know them (see arrow_to_starrocks_converter.cpp), so the TVF must
    // tolerate their absence and skip the corresponding fail-closed check.
    int64_t expected_size = -1;
    int64_t expected_mtime_ms = -1;
};

Status parse_anchor(const JsonValue& json, Anchor* out) {
    vpack::Slice slice = json.to_vslice();
    if (!slice.isObject()) {
        return Status::InvalidArgument("parquet_read_rows: source_info must be a JSON object");
    }

    auto get_required_str = [&](const char* key, std::string* dst) -> Status {
        vpack::Slice v = slice.get(key);
        if (v.isNone() || v.isNull()) {
            return Status::InvalidArgument(
                    strings::Substitute("parquet_read_rows: source_info missing required field '$0'", key));
        }
        if (!v.isString()) {
            return Status::InvalidArgument(
                    strings::Substitute("parquet_read_rows: source_info field '$0' must be a string", key));
        }
        *dst = v.copyString();
        return Status::OK();
    };

    auto get_required_int = [&](const char* key, int64_t* dst) -> Status {
        vpack::Slice v = slice.get(key);
        if (v.isNone() || v.isNull()) {
            return Status::InvalidArgument(
                    strings::Substitute("parquet_read_rows: source_info missing required field '$0'", key));
        }
        if (v.isInteger()) {
            *dst = v.getInt();
        } else if (v.isDouble()) {
            *dst = static_cast<int64_t>(v.getDouble());
        } else {
            return Status::InvalidArgument(
                    strings::Substitute("parquet_read_rows: source_info field '$0' must be a number", key));
        }
        return Status::OK();
    };

    auto get_optional_int = [&](const char* key, int64_t* dst) -> Status {
        vpack::Slice v = slice.get(key);
        if (v.isNone() || v.isNull()) {
            return Status::OK();
        }
        if (v.isInteger()) {
            *dst = v.getInt();
            return Status::OK();
        }
        if (v.isDouble()) {
            *dst = static_cast<int64_t>(v.getDouble());
            return Status::OK();
        }
        return Status::InvalidArgument(
                strings::Substitute("parquet_read_rows: source_info field '$0' must be a number when present", key));
    };

    RETURN_IF_ERROR(get_required_str("file", &out->file));
    RETURN_IF_ERROR(get_required_int("row_in_file", &out->row_in_file));
    RETURN_IF_ERROR(get_optional_int("file_size", &out->expected_size));
    RETURN_IF_ERROR(get_optional_int("file_mtime_ms", &out->expected_mtime_ms));
    if (out->row_in_file < 0) {
        return Status::InvalidArgument("parquet_read_rows: row_in_file must be non-negative");
    }
    return Status::OK();
}

void append_cell_value(const arrow::Array& array, int64_t row, rapidjson::Value* out,
                       rapidjson::Document::AllocatorType* alloc) {
    if (array.IsNull(row)) {
        out->SetNull();
        return;
    }
    switch (array.type_id()) {
    case arrow::Type::BOOL:
        out->SetBool(static_cast<const arrow::BooleanArray&>(array).Value(row));
        return;
    case arrow::Type::INT8:
        out->SetInt(static_cast<const arrow::Int8Array&>(array).Value(row));
        return;
    case arrow::Type::INT16:
        out->SetInt(static_cast<const arrow::Int16Array&>(array).Value(row));
        return;
    case arrow::Type::INT32:
        out->SetInt(static_cast<const arrow::Int32Array&>(array).Value(row));
        return;
    case arrow::Type::INT64:
        out->SetInt64(static_cast<const arrow::Int64Array&>(array).Value(row));
        return;
    case arrow::Type::UINT8:
        out->SetUint(static_cast<const arrow::UInt8Array&>(array).Value(row));
        return;
    case arrow::Type::UINT16:
        out->SetUint(static_cast<const arrow::UInt16Array&>(array).Value(row));
        return;
    case arrow::Type::UINT32:
        out->SetUint(static_cast<const arrow::UInt32Array&>(array).Value(row));
        return;
    case arrow::Type::UINT64:
        out->SetUint64(static_cast<const arrow::UInt64Array&>(array).Value(row));
        return;
    case arrow::Type::FLOAT:
        out->SetDouble(static_cast<const arrow::FloatArray&>(array).Value(row));
        return;
    case arrow::Type::DOUBLE:
        out->SetDouble(static_cast<const arrow::DoubleArray&>(array).Value(row));
        return;
    case arrow::Type::STRING: {
        const auto& a = static_cast<const arrow::StringArray&>(array);
        auto view = a.GetView(row);
        out->SetString(view.data(), static_cast<rapidjson::SizeType>(view.size()), *alloc);
        return;
    }
    case arrow::Type::BINARY: {
        const auto& a = static_cast<const arrow::BinaryArray&>(array);
        auto view = a.GetView(row);
        // Binary data may not be valid UTF-8; emit as a JSON string of raw bytes.
        // Consumers needing structured access should add explicit casts on top.
        out->SetString(view.data(), static_cast<rapidjson::SizeType>(view.size()), *alloc);
        return;
    }
    case arrow::Type::LARGE_STRING: {
        const auto& a = static_cast<const arrow::LargeStringArray&>(array);
        auto view = a.GetView(row);
        out->SetString(view.data(), static_cast<rapidjson::SizeType>(view.size()), *alloc);
        return;
    }
    case arrow::Type::LARGE_BINARY: {
        const auto& a = static_cast<const arrow::LargeBinaryArray&>(array);
        auto view = a.GetView(row);
        out->SetString(view.data(), static_cast<rapidjson::SizeType>(view.size()), *alloc);
        return;
    }
    default: {
        // Fallback: render via arrow's stringifier and embed as a JSON string.
        // Covers decimal/timestamp/date/list/struct/map/union and friends in a
        // human-readable form; downstream callers that need typed access should
        // re-cast from the JSON envelope.
        auto sliced = array.Slice(row, 1);
        std::string s = sliced->ToString();
        out->SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()), *alloc);
        return;
    }
    }
}

std::string row_to_json(const arrow::Table& table, int64_t row) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();
    const auto& schema = table.schema();
    for (int c = 0; c < table.num_columns(); ++c) {
        const auto& col = table.column(c);
        int64_t local_row = row;
        const arrow::Array* arr = nullptr;
        for (int chk = 0; chk < col->num_chunks(); ++chk) {
            const auto& chunk = col->chunk(chk);
            if (local_row < chunk->length()) {
                arr = chunk.get();
                break;
            }
            local_row -= chunk->length();
        }
        rapidjson::Value v;
        if (arr == nullptr) {
            v.SetNull();
        } else {
            append_cell_value(*arr, local_row, &v, &alloc);
        }
        const auto& name = schema->field(c)->name();
        rapidjson::Value k(name.c_str(), static_cast<rapidjson::SizeType>(name.size()), alloc);
        doc.AddMember(k, v, alloc);
    }
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);
    return std::string(buf.GetString(), buf.GetSize());
}

Status rehydrate_one(const Anchor& anchor, std::string* out_json) {
    // 1. Resolve filesystem and decide on the file size to pass to arrow.
    //    Several backends (S3/OSS, Starlet) do not implement get_file_size or
    //    get_file_modified_time — when they don't, fall back to the anchor's
    //    embedded size and skip the corresponding check rather than failing.
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(anchor.file));

    int64_t file_size = -1;
    auto cur_size_or = fs->get_file_size(anchor.file);
    if (cur_size_or.ok()) {
        file_size = static_cast<int64_t>(cur_size_or.value());
        if (anchor.expected_size >= 0 && file_size != anchor.expected_size) {
            return Status::Corruption(strings::Substitute(
                    "parquet_read_rows: file '$0' size changed (expected=$1, actual=$2); fail-closed",
                    anchor.file, anchor.expected_size, file_size));
        }
    } else if (cur_size_or.status().is_not_supported()) {
        // FS can't tell us; rely on anchor.
        if (anchor.expected_size < 0) {
            return Status::InvalidArgument(strings::Substitute(
                    "parquet_read_rows: filesystem does not expose file size for '$0' and source_info has no "
                    "file_size; cannot open file safely",
                    anchor.file));
        }
        file_size = anchor.expected_size;
    } else {
        return cur_size_or.status();
    }

    // mtime check is best-effort: POSIX returns seconds, but several backends
    // do not implement get_file_modified_time. Treat NotSupported as "cannot
    // verify, proceed" — the size check above already catches the most common
    // drift. When a backend does return a value, the anchor's millisecond
    // resolution is matched against current_seconds * 1000 (Hadoop FileStatus
    // convention used at write time).
    if (anchor.expected_mtime_ms >= 0) {
        auto cur_mtime_or = fs->get_file_modified_time(anchor.file);
        if (cur_mtime_or.ok()) {
            int64_t cur_mtime_ms = static_cast<int64_t>(cur_mtime_or.value()) * 1000;
            if (cur_mtime_ms != anchor.expected_mtime_ms) {
                return Status::Corruption(strings::Substitute(
                        "parquet_read_rows: file '$0' mtime changed (expected_ms=$1, actual_ms=$2); fail-closed",
                        anchor.file, anchor.expected_mtime_ms, cur_mtime_ms));
            }
        } else if (!cur_mtime_or.status().is_not_supported()) {
            return cur_mtime_or.status();
        }
    }

    // 2. Open file + adapt to arrow IO.
    ASSIGN_OR_RETURN(auto raf, fs->new_random_access_file(anchor.file));
    auto shared_raf = std::shared_ptr<RandomAccessFile>(std::move(raf));
    auto arrow_file = std::make_shared<ArrowRandomAccessFile>(shared_raf, file_size);

    // 3. Open parquet metadata.
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<parquet::FileMetaData> file_metadata;
    try {
        parquet::ReaderProperties pq_props(arrow::default_memory_pool());
        auto pq_reader = parquet::ParquetFileReader::Open(arrow_file, pq_props);
        file_metadata = pq_reader->metadata();
        auto open_st = parquet::arrow::FileReader::Make(arrow::default_memory_pool(), std::move(pq_reader),
                                                       parquet::ArrowReaderProperties(), &reader);
        if (!open_st.ok()) {
            return Status::IOError(strings::Substitute("parquet_read_rows: failed to open parquet '$0': $1",
                                                       anchor.file, open_st.ToString()));
        }
    } catch (const parquet::ParquetException& e) {
        return Status::IOError(
                strings::Substitute("parquet_read_rows: parquet exception on '$0': $1", anchor.file, e.what()));
    }

    int64_t total_rows = file_metadata->num_rows();
    if (anchor.row_in_file >= total_rows) {
        return Status::InvalidArgument(strings::Substitute(
                "parquet_read_rows: row_in_file=$0 out of range (total_rows=$1) for file '$2'",
                anchor.row_in_file, total_rows, anchor.file));
    }

    // 4. Locate the row group that contains this anchor.
    int64_t cumulative = 0;
    int row_group_id = -1;
    int64_t row_in_group = 0;
    for (int rg = 0; rg < file_metadata->num_row_groups(); ++rg) {
        int64_t rg_rows = file_metadata->RowGroup(rg)->num_rows();
        if (anchor.row_in_file < cumulative + rg_rows) {
            row_group_id = rg;
            row_in_group = anchor.row_in_file - cumulative;
            break;
        }
        cumulative += rg_rows;
    }
    if (row_group_id < 0) {
        return Status::InternalError(strings::Substitute(
                "parquet_read_rows: could not locate row group for row_in_file=$0 in '$1'",
                anchor.row_in_file, anchor.file));
    }

    // 5. Read just that row group.
    std::shared_ptr<arrow::Table> table;
    try {
        auto read_st = reader->ReadRowGroup(row_group_id, &table);
        if (!read_st.ok()) {
            return Status::IOError(strings::Substitute("parquet_read_rows: read row group $0 of '$1' failed: $2",
                                                       row_group_id, anchor.file, read_st.ToString()));
        }
    } catch (const parquet::ParquetException& e) {
        return Status::IOError(strings::Substitute("parquet_read_rows: parquet exception reading row group $0 of '$1': $2",
                                                   row_group_id, anchor.file, e.what()));
    }
    if (row_in_group >= table->num_rows()) {
        return Status::InternalError(strings::Substitute(
                "parquet_read_rows: row group $0 of '$1' has $2 rows, expected at least $3",
                row_group_id, anchor.file, table->num_rows(), row_in_group + 1));
    }

    // 6. Serialize the target row to a JSON object keyed by column name.
    *out_json = row_to_json(*table, row_in_group);
    return Status::OK();
}

} // namespace

Status ParquetReadRows::init(const TFunction& /*fn*/, TableFunctionState** state) const {
    *state = new TableFunctionState();
    return Status::OK();
}

Status ParquetReadRows::prepare(TableFunctionState* /*state*/) const {
    return Status::OK();
}

Status ParquetReadRows::open(RuntimeState* /*runtime_state*/, TableFunctionState* /*state*/) const {
    return Status::OK();
}

Status ParquetReadRows::close(RuntimeState* /*runtime_state*/, TableFunctionState* state) const {
    delete state;
    return Status::OK();
}

std::pair<Columns, UInt32Column::Ptr> ParquetReadRows::process(RuntimeState* /*runtime_state*/,
                                                               TableFunctionState* state) const {
    auto offset_col = UInt32Column::create();
    Columns result;
    auto file_col = BinaryColumn::create();
    auto row_col = Int64Column::create();
    auto raw_col = JsonColumn::create();
    result.emplace_back(file_col);
    result.emplace_back(row_col);
    result.emplace_back(raw_col);

    offset_col->append(0);

    if (state->get_columns().empty()) {
        return std::make_pair(std::move(result), std::move(offset_col));
    }

    auto& arg0 = state->get_columns()[0];
    size_t num_input_rows = arg0->size();
    state->set_processed_rows(num_input_rows);

    auto* json_column = down_cast<JsonColumn*>(ColumnHelper::get_data_column(arg0->as_mutable_raw_ptr()));

    int64_t max_anchors = config::parquet_read_rows_max_anchors;
    if (max_anchors > 0 && static_cast<int64_t>(num_input_rows) > max_anchors) {
        state->set_status(Status::InvalidArgument(strings::Substitute(
                "parquet_read_rows: anchor count $0 exceeds parquet_read_rows_max_anchors=$1",
                num_input_rows, max_anchors)));
        for (size_t i = 0; i < num_input_rows; ++i) {
            offset_col->append(0);
        }
        return std::make_pair(std::move(result), std::move(offset_col));
    }

    uint32_t cumulative = 0;
    for (size_t i = 0; i < num_input_rows; ++i) {
        if (arg0->is_null(i)) {
            state->set_status(Status::InvalidArgument(
                    "parquet_read_rows: source_info is NULL; cannot rehydrate"));
            offset_col->append(cumulative);
            continue;
        }

        const JsonValue* jv = json_column->get_object(i);
        if (jv == nullptr) {
            state->set_status(Status::InvalidArgument(
                    "parquet_read_rows: source_info JSON is unreadable"));
            offset_col->append(cumulative);
            continue;
        }

        Anchor anchor;
        Status pst = parse_anchor(*jv, &anchor);
        if (!pst.ok()) {
            state->set_status(pst);
            offset_col->append(cumulative);
            continue;
        }

        std::string out_json;
        Status rst = rehydrate_one(anchor, &out_json);
        if (!rst.ok()) {
            state->set_status(rst);
            offset_col->append(cumulative);
            continue;
        }

        auto parsed = JsonValue::parse(Slice(out_json));
        if (!parsed.ok()) {
            state->set_status(parsed.status());
            offset_col->append(cumulative);
            continue;
        }

        file_col->append(Slice(anchor.file));
        row_col->append(anchor.row_in_file);
        raw_col->append(std::move(parsed).value());
        cumulative += 1;
        offset_col->append(cumulative);
    }

    return std::make_pair(std::move(result), std::move(offset_col));
}

} // namespace starrocks
