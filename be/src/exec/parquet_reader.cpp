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

#include "exec/parquet_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <gutil/strings/substitute.h>

#include <memory>
#include <utility>

#include "common/config_scan_io_fwd.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "exec/file_scanner/file_scanner.h"
#include "fmt/format.h"
#include "parquet/arrow/schema.h"
#include "parquet/schema.h"
#include "parquet_schema_builder.h"
#include "runtime/byte_buffer.h"
#include "runtime/descriptors.h"

namespace starrocks {
// ====================================================================================================================

namespace {

// Recursively re-tag ONLY the timezone-naive TIMESTAMP leaves that originate from an INT96 parquet
// column as UTC, walking the Arrow ArrayData tree in lockstep with the parquet SchemaField tree.
// The two trees are isomorphic by child index (STRUCT: child i <-> field i; LIST / LARGE_LIST /
// FIXED_SIZE_LIST: the single element child; MAP: Arrow's struct<key,value> entries <-> the
// SchemaField key_value group), so we descend both together and match strictly by child index --
// never by field name, because Arrow does not rename list/map child fields to canonical names on
// read. A sibling INT64 isAdjustedToUTC=false (wall-clock) leaf is therefore left untouched even
// when it shares a complex column with an INT96 leaf. The enclosing LIST/.../STRUCT/MAP type
// metadata is rebuilt bottom-up; ArrayData::Copy() is a shallow copy, so the underlying
// value/offset/validity buffers are aliased unchanged -- this is a metadata-only rewrite with no
// per-row cost. Returns the (possibly) rewritten ArrayData and sets `*changed` when any leaf was
// re-tagged; otherwise returns `data` untouched.
std::shared_ptr<arrow::ArrayData> rectify_int96_array_data(const std::shared_ptr<arrow::ArrayData>& data,
                                                           const ::parquet::arrow::SchemaField& field,
                                                           const ::parquet::SchemaDescriptor& descr, bool* changed) {
    if (field.is_leaf()) {
        // INT96 is a UTC-normalized instant that Arrow decodes timezone-naive. Tag it as UTC so the
        // downstream converter overrides it with the session timezone and shifts the value (matching
        // the native parquet reader's Int96ToDateTimeConverter). A genuinely naive INT64
        // isAdjustedToUTC=false leaf has a non-INT96 physical type and is left untouched so it stays
        // wall-clock.
        if (data->type->id() == arrow::Type::TIMESTAMP &&
            descr.Column(field.column_index)->physical_type() == ::parquet::Type::INT96) {
            auto ts_type = std::static_pointer_cast<arrow::TimestampType>(data->type);
            if (ts_type->timezone().empty()) {
                auto new_data = data->Copy();
                new_data->type = arrow::timestamp(ts_type->unit(), "UTC");
                *changed = true;
                return new_data;
            }
        }
        return data;
    }

    // Interior node: recurse children in lockstep with the SchemaField children. Bail out if the
    // structures somehow disagree (should not happen for parquet::arrow-produced trees).
    if (field.children.size() != data->child_data.size()) {
        return data;
    }
    bool child_changed = false;
    std::vector<std::shared_ptr<arrow::ArrayData>> new_children;
    new_children.reserve(data->child_data.size());
    for (size_t i = 0; i < data->child_data.size(); ++i) {
        new_children.push_back(rectify_int96_array_data(data->child_data[i], field.children[i], descr, &child_changed));
    }
    if (!child_changed) {
        return data;
    }
    // Rebuild the enclosing type from the re-tagged child types, preserving child field names and
    // nullability via the field-taking Arrow factories.
    const auto& type = data->type;
    std::shared_ptr<arrow::DataType> new_type;
    switch (type->id()) {
    case arrow::Type::LIST:
        new_type = arrow::list(type->field(0)->WithType(new_children[0]->type));
        break;
    // LARGE_LIST and FIXED_SIZE_LIST are handled so this stays a complete Arrow ArrayData
    // re-tagger, but the parquet->Arrow read path that currently calls it never produces them:
    // parquet repeated groups map to LIST by default (the reader does not request
    // list_type=LARGE_LIST), and parquet has no fixed-size-list concept. They are kept for forward
    // compatibility (e.g. if a future reader enables LARGE_LIST for very large array columns) and
    // are therefore not exercised by the current tests.
    case arrow::Type::LARGE_LIST:
        new_type = arrow::large_list(type->field(0)->WithType(new_children[0]->type));
        break;
    case arrow::Type::FIXED_SIZE_LIST: {
        auto list_type = std::static_pointer_cast<arrow::FixedSizeListType>(type);
        new_type = arrow::fixed_size_list(type->field(0)->WithType(new_children[0]->type), list_type->list_size());
        break;
    }
    case arrow::Type::MAP: {
        // A MapArray's single child is a struct<key, value>; the recursion above already rebuilt it
        // as new_children[0]. Derive the key type and (possibly re-tagged) value field from that
        // struct so keys()/items() see the rewritten types.
        auto map_type = std::static_pointer_cast<arrow::MapType>(type);
        auto entries = std::static_pointer_cast<arrow::StructType>(new_children[0]->type);
        new_type = arrow::map(entries->field(0)->type(), entries->field(1), map_type->keys_sorted());
        break;
    }
    case arrow::Type::STRUCT: {
        auto struct_type = std::static_pointer_cast<arrow::StructType>(type);
        arrow::FieldVector new_fields;
        new_fields.reserve(struct_type->num_fields());
        for (int j = 0; j < struct_type->num_fields(); ++j) {
            new_fields.push_back(struct_type->field(j)->WithType(new_children[j]->type));
        }
        new_type = arrow::struct_(new_fields);
        break;
    }
    default:
        return data; // not a nested type we rebuild; leave untouched
    }
    auto new_data = data->Copy();
    new_data->type = std::move(new_type);
    new_data->child_data = std::move(new_children);
    *changed = true;
    return new_data;
}

} // namespace

ParquetReaderWrap::~ParquetReaderWrap() {
    close();
}

ParquetReaderWrap::ParquetReaderWrap(std::shared_ptr<arrow::io::RandomAccessFile>&& parquet_file,
                                     int32_t num_of_columns_from_file, int64_t read_offset, int64_t read_size)

        : _num_of_columns_from_file(num_of_columns_from_file),

          _read_offset(read_offset),
          _read_size(read_size) {
    _parquet = std::move(parquet_file);
    _properties = ::parquet::ReaderProperties();
    _filename = (reinterpret_cast<ParquetChunkFile*>(_parquet.get()))->filename();
}

Status ParquetReaderWrap::next_selected_row_group() {
    while (_current_group < _total_groups) {
        int64_t row_group_start = _file_metadata->RowGroup(_current_group)->file_offset();

        if (_read_size == 0) {
            return Status::EndOfFile("End of row group");
        }

        int64_t scan_start = _read_offset;
        int64_t scan_end = _read_size + scan_start;

        if (row_group_start >= scan_start && row_group_start < scan_end) {
            return Status::OK();
        }

        _current_group++;
    }

    return Status::EndOfFile("End of row group");
}

Status ParquetReaderWrap::_init_parquet_reader() {
    try {
        ::parquet::ArrowReaderProperties arrow_reader_properties;
        /*
        * timestamp unit to use for INT96-encoded timestamps in parquet.
        * SECOND, MICRO, MILLI, NANO
        * We use MICRO second as the unit to parse int96 timestamp, which is the precision of DATETIME/TIMESTAMP in MySQL.
        * https://dev.mysql.com/doc/refman/8.0/en/datetime.html
        * A DATETIME or TIMESTAMP value can include a trailing fractional seconds part in up to microseconds (6 digits) precision
        */
        arrow_reader_properties.set_coerce_int96_timestamp_unit(arrow::TimeUnit::MICRO);

        // arrow default batch size is 64K,
        // the bigger batch size, the more memory it uses
        arrow_reader_properties.set_batch_size(config::arrow_read_batch_size);

        // io coalesce
        // performance test 0: tpcds store_sales, 23 columns, 649M, 7218819 lines
        //                  | file read time | file read count | memory
        // io coalesce 8M   | 13s            |   80            | 587M
        // buffer stream 8M | 15s            |   176           | 1313M
        // buffer stream 1M | 29s            |   1145          | 1157M
        //
        // performance test 1: 1001 columns table, 147M, 50000 lines
        //                  | file read time | file read count | memory
        // io coalesce 8M   | 3s             |   20            | 1G
        // buffer stream 8M | 15s            |   1003          | 10.1G
        // buffer stream 1M | 15s            |   1003          | 3.3G
        //
        arrow_reader_properties.set_pre_buffer(true);
        auto cache_options = arrow::io::CacheOptions::LazyDefaults();
        cache_options.hole_size_limit = config::arrow_io_coalesce_read_max_distance_size;
        cache_options.range_size_limit = config::arrow_io_coalesce_read_max_buffer_size;
        arrow_reader_properties.set_cache_options(cache_options);

        // new file reader for parquet file
        auto st = ::parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                                     ::parquet::ParquetFileReader::Open(_parquet, _properties),
                                                     arrow_reader_properties, &_reader);
        if (!st.ok()) {
            std::ostringstream oss;
            oss << "Failed to create parquet file reader. error: " << st.ToString() << ", filename: " << _filename;
            LOG(INFO) << oss.str();
            return Status::InternalError(oss.str());
        }

        if (!_reader || !_reader->parquet_reader()) {
            LOG(INFO) << "Ignore the parquet file because of unexpected nullptr ParquetReader";
            return Status::EndOfFile("Unexpected nullptr ParquetReader");
        }
        _file_metadata = _reader->parquet_reader()->metadata();
        if (!_file_metadata) {
            LOG(INFO) << "Ignore the parquet file because of unexpected nullptr FileMetaData";
            return Status::EndOfFile("Unexpected nullptr FileMetaData");
        }

        _num_rows = _file_metadata->num_rows();
        // initial members
        _total_groups = _file_metadata->num_row_groups();
        if (_total_groups == 0) {
            return Status::EndOfFile("Empty Parquet File");
        }
        RETURN_IF_ERROR(next_selected_row_group());

        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows();

        {
            // Initialize _map_column, map column name to it's index
            // For nested type, it has multiple column, we need to map field_name to multiple indices
            auto parquet_schema = _file_metadata->schema();
            for (int i = 0; i < parquet_schema->num_columns(); i++) {
                auto column_desc = parquet_schema->Column(i);
                const auto dot_vector = column_desc->path()->ToDotVector();
                const std::string& field_name = dot_vector[0];
                _map_column_nested[field_name].push_back(i);
                // Record the top-level column whenever any of its leaves is INT96 (including leaves
                // nested inside ARRAY/MAP/STRUCT, since dot_vector[0] is the top-level column name).
                // This is only a coarse filter so _rectify_int96_timezone() can skip columns with no
                // INT96 leaf; the precise per-leaf re-tag (which leaf is INT96 vs an INT64
                // wall-clock sibling) is resolved there via the FileReader schema manifest.
                if (column_desc->physical_type() == ::parquet::Type::INT96) {
                    _int96_columns.insert(field_name);
                }
            }
        }

        return Status::OK();
    } catch (::parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what() << ", filename: " << _filename;
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
}

Status ParquetReaderWrap::init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    RETURN_IF_ERROR(_init_parquet_reader());
    try {
        if (_current_line_of_group == 0) { // the first read
            RETURN_IF_ERROR(column_indices(tuple_slot_descs));
            arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);

            if (!status.ok()) {
                LOG(WARNING) << "Get RecordBatch Failed. error: " << status.ToString() << ", filename: " << _filename;
                return Status::InternalError(fmt::format("{}. filename: {}", status.ToString(), _filename));
            }
            if (!_rb_batch) {
                LOG(INFO) << "Ignore the parquet file because of an unexpected nullptr "
                             "RecordBatchReader";
                return Status::EndOfFile("Unexpected nullptr RecordBatchReader");
            }
            status = _rb_batch->ReadNext(&_batch);
            if (!status.ok()) {
                LOG(WARNING) << "The first read record. error: " << status.ToString() << ", filename: " << _filename;
                return Status::InternalError(fmt::format("{}. filename: {}", status.ToString(), _filename));
            }
            if (!_batch) {
                LOG(INFO) << "Ignore the parquet file because of an expected nullptr RecordBatch";
                return Status::EndOfFile("Unexpected nullptr RecordBatch");
            }
            _current_line_of_batch = 0;
            //save column type
            std::shared_ptr<arrow::Schema> field_schema = _batch->schema();
            if (!field_schema) {
                LOG(INFO) << "Ignore the parquet file because of an expected nullptr Schema";
                return Status::EndOfFile("Unexpected nullptr RecordBatch");
            }
        }
        return Status::OK();
    } catch (::parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what();
        LOG(WARNING) << str_error.str() << " filename: " << _filename;
        return Status::InternalError(fmt::format("{}. filename: {}", str_error.str(), _filename));
    }
}

Status ParquetReaderWrap::get_schema(std::vector<SlotDescriptor>* schema) {
    auto st = _init_parquet_reader();
    // Initializing a reader on empty Parquet files (with 0 row groups) returns EOF,
    // but the file schema is still available
    if (!st.ok() && !(st.is_end_of_file() && _file_metadata != nullptr)) {
        return st;
    }

    const auto& file_schema = _file_metadata->schema();

    for (auto i = 0; i < file_schema->group_node()->field_count(); i++) {
        const auto& field = file_schema->group_node()->field(i);
        const auto& name = field->name();

        TypeDescriptor tp;
        RETURN_IF_ERROR(get_parquet_type(field, &tp));

        schema->emplace_back(i, name, tp);
    }

    return Status::OK();
}

void ParquetReaderWrap::close() {
    [[maybe_unused]] auto st = _parquet->Close();
}

Status ParquetReaderWrap::size(int64_t* size) {
    auto size_res = _parquet->GetSize();
    if (!size_res.ok()) {
        return Status::InternalError(size_res.status().ToString());
    }
    *size = size_res.ValueOrDie();
    return Status::OK();
}

Status ParquetReaderWrap::column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    _parquet_column_ids.clear();
    for (int i = 0; i < _num_of_columns_from_file; i++) {
        auto* slot_desc = tuple_slot_descs.at(i);
        if (slot_desc == nullptr) {
            continue;
        }
        std::string col_name(slot_desc->col_name());

        auto iter = _map_column_nested.find(col_name);
        if (iter != _map_column_nested.end()) {
            for (auto index : iter->second) {
                _parquet_column_ids.emplace_back(index);
            }
        } else if (!_invalid_as_null) {
            std::stringstream str_error;
            str_error << "Column: " << slot_desc->col_name() << " is not found in file: " << _filename;
            LOG(WARNING) << str_error.str();
            return Status::NotFound(str_error.str());
        }
    }
    return Status::OK();
}

Status ParquetReaderWrap::read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) {
    if (_current_line_of_group >= _rows_of_group) { // read next row group
        VLOG(7) << "read_record_batch, current group id:" << _current_group
                << " current line of group:" << _current_line_of_group
                << " is larger than rows group size:" << _rows_of_group << ". start to read next row group";
        _current_group++;
        auto st = next_selected_row_group();
        if (!st.ok()) { // read completed.
            _parquet_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows(); //get rows of the current row group
        // read batch
        arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Get RecordBatchReader Failed. filename: {}", _filename));
        }
        status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Read Batch Error With Libarrow. filename: {}", _filename));
        }

        // arrow::RecordBatchReader::ReadNext returns null at end of stream.
        // Since we count the batches read, EOF implies reader source failure.
        if (_batch == nullptr) {
            LOG(WARNING) << "Unexpected EOF. Row groups less than expected. expected: " << _total_groups
                         << " got: " << _current_group << ", filename" << _filename;
            return Status::InternalError(fmt::format("Unexpected EOF. filename: {}", _filename));
        }

        _current_line_of_batch = 0;
    } else if (_current_line_of_batch >= _batch->num_rows()) {
        VLOG(7) << "read_record_batch, current group id:" << _current_group
                << " current line of batch:" << _current_line_of_batch
                << " is larger than batch size:" << _batch->num_rows() << ". start to read next batch";
        arrow::Status status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError(fmt::format("Read Batch Error With Libarrow. filename: {}", _filename));
        }

        // arrow::RecordBatchReader::ReadNext returns null at end of stream.
        // Since we count the batches read, EOF implies reader source failure.
        if (_batch == nullptr) {
            LOG(WARNING) << "Unexpected EOF. Row groups less than expected. expected: " << _total_groups
                         << " got: " << _current_group << ", filename: " << _filename;
            return Status::InternalError(fmt::format("Unexpected EOF. filename: {}", _filename));
        }

        _current_line_of_batch = 0;
    }
    return Status::OK();
}

void ParquetReaderWrap::_rectify_int96_timezone() {
    // _batch is guaranteed non-null by the caller: get_batch() is only reached after a
    // successful read_record_batch()/next_batch(), which always populates _batch.
    //
    // _int96_columns is a coarse filter: it holds every top-level column that contains at least one
    // INT96 leaf, so columns with no INT96 leaf are skipped entirely (zero overhead). The precise
    // per-leaf decision -- re-tag only genuine INT96 leaves, leaving any sibling INT64 wall-clock
    // leaf untouched -- is made by rectify_int96_array_data() while it walks the parquet
    // SchemaField tree (from the FileReader manifest) in lockstep with the column's ArrayData tree.
    if (_int96_columns.empty()) {
        return;
    }
    // Why SchemaManifest: Arrow decodes INT96 and INT64 isAdjustedToUTC=false to the SAME
    // timezone-naive timestamp type, so the decoded ArrayData alone cannot tell which leaf came
    // from INT96. The parquet SchemaDescriptor still records each leaf's physical type, but it is
    // indexed by flat leaf-column index whereas the data is a nested tree. SchemaManifest is Arrow's
    // bridge between the two: its SchemaField tree is isomorphic (by child index) to the produced
    // Arrow field/array tree, and every leaf SchemaField carries the parquet column_index. Walking
    // the manifest in lockstep with the ArrayData tree therefore lets us re-tag exactly the genuine
    // INT96 leaves and nothing else (a top-level-column-name set cannot, because one nested column
    // can contain both an INT96 leaf and an INT64 wall-clock leaf).
    const ::parquet::arrow::SchemaManifest& manifest = _reader->manifest();
    const ::parquet::SchemaDescriptor* descr = _file_metadata->schema();

    auto fields = _batch->schema()->fields();
    auto columns = _batch->columns();
    bool changed = false;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (_int96_columns.find(fields[i]->name()) == _int96_columns.end()) {
            continue;
        }
        // Locate the matching top-level parquet SchemaField by top-level column name. Matching by
        // name is safe here because this is the user's column name, not an Arrow-synthesized
        // list/map child name; the descent below is purely structural (by child index).
        const ::parquet::arrow::SchemaField* schema_field = nullptr;
        for (const auto& sf : manifest.schema_fields) {
            if (sf.field->name() == fields[i]->name()) {
                schema_field = &sf;
                break;
            }
        }
        if (schema_field == nullptr) {
            continue;
        }
        bool column_changed = false;
        auto new_data = rectify_int96_array_data(columns[i]->data(), *schema_field, *descr, &column_changed);
        if (column_changed) {
            columns[i] = arrow::MakeArray(new_data);
            fields[i] = fields[i]->WithType(new_data->type);
            changed = true;
        }
    }
    if (changed) {
        _batch = arrow::RecordBatch::Make(arrow::schema(fields), _batch->num_rows(), columns);
    }
}

const std::shared_ptr<arrow::RecordBatch>& ParquetReaderWrap::get_batch() {
    _rectify_int96_timezone();
    _current_line_of_batch += _batch->num_rows();
    _current_line_of_group += _batch->num_rows();
    return _batch;
}

// ====================================================================================================================
ParquetChunkReader::ParquetChunkReader(std::shared_ptr<ParquetReaderWrap>&& parquet_reader,
                                       const std::vector<SlotDescriptor*>& src_slot_desc, std::string time_zone)
        : _parquet_reader(std::move(parquet_reader)),
          _src_slot_descs(src_slot_desc),
          _time_zone(std::move(time_zone)) {}

ParquetChunkReader::~ParquetChunkReader() {
    _parquet_reader->close();
}

int64_t ParquetChunkReader::total_num_rows() const {
    return _parquet_reader->num_rows();
}

Status ParquetChunkReader::next_batch(RecordBatchPtr* batch) {
    switch (_state) {
    case State::UNINITIALIZED: {
        RETURN_IF_ERROR(_parquet_reader->init_parquet_reader(_src_slot_descs));
        _state = INITIALIZED;
        break;
    }
    case State::INITIALIZED: {
        bool eof = false;
        auto status = _parquet_reader->read_record_batch(_src_slot_descs, &eof);
        if (status.is_end_of_file() || eof) {
            *batch = nullptr;
            _state = END_OF_FILE;
            return Status::EndOfFile(Slice());
        } else if (!status.ok()) {
            return status;
        }
        break;
    }
    case State::END_OF_FILE: {
        *batch = nullptr;
        return Status::EndOfFile(Slice());
    }
    }
    *batch = _parquet_reader->get_batch();
    return Status::OK();
}

Status ParquetChunkReader::get_schema(std::vector<SlotDescriptor>* schema) {
    RETURN_IF_ERROR(_parquet_reader->init_parquet_reader(_src_slot_descs));
    return _parquet_reader->get_schema(schema);
}

using StarRocksStatusCode = ::starrocks::TStatusCode::type;
using ArrowStatusCode = ::arrow::StatusCode;
using StarRocksStatus = ::starrocks::Status;
using ArrowStatus = ::arrow::Status;

ParquetChunkFile::ParquetChunkFile(std::shared_ptr<starrocks::RandomAccessFile> file, uint64_t pos,
                                   ScannerCounter* counter)
        : _file(std::move(file)), _pos(pos), _counter(counter) {}

ParquetChunkFile::~ParquetChunkFile() {
    [[maybe_unused]] auto st = Close();
}

arrow::Status ParquetChunkFile::Close() {
    _file.reset();
    return ArrowStatus::OK();
}

bool ParquetChunkFile::closed() const {
    return false;
}

arrow::Result<int64_t> ParquetChunkFile::Read(int64_t nbytes, void* buffer) {
    return ReadAt(_pos, nbytes, buffer);
}

arrow::Result<int64_t> ParquetChunkFile::ReadAt(int64_t position, int64_t nbytes, void* out) {
    _pos += nbytes;
    ++_counter->file_read_count;
    SCOPED_RAW_TIMER(&_counter->file_read_ns);
    auto status = _file->read_at_fully(position, out, nbytes);
    return status.ok()
                   ? nbytes
                   : arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, std::string(status.message())));
}

arrow::Result<int64_t> ParquetChunkFile::GetSize() {
    const StatusOr<uint64_t> status_or = _file->get_size();
    return status_or.ok() ? status_or.value()
                          : arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError,
                                                                 std::string(status_or.status().message())));
}

arrow::Status ParquetChunkFile::Seek(int64_t position) {
    _pos = position;
    return ArrowStatus::OK();
}

arrow::Result<int64_t> ParquetChunkFile::Tell() const {
    return _pos;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ParquetChunkFile::Read(int64_t nbytes) {
    auto tracker = CurrentThread::mem_tracker();
    if (tracker == nullptr) {
        return arrow::Status::ExecutionError("current thread memory tracker Not Found when allocate arrow Buffer");
    }
    std::unique_ptr<arrow::Buffer> buffer_res;
    ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(nbytes, arrow::default_memory_pool()).Value(&buffer_res));
    std::shared_ptr<arrow::Buffer> read_buf(buffer_res.release(), MemTrackerDeleter(tracker));
    int64_t bytes_read_res = 0;
    ARROW_RETURN_NOT_OK(ReadAt(_pos, nbytes, read_buf->mutable_data()).Value(&bytes_read_res));
    // If bytes_read is equal with read_buf's capacity, we just assign
    if (bytes_read_res == nbytes) {
        return std::move(read_buf);
    } else {
        std::shared_ptr<arrow::Buffer> slice_buf(new arrow::Buffer(read_buf, 0, bytes_read_res),
                                                 MemTrackerDeleter(tracker));
        return slice_buf;
    }
}

const std::string& ParquetChunkFile::filename() const {
    return _file->filename();
}

} // namespace starrocks
