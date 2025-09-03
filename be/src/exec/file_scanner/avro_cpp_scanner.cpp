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

#include "exec/file_scanner/avro_cpp_scanner.h"

#include <fmt/format.h>

#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "exprs/column_ref.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"

namespace starrocks {

AvroCppScanner::AvroCppScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                               ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _max_chunk_size(state->chunk_size()) {}

Status AvroCppScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());

    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    // check column from path
    const auto& first_range = _scan_range.ranges[0];
    if (!first_range.__isset.num_of_columns_from_file) {
        return Status::InternalError("'num_of_columns_from_file' not set");
    }

    for (const auto& rng : _scan_range.ranges) {
        if (rng.columns_from_path.size() != first_range.columns_from_path.size()) {
            return Status::InvalidArgument("Path column count of range mismatch");
        }
        if (rng.num_of_columns_from_file != first_range.num_of_columns_from_file) {
            return Status::InvalidArgument("Column count from file of range mismatch");
        }
        if (rng.num_of_columns_from_file + rng.columns_from_path.size() != _src_slot_descriptors.size()) {
            return Status::InvalidArgument("Slot descriptor and column count mismatch");
        }
    }

    _num_of_columns_from_file = first_range.num_of_columns_from_file;
    for (int i = _num_of_columns_from_file; i < _src_slot_descriptors.size(); i++) {
        if (_src_slot_descriptors[i] != nullptr && _src_slot_descriptors[i]->type().type != TYPE_VARCHAR) {
            auto t = _src_slot_descriptors[i]->type();
            return Status::InvalidArgument(fmt::format("Incorrect path column type {}", t.debug_string()));
        }
    }

    // init timezone
    if (!TimezoneUtils::find_cctz_time_zone(_state->timezone(), _timezone)) {
        return Status::InvalidArgument(fmt::format("Can not find cctz time zone {}", _state->timezone()));
    }

    // create avro column readers
    RETURN_IF_ERROR(create_column_readers());
    return Status::OK();
}

Status AvroCppScanner::create_column_readers() {
    _column_readers.resize(_num_of_columns_from_file);
    for (int column_pos = 0; column_pos < _num_of_columns_from_file; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }

        _column_readers[column_pos] = avrocpp::ColumnReader::get_nullable_column_reader(
                slot_desc->col_name(), slot_desc->type(), _timezone, !_strict_mode);
    }

    return Status::OK();
}

void AvroCppScanner::close() {
    if (_cur_file_reader != nullptr) {
        _cur_file_reader.reset();
    }

    _pool.clear();
    FileScanner::close();
}

StatusOr<ChunkPtr> AvroCppScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);

    ChunkPtr src_chunk;
    RETURN_IF_ERROR(create_src_chunk(&src_chunk));

    while (true) {
        RETURN_IF_ERROR(next_avro_chunk(src_chunk));
        if (!src_chunk->is_empty()) {
            materialize_src_chunk_adaptive_nullable_column(src_chunk);
            break;
        }
    }

    fill_columns_from_path(src_chunk, _num_of_columns_from_file, _scan_range.ranges[_next_range - 1].columns_from_path,
                           src_chunk->num_rows());

    return materialize(src_chunk, src_chunk);
}

Status AvroCppScanner::create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);

    *chunk = std::make_shared<Chunk>();
    for (int column_pos = 0; column_pos < _num_of_columns_from_file; ++column_pos) {
        auto slot_desc = _src_slot_descriptors[column_pos];
        if (slot_desc == nullptr) {
            continue;
        }

        auto column = ColumnHelper::create_column(slot_desc->type(), true, false, 0, true);
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }

    return Status::OK();
}

Status AvroCppScanner::next_avro_chunk(ChunkPtr& chunk) {
    RETURN_IF_ERROR(open_next_reader());

    SCOPED_RAW_TIMER(&_counter->read_batch_ns);
    auto st = _cur_file_reader->read_chunk(chunk, _max_chunk_size);
    if (st.is_end_of_file()) {
        _cur_file_eof = true;
    } else if (!st.is_time_out()) {
        return st;
    }

    return Status::OK();
}

Status AvroCppScanner::open_next_reader() {
    if (_cur_file_reader != nullptr && !_cur_file_eof) {
        return Status::OK();
    }

    while (_next_range < _scan_range.ranges.size()) {
        const auto& range_desc = _scan_range.ranges[_next_range];
        auto avro_reader_or = open_avro_reader(range_desc);

        const auto& st = avro_reader_or.status();
        if (st.is_end_of_file()) {
            // maybe empty file
            ++_next_range;
            continue;
        } else if (!st.ok()) {
            return st;
        }

        _cur_file_reader = std::move(avro_reader_or.value());
        _cur_file_eof = false;
        ++_next_range;
        return Status::OK();
    }

    return Status::EndOfFile("No more file to read");
}

StatusOr<AvroReaderUniquePtr> AvroCppScanner::open_avro_reader(const TBrokerRangeDesc& range_desc) {
    std::shared_ptr<RandomAccessFile> file;
    Status st = create_random_access_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params,
                                          CompressionTypePB::NO_COMPRESSION, &file);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create random access file. status: " << st.to_string()
                     << ", file: " << range_desc.path;
        return st;
    }

    // return eof when file is empty
    ASSIGN_OR_RETURN(auto filesize, file->get_size());
    if (filesize == 0) {
        return Status::EndOfFile("Empty file");
    }

    auto col_not_found_as_null =
            _scan_range.params.__isset.flexible_column_mapping && _scan_range.params.flexible_column_mapping;
    auto avro_reader = std::make_unique<AvroReader>();
    RETURN_IF_ERROR(avro_reader->init(
            std::make_unique<AvroBufferInputStream>(file, config::avro_reader_buffer_size_bytes, _counter),
            range_desc.path, _state, _counter, &_src_slot_descriptors, &_column_readers, col_not_found_as_null));
    return std::move(avro_reader);
}

void AvroCppScanner::materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        AdaptiveNullableColumn* adaptive_column =
                down_cast<AdaptiveNullableColumn*>(chunk->get_column_by_index(i).get());
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

Status AvroCppScanner::get_schema(std::vector<SlotDescriptor>* schema) {
    ASSIGN_OR_RETURN(auto avro_reader, open_avro_reader(_scan_range.ranges[0]));
    return avro_reader->get_schema(schema);
}

} // namespace starrocks