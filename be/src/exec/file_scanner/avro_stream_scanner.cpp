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

#include "exec/file_scanner/avro_stream_scanner.h"

#include <fmt/format.h>

#include <avrocpp/Exception.hh>
#include <avrocpp/GenericDatum.hh>
#include <avrocpp/Types.hh>

#include "base/time/timezone_utils.h"
#include "column/adaptive_nullable_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/runtime_profile.h"
#include "exec/file_scanner/json_scanner.h" // JsonScanner::parse_json_paths
#include "fs/fs.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "util/byte_buffer.h"

namespace starrocks {

AvroStreamScanner::AvroStreamScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                                     ScannerCounter* counter)
        : FileScanner(state, profile, scan_range.params, counter),
          _scan_range(scan_range),
          _max_chunk_size(state->chunk_size()) {
    _file_format_str = "avro";
}

AvroStreamScanner::~AvroStreamScanner() = default;

Status AvroStreamScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_scan_range.ranges.empty()) {
        return Status::OK();
    }

    if (!TimezoneUtils::find_cctz_time_zone(_state->timezone(), _timezone)) {
        return Status::InvalidArgument(fmt::format("can not find cctz time zone {}", _state->timezone()));
    }

    const TBrokerRangeDesc& range_desc = _scan_range.ranges[0];

    if (!_scan_range.params.__isset.confluent_schema_registry_url) {
        return Status::InternalError("'confluent_schema_registry_url' not set");
    }
    _decoder = std::make_unique<ConfluentAvroDecoder>(_scan_range.params.confluent_schema_registry_url);
    RETURN_IF_ERROR(_decoder->init());

    if (range_desc.__isset.jsonpaths) {
        std::string jsonpaths = avro_preprocess_jsonpaths(range_desc.jsonpaths);
        RETURN_IF_ERROR(JsonScanner::parse_json_paths(jsonpaths, &_json_paths));
    }

    RETURN_IF_ERROR(create_column_readers());
    RETURN_IF_ERROR(create_sequential_file(range_desc, _scan_range.broker_addresses[0], _scan_range.params, &_file));
    ++_counter->num_files_read;
    return Status::OK();
}

Status AvroStreamScanner::create_column_readers() {
    _column_readers.resize(_src_slot_descriptors.size());
    _field_names.resize(_src_slot_descriptors.size());
    for (size_t i = 0; i < _src_slot_descriptors.size(); ++i) {
        auto* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        _column_readers[i] = avrocpp::ColumnReader::get_nullable_column_reader(slot_desc->col_name(), slot_desc->type(),
                                                                               _timezone, !_strict_mode);
        // Materialize the avro field name once; fill_row() reuses it per message instead of rebuilding a
        // std::string from the pmr-backed col_name() string_view for every cell.
        _field_names[i] = slot_desc->col_name();
    }
    return Status::OK();
}

Status AvroStreamScanner::create_src_chunk(ChunkPtr* chunk) {
    SCOPED_RAW_TIMER(&_counter->init_chunk_ns);
    *chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _src_slot_descriptors.size(); ++i) {
        auto* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto column = ColumnHelper::create_column(slot_desc->type(), true, false, 0, true);
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }
    return Status::OK();
}

Status AvroStreamScanner::fill_row(const avro::GenericDatum& datum, Chunk* chunk) {
    const bool use_jsonpath = !_json_paths.empty();

    const avro::GenericRecord* record = nullptr;
    if (!use_jsonpath) {
        const avro::GenericDatum* d = &datum;
        while (d->type() == avro::AVRO_UNION) {
            d = &d->value<avro::GenericUnion>().datum();
        }
        if (d->type() != avro::AVRO_RECORD) {
            return Status::InternalError("avro message top-level value is not a record");
        }
        record = &d->value<avro::GenericRecord>();
    }

    for (size_t i = 0; i < _src_slot_descriptors.size(); ++i) {
        auto* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* column = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_slot_id(slot_desc->id()));

        if (use_jsonpath) {
            if (i >= _json_paths.size()) {
                column->append_nulls(1);
                continue;
            }
            auto extracted = avro_extract_field(datum, _json_paths[i]);
            if (extracted.status().is_not_found()) {
                column->append_nulls(1);
                continue;
            }
            RETURN_IF_ERROR(extracted.status());
            RETURN_IF_ERROR(_column_readers[i]->read_datum_for_adaptive_column(*extracted.value(), column));
        } else {
            // GenericRecord::hasField/field take const std::string&; reuse the name materialized once in
            // create_column_readers() (col_name() is a pmr-backed std::string_view).
            const std::string& field_name = _field_names[i];
            if (record->hasField(field_name)) {
                RETURN_IF_ERROR(_column_readers[i]->read_datum_for_adaptive_column(record->field(field_name), column));
            } else {
                column->append_nulls(1);
            }
        }
    }
    return Status::OK();
}

Status AvroStreamScanner::parse_one_message(const uint8_t* data, size_t size, Chunk* chunk) {
    const size_t rows_before = chunk->num_rows();

    Status st = _decoder->decode(data, size, &_datum);
    if (!st.ok()) {
        _counter->num_rows_filtered++;
        RuntimeStateHelper::append_error_msg_to_file(_state, "", st.to_string());
        return st;
    }

    // decode() converts avrocpp exceptions to Status itself; the readers below it do not, and a datum
    // shape the guards don't anticipate must cost one filtered row, never the BE.
    try {
        st = fill_row(_datum, chunk);
    } catch (const avro::Exception& e) {
        st = Status::DataQualityError(fmt::format("avro read failed: {}", e.what()));
    }
    if (!st.ok()) {
        if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
            RuntimeStateHelper::append_error_msg_to_file(_state, "", st.to_string());
        }
        chunk->set_num_rows(rows_before);
        return st;
    }
    return Status::OK();
}

StatusOr<ChunkPtr> AvroStreamScanner::get_next() {
    SCOPED_RAW_TIMER(&_counter->total_ns);
    // open() succeeds without initializing _file when the scan range carries no ranges.
    if (_file == nullptr) {
        return Status::EndOfFile("no avro ranges to read");
    }
    ChunkPtr src_chunk;
    RETURN_IF_ERROR(create_src_chunk(&src_chunk));

    auto* stream = down_cast<StreamLoadPipeInputStream*>(_file->stream().get());

    Status st = Status::OK();
    while (src_chunk->num_rows() < static_cast<size_t>(_max_chunk_size)) {
        ByteBufferPtr buf;
        {
            ++_counter->file_read_count;
            SCOPED_RAW_TIMER(&_counter->file_read_ns);
            auto res = stream->pipe()->read();
            if (!res.ok()) {
                st = res.status();
                break;
            }
            buf = std::move(res.value());
        }
        if (buf == nullptr || buf->remaining() == 0) {
            continue;
        }
        // Confluent framing requires one message per buffer; StreamLoadPipe::read() returns the whole
        // buffer that KafkaConsumerPipe::append_json enqueued, so each buffer is exactly one message.
        RETURN_IF_ERROR(
                parse_one_message(reinterpret_cast<const uint8_t*>(buf->ptr), buf->remaining(), src_chunk.get()));
    }

    // A non-timeout/non-EOF read error (cancel/abort/internal) is fatal regardless of how many rows
    // were already filled, matching the legacy AvroScanner. Otherwise it would be deferred to the next
    // get_next(), or lost entirely if the caller stops after consuming this chunk.
    if (!st.ok() && !st.is_time_out() && !st.is_end_of_file()) {
        return st;
    }

    if (src_chunk->num_rows() == 0) {
        if (st.is_end_of_file()) {
            return Status::EndOfFile("EOF of reading avro stream, nothing read");
        }
        // Timeout with nothing read yet: surface it so the task retries; with rows already filled we
        // fall through and materialize what we have (legacy behavior).
        if (st.is_time_out()) {
            return st;
        }
    }

    materialize_src_chunk_adaptive_nullable_column(src_chunk);
    return materialize(src_chunk, src_chunk);
}

void AvroStreamScanner::materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk) {
    chunk->materialized_nullable();
    for (int i = 0; i < chunk->num_columns(); i++) {
        auto* adaptive_column = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_index(i));
        chunk->update_column_by_index(NullableColumn::create(adaptive_column->materialized_raw_data_column(),
                                                             adaptive_column->materialized_raw_null_column()),
                                      i);
    }
}

void AvroStreamScanner::close() {
    if (!_closed) {
        _file.reset();
        _closed = true;
    }
    FileScanner::close();
}

} // namespace starrocks
