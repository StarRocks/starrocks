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

#include <algorithm>
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
#include "exec/file_scanner/stream_source_meta.h"
#include "formats/avro/cpp/avro_schema_builder.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "types/type_descriptor.h"
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

    _enable_schema_evolution = _state->query_options().enable_avro_schema_evolution;

    if (range_desc.__isset.jsonpaths) {
        std::string jsonpaths = avro_preprocess_jsonpaths(range_desc.jsonpaths);
        RETURN_IF_ERROR(JsonScanner::parse_json_paths(jsonpaths, &_json_paths));
    }

    _meta_cols = build_stream_source_meta_columns(_scan_range.params.stream_source_meta_columns);

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
        // Classify the slot for schema-evolution detection: metadata slots are a reserved namespace
        // (filled by id, not from payload), payload slots are the ones that can evolve.
        if (_meta_cols.count(slot_desc->id()) > 0) {
            _meta_slot_names.insert(_field_names[i]);
        } else {
            _payload_types.emplace(_field_names[i], slot_desc->type());
        }
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

Status AvroStreamScanner::fill_row(const avro::GenericDatum& datum, Chunk* chunk, const StreamMessageMeta* meta) {
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

    // Hidden metadata columns carry no jsonpath, so they are transparent to the positional
    // slot->jsonpath mapping: a jsonpath maps to the i-th non-metadata slot. This keeps payload columns
    // aligned even when a metadata column sits before jsonpath-backed columns.
    size_t meta_count = 0;
    for (size_t i = 0; i < _src_slot_descriptors.size(); ++i) {
        auto* slot_desc = _src_slot_descriptors[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* column = down_cast<AdaptiveNullableColumn*>(chunk->get_column_raw_ptr_by_slot_id(slot_desc->id()));

        // Hidden source-metadata slots are filled from the message meta (by slot id), not the payload,
        // before the jsonpath/by-name extraction below.
        if (auto it = _meta_cols.find(slot_desc->id()); it != _meta_cols.end()) {
            RETURN_IF_ERROR(fill_stream_source_meta_column(it->second.kind, it->second.key, meta, column));
            meta_count++;
            continue;
        }

        if (use_jsonpath) {
            size_t jp = i - meta_count;
            if (jp >= _json_paths.size()) {
                column->append_nulls(1);
                continue;
            }
            auto extracted = avro_extract_field(datum, _json_paths[jp]);
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
                const avro::GenericDatum& field_datum = record->field(field_name);
                // varchar/varbinary length evolution: an over-length value fails the task so the FE can
                // widen the column (the avro schema carries no length, so this is value-driven).
                if (_enable_schema_evolution) {
                    RETURN_IF_ERROR(check_varchar_widen(field_name, slot_desc->type(), field_datum));
                }
                RETURN_IF_ERROR(_column_readers[i]->read_datum_for_adaptive_column(field_datum, column));
            } else {
                column->append_nulls(1);
            }
        }
    }
    return Status::OK();
}

// Unwrap an avro union down to its single non-null branch (the shape of a nullable field, ["null", T]).
// A genuine multi-branch union has no single mapped type, so it is returned as-is and treated as opaque.
static avro::NodePtr avro_non_null_branch(const avro::NodePtr& node) {
    if (node->type() != avro::AVRO_UNION) {
        return node;
    }
    avro::NodePtr branch;
    for (size_t i = 0; i < node->leaves(); ++i) {
        if (node->leafAt(i)->type() != avro::AVRO_NULL) {
            if (branch != nullptr) {
                return node; // more than one non-null branch: opaque
            }
            branch = node->leafAt(i);
        }
    }
    return branch != nullptr ? branch : node;
}

// Byte-width rank within the integer and float families, so "is the column wide enough" is a comparison.
static int numeric_rank(LogicalType t) {
    switch (t) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
    case TYPE_FLOAT:
        return 4;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
        return 8;
    case TYPE_LARGEINT:
        return 16;
    default:
        return 0;
    }
}

// True if column `col` can hold the writer field's mapped scalar type `desired` without loss. A
// varchar/varbinary column accommodates any value (the reader stringifies / JSON-encodes it), so it always
// fits. Otherwise the column must be the same type or a wider one in the same family; cross-family or
// narrowing changes (long->INT, double->FLOAT, int->DATE, scalar->other scalar) do not fit and are
// escalated to the FE. Varchar *length* is not judged here (avro strings are unbounded) — an over-length
// value is caught per row in fill_row().
static bool avro_scalar_fits(const TypeDescriptor& desired, const TypeDescriptor& col) {
    if (is_string_type(col.type) || is_binary_type(col.type)) {
        return true;
    }
    if (is_decimalv3_field_type(col.type)) {
        if (is_decimalv3_field_type(desired.type)) {
            return col.precision >= desired.precision && col.scale >= desired.scale;
        }
        // integer/float into decimal: the reader casts with a per-value overflow check.
        return is_integer_type(desired.type) || is_float_type(desired.type);
    }
    if (is_integer_type(col.type)) {
        return is_integer_type(desired.type) && numeric_rank(col.type) >= numeric_rank(desired.type);
    }
    if (is_float_type(col.type)) {
        if (is_integer_type(desired.type)) {
            return true; // an integer always fits a float column
        }
        return is_float_type(desired.type) && numeric_rank(col.type) >= numeric_rank(desired.type);
    }
    // DATE/DATETIME/BOOLEAN/JSON/...: only an exact type match fits.
    return desired.type == col.type;
}

// True if everything the avro node carries is accommodated by `table_type`: every struct field has a
// matching column/subfield (by name) AND every scalar's writer type fits the column type, recursing
// through struct/array/map. A false return is the signal to escalate the schema to the FE.
static bool avro_field_covered(const avro::NodePtr& node_in, const TypeDescriptor& table_type) {
    const avro::NodePtr node = avro_non_null_branch(node_in);
    switch (node->type()) {
    case avro::AVRO_RECORD: {
        // A non-struct column still holds a record if it is varchar/varbinary (the reader JSON-encodes it);
        // any other column type cannot, so the kind change must be escalated.
        if (table_type.type != TYPE_STRUCT) {
            return is_string_type(table_type.type) || is_binary_type(table_type.type);
        }
        for (size_t i = 0; i < node->leaves(); ++i) {
            const std::string& field_name = node->nameAt(i);
            auto it = std::find(table_type.field_names.begin(), table_type.field_names.end(), field_name);
            if (it == table_type.field_names.end()) {
                return false; // a struct subfield with no matching field on the table side
            }
            size_t idx = std::distance(table_type.field_names.begin(), it);
            if (!avro_field_covered(node->leafAt(i), table_type.children[idx])) {
                return false;
            }
        }
        return true;
    }
    case avro::AVRO_ARRAY:
        if (table_type.type != TYPE_ARRAY || table_type.children.empty()) {
            return is_string_type(table_type.type) || is_binary_type(table_type.type);
        }
        return avro_field_covered(node->leafAt(0), table_type.children[0]);
    case avro::AVRO_MAP:
        if (table_type.type != TYPE_MAP || table_type.children.size() < 2) {
            return is_string_type(table_type.type) || is_binary_type(table_type.type);
        }
        // Avro map keys are always strings; a non-string key column cannot take them (the map reader
        // rejects it per row), so escalate instead of letting every row fail.
        if (!is_string_type(table_type.children[0].type) && !is_binary_type(table_type.children[0].type)) {
            return false;
        }
        return avro_field_covered(node->leafAt(1), table_type.children[1]); // children: [0]=key, [1]=value
    default: {
        TypeDescriptor desired;
        if (!get_avro_type(node, &desired).ok()) {
            return true; // unmappable writer type: don't escalate (defensive, avoids spurious failures)
        }
        return avro_scalar_fits(desired, table_type);
    }
    }
}

bool avro_schema_needs_evolution(const avro::NodePtr& record_node,
                                 const std::unordered_map<std::string, TypeDescriptor>& payload_types,
                                 const std::unordered_set<std::string>& meta_names) {
    for (size_t i = 0; i < record_node->leaves(); ++i) {
        const std::string& field_name = record_node->nameAt(i);
        // A field colliding with a hidden metadata column: the slot is filled by id, not from the payload,
        // so a same-named field would be silently dropped. Escalate so the FE can fail the job and the user
        // resolves the clash (the FE recognizes the collision from the full schema).
        if (meta_names.count(field_name) > 0) {
            return true;
        }
        auto it = payload_types.find(field_name);
        if (it == payload_types.end()) {
            return true; // a top-level field with no table column
        }
        if (!avro_field_covered(record_node->leafAt(i), it->second)) {
            return true; // a new struct subfield somewhere under an existing column
        }
    }
    return false;
}

StatusOr<std::vector<TColumn>> avro_record_to_columns(const avro::NodePtr& record_node) {
    std::vector<TColumn> columns;
    columns.reserve(record_node->leaves());
    for (size_t i = 0; i < record_node->leaves(); ++i) {
        TypeDescriptor type_desc;
        RETURN_IF_ERROR(get_avro_type(record_node->leafAt(i), &type_desc));
        TColumn col;
        col.__set_column_name(record_node->nameAt(i));
        col.__set_type_desc(type_desc.to_thrift());
        col.__set_is_allow_null(true);
        columns.emplace_back(std::move(col));
    }
    return columns;
}

bool avro_value_overflows_varchar(const TypeDescriptor& col_type, const avro::GenericDatum& datum) {
    const bool varchar = col_type.type == TYPE_VARCHAR;
    const bool varbinary = col_type.type == TYPE_VARBINARY;
    if ((!varchar && !varbinary) || col_type.len >= TypeDescriptor::MAX_VARCHAR_LENGTH) {
        return false; // not a narrow varchar/varbinary: nothing to widen
    }
    const avro::GenericDatum* d = &datum;
    while (d->type() == avro::AVRO_UNION) {
        d = &d->value<avro::GenericUnion>().datum();
    }
    size_t value_len = 0;
    if (varchar && d->type() == avro::AVRO_STRING) {
        value_len = d->value<std::string>().size();
    } else if (varbinary && d->type() == avro::AVRO_BYTES) {
        value_len = d->value<std::vector<uint8_t>>().size();
    } else {
        return false; // other value kinds become a per-row data error in the reader, not a widen
    }
    return static_cast<int>(value_len) > col_type.len;
}

Status AvroStreamScanner::check_varchar_widen(const std::string& field_name, const TypeDescriptor& col_type,
                                              const avro::GenericDatum& field_datum) {
    if (!avro_value_overflows_varchar(col_type, field_datum)) {
        return Status::OK();
    }
    _state->add_avro_widen_column(_last_checked_schema_id, field_name);
    return Status::InternalError(
            fmt::format("avro schema evolution: value exceeds {}({}) column '{}'; column needs widening",
                        col_type.type == TYPE_VARCHAR ? "varchar" : "varbinary", col_type.len, field_name));
}

Status AvroStreamScanner::check_for_new_columns(const avro::GenericDatum& datum, int32_t schema_id) {
    // Unwrap unions down to the record, mirroring fill_row().
    const avro::GenericDatum* d = &datum;
    while (d->type() == avro::AVRO_UNION) {
        d = &d->value<avro::GenericUnion>().datum();
    }
    if (d->type() != avro::AVRO_RECORD) {
        // Non-record top level has no columns to evolve; remember the id so we don't re-check it.
        _last_checked_schema_id = schema_id;
        return Status::OK();
    }

    const avro::GenericRecord& record = d->value<avro::GenericRecord>();
    _last_checked_schema_id = schema_id;
    if (!avro_schema_needs_evolution(record.schema(), _payload_types, _meta_slot_names)) {
        // Schema id changed but the table already covers every field (also how the loop ends after an
        // ALTER: the next task's dest tuple covers the schema, so it no longer escalates); nothing to do.
        return Status::OK();
    }

    // The table is missing at least one field this schema carries. Ship the full writer schema to the FE
    // (every top-level field with its complete nested type) and fail the task. The FE diffs it against the
    // live table and owns every decision: ADD COLUMN for an absent column, ADD FIELD for a new struct
    // subfield, or fail the job on a metadata-column collision. The BE neither computes the diff nor picks
    // the DDL — with the whole schema in hand that all belongs on the FE.
    ASSIGN_OR_RETURN(auto columns, avro_record_to_columns(record.schema()));
    const size_t num_cols = columns.size();
    _state->set_avro_schema_change(schema_id, std::move(columns));
    return Status::InternalError(fmt::format(
            "avro schema evolution required: schema id {} not covered by table; sent {} schema column(s) to FE",
            schema_id, num_cols));
}

Status AvroStreamScanner::parse_one_message(const uint8_t* data, size_t size, Chunk* chunk,
                                            const StreamMessageMeta* meta) {
    const size_t rows_before = chunk->num_rows();
    auto meta_ctx = [meta]() -> std::string {
        return meta != nullptr ? fmt::format("[meta: {}]", meta->to_string()) : "";
    };

    int32_t schema_id = -1;
    Status st = _decoder->decode(data, size, &_datum, &schema_id);
    if (!st.ok()) {
        _counter->num_rows_filtered++;
        RuntimeStateHelper::append_error_msg_to_file(_state, meta_ctx(), st.to_string());
        return st;
    }

    // Detect new columns before filling the row. On the first message of a new schema id this either
    // returns an error (table needs an ALTER) or marks the id as checked; fill_row then runs as usual.
    // Schema evolution runs only on the by-name path (_json_paths empty). jsonpaths are excluded because
    // routine-load avro never had a working jsonpath->column mapping, so that path carries no by-name
    // field->column binding for evolution to build on.
    if (_enable_schema_evolution && _json_paths.empty() && schema_id != _last_checked_schema_id) {
        RETURN_IF_ERROR(check_for_new_columns(_datum, schema_id));
    }

    // decode() converts avrocpp exceptions to Status itself; the readers below it do not, and a datum
    // shape the guards don't anticipate must cost one filtered row, never the BE.
    try {
        st = fill_row(_datum, chunk, meta);
    } catch (const avro::Exception& e) {
        st = Status::DataQualityError(fmt::format("avro read failed: {}", e.what()));
    }
    if (!st.ok()) {
        if (_counter->num_rows_filtered++ < MAX_ERROR_LINES_IN_FILE) {
            RuntimeStateHelper::append_error_msg_to_file(_state, meta_ctx(), st.to_string());
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
        RETURN_IF_ERROR(parse_one_message(reinterpret_cast<const uint8_t*>(buf->ptr), buf->remaining(), src_chunk.get(),
                                          stream_source_meta_of(buf)));
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
