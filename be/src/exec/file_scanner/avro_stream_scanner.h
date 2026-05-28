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

#pragma once

#include <avrocpp/Generic.hh>
#include <unordered_map>
#include <unordered_set>

#include "exec/file_scanner/avro_datum_path.h"
#include "exec/file_scanner/confluent_avro_decoder.h"
#include "exec/file_scanner/file_scanner.h"
#include "exec/file_scanner/stream_source_meta.h"
#include "formats/avro/cpp/column_reader.h"
#include "gen_cpp/Descriptors_types.h"
#include "types/type_descriptor.h"

namespace starrocks {

class SequentialFile;
class StreamMessageMeta;

// Serialize every top-level field of an avro record's writer schema (`record_node`) into a TColumn with
// its full avro-derived type (nested struct/array/map preserved via type_desc) and is_allow_null=true.
// This is the whole writer schema, not a diff: the FE compares it against the live table and decides what
// to do. Exposed for unit tests.
StatusOr<std::vector<TColumn>> avro_record_to_columns(const avro::NodePtr& record_node);

// True if the writer schema (`record_node`) carries anything the table does not yet accommodate: a
// top-level field with no payload column, a struct subfield (recursively) with no matching field, a scalar
// whose writer type does not fit the column type (e.g. long into INT, decimal precision growth, scalar
// turned struct), or a field colliding with a hidden metadata column. `payload_types` maps each payload
// column's name to its table type; `meta_names` are the reserved metadata-column names. Varchar *length*
// is not judged here (avro strings are unbounded); over-length values are handled per row by the scanner.
// Used to decide whether to escalate the schema to the FE. Exposed for unit tests.
bool avro_schema_needs_evolution(const avro::NodePtr& record_node,
                                 const std::unordered_map<std::string, TypeDescriptor>& payload_types,
                                 const std::unordered_set<std::string>& meta_names);

// True if `col_type` is a length-limited varchar/varbinary (below the hard max) and the avro `datum` would
// not fit by length — the signal to widen that column. Avro strings/bytes carry no length, so an over-narrow
// string column is only detectable per value, here. Other column types and value kinds return false (an
// over-length non-string value is an ordinary per-row data error in the reader). Exposed for unit tests.
bool avro_value_overflows_varchar(const TypeDescriptor& col_type, const avro::GenericDatum& datum);

// Routine-load (Kafka) Avro scanner on the avrocpp stack. Reads one Confluent-framed message per pipe
// buffer, decodes it to an avro::GenericDatum, and fills columns through the avrocpp column readers
// (which support STRUCT/MAP/ARRAY). Selected per job via the thrift use_native_avro_reader flag, which
// the FE resolves at job creation (defaulting to the FE config enable_routine_load_native_avro_reader).
// Pulsar avro never reaches here: PulsarTaskInfo sends every non-json format to the BE as CSV_PLAIN.
class AvroStreamScanner final : public FileScanner {
public:
    AvroStreamScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                      ScannerCounter* counter);
    ~AvroStreamScanner() override;

    Status open() override;
    StatusOr<ChunkPtr> get_next() override;
    void close() override;

private:
    Status create_column_readers();
    Status create_src_chunk(ChunkPtr* chunk);
    // Decodes one message and appends one row; per-field "not found" becomes null, while decode and
    // structural failures return an error and fail the task. `meta` carries the message's source
    // metadata for the hidden metadata columns described by _meta_cols (may be null).
    Status parse_one_message(const uint8_t* data, size_t size, Chunk* chunk, const StreamMessageMeta* meta);
    Status fill_row(const avro::GenericDatum& datum, Chunk* chunk, const StreamMessageMeta* meta);
    // Schema evolution: when a message's writer schema carries a field the table does not yet cover (a
    // missing column, a missing struct subfield, or a metadata-column collision), record the full writer
    // schema on the RuntimeState and return an error so the task fails and the FE can diff + ALTER +
    // retry. The BE only decides whether to escalate; the FE picks ADD COLUMN vs ADD FIELD vs fail.
    // Checked once per distinct schema id (cached via _last_checked_schema_id).
    // Not supported in jsonpath mode: routine-load avro never had a working jsonpath->column mapping, so
    // that path carries no by-name field->column binding for evolution to build on — the caller skips the
    // check then.
    Status check_for_new_columns(const avro::GenericDatum& datum, int32_t schema_id);
    // Schema evolution for varchar/varbinary length: avro strings/bytes are unbounded, so an over-narrow
    // column only shows up when a value overruns it. On overrun, record the column on the RuntimeState and
    // return an error so the task fails and the FE widens the column (to the varchar max).
    Status check_varchar_widen(const std::string& field_name, const TypeDescriptor& col_type,
                               const avro::GenericDatum& field_datum);
    void materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    const TBrokerScanRange& _scan_range;
    const int _max_chunk_size;
    cctz::time_zone _timezone;
    std::unique_ptr<ConfluentAvroDecoder> _decoder;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    // Avro field name per source slot (parallel to _column_readers), materialized once in
    // create_column_readers() so the non-jsonpath fill_row() does not rebuild a std::string per cell.
    std::vector<std::string> _field_names;
    // Payload (non-metadata) columns by avro field name -> table type, built once in
    // create_column_readers(). check_for_new_columns() walks it to test, recursively, whether the message
    // schema is already covered (top-level columns and nested struct subfields alike).
    std::unordered_map<std::string, TypeDescriptor> _payload_types;
    // Names of hidden metadata slots; an avro field colliding with one of these fails the task instead
    // of being treated as covered or new (the metadata slot is filled by id, not from the payload).
    std::unordered_set<std::string> _meta_slot_names;
    std::vector<std::vector<SimpleJsonPath>> _json_paths;
    // Hidden source-metadata slots (source slot id -> descriptor), built from the scan-range params.
    StreamSourceMetaColumns _meta_cols;
    std::shared_ptr<SequentialFile> _file;
    // Schema-evolution gate (from query_options) and the last schema id already checked, so the
    // per-field comparison runs only when the message's schema id changes. -2 = nothing checked yet.
    bool _enable_schema_evolution = false;
    int32_t _last_checked_schema_id = -2;
    // Reused across messages; the GenericDatum from the previous message is overwritten on each decode.
    avro::GenericDatum _datum;
    bool _closed = false;
};

} // namespace starrocks
