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

#include "exec/file_scanner/avro_datum_path.h"
#include "exec/file_scanner/confluent_avro_decoder.h"
#include "exec/file_scanner/file_scanner.h"
#include "exec/file_scanner/stream_source_meta.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks {

class SequentialFile;
class StreamMessageMeta;

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
    void materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    const TBrokerScanRange& _scan_range;
    const int _max_chunk_size;
    cctz::time_zone _timezone;
    std::unique_ptr<ConfluentAvroDecoder> _decoder;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    // Avro field name per source slot (parallel to _column_readers), materialized once in
    // create_column_readers() so the non-jsonpath fill_row() does not rebuild a std::string per cell.
    std::vector<std::string> _field_names;
    std::vector<std::vector<SimpleJsonPath>> _json_paths;
    // Hidden source-metadata slots (source slot id -> descriptor), built from the scan-range params.
    StreamSourceMetaColumns _meta_cols;
    std::shared_ptr<SequentialFile> _file;
    // Reused across messages; the GenericDatum from the previous message is overwritten on each decode.
    avro::GenericDatum _datum;
    bool _closed = false;
};

} // namespace starrocks
