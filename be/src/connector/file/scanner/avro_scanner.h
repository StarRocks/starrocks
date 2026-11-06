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

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "base/container/raw_container.h"
#include "base/string/slice.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "compute_env/load/stream_load_pipe.h"
#include "connector/file/scanner/file_scanner.h"
#include "connector/file/scanner/json_scanner.h"
#include "connector/file/scanner/stream_source_meta.h"
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "types/simple_json_path.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "avro.h"
#include "libserdes/serdes.h"
#ifdef __cplusplus
}
#endif

namespace starrocks {

using AvroPath = SimpleJsonPath;

class StreamMessageMeta;

class AvroScanner final : public FileScanner {
public:
    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter);

    // A new constructor is introduced for the single test.
    AvroScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter, std::string schema_text);
    ~AvroScanner() override;

    // Open this scanner, will initialize information needed
    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    // Close this scanner
    void close() override;

    static std::string preprocess_jsonpaths(std::string jsonpath);

    struct SlotInfo {
        SlotInfo() = default;
        SlotId id{-2};
        TypeDescriptor type;
        std::string key;
    };

private:
    // Field-index -> slot mapping for the no-jsonpath path. It depends on the writer schema, so it is
    // kept per schema id (see AvroDecodeEntry) rather than once per scanner.
    struct AvroFieldMapping {
        bool initialized = false;
        std::vector<SlotInfo> data_idx_to_slot;
        std::vector<std::string> data_idx_to_fieldname;
    };

    // Decoder state reused for every message that carries the same Confluent schema id. Building the
    // generic class and value tree is far more expensive than reading a message into it, so they are
    // created once per id and the value is overwritten by each read.
    struct AvroDecodeEntry {
        avro_value_iface_t* iface = nullptr;
        avro_value_t value{};
        bool value_created = false;
        avro_reader_t reader = nullptr;
        bool root_is_bytes = false;
        AvroFieldMapping mapping;
    };

    Status _construct_avro_types();
    Status _construct_cast_exprs();
    StatusOr<ChunkPtr> _cast_chunk(const starrocks::ChunkPtr& src_chunk);
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _parse_avro(Chunk* chunk, const std::shared_ptr<SequentialFile>& file);
    void _report_error(const std::string& line, const std::string& err_msg);
    Status _construct_row(const avro_value_t& avro_value, Chunk* chunk, const StreamMessageMeta* meta);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);
    Status _construct_column(const avro_value_t& input_value, Column* column, const TypeDescriptor& type_desc,
                             std::string_view col_name);
    Status _extract_field(const avro_value_t& input_value, const std::vector<AvroPath>& paths,
                          avro_value_t* output_value);
    Status _handle_union(const avro_value_t* input_value, avro_value_t* branch);
    Status _get_array_element(const avro_value_t* cur_value, size_t idx, avro_value_t* element);
    std::string _preprocess_jsonpaths(std::string jsonpath);
    Status _init_field_mapping(const avro_value_t& avro_value, AvroFieldMapping* mapping);
    Status _construct_row_without_jsonpath(const avro_value_t& avro_value, Chunk* chunk, const StreamMessageMeta* meta,
                                           AvroFieldMapping* mapping);
    // Decodes one Confluent-framed message into the cached value for its schema id. On failure a
    // description is left in _err_buf (same texts as serdes_deserialize_avro) and an error is returned.
    serdes_err_t _decode_confluent_message(const uint8_t* data, size_t length, AvroDecodeEntry** entry);
    void _release_decode_cache();

    const TBrokerScanRange& _scan_range;
    serdes_t* _serdes;
    std::string _schema_text;
    bool _closed;
    char _err_buf[512] = {0};
    std::vector<Column*> _column_raw_ptrs;
    ByteBufferPtr _parser_buf;
    std::vector<std::vector<AvroPath>> _json_paths;
    std::vector<TypeDescriptor> _avro_types;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;
    std::shared_ptr<SequentialFile> _file;
    std::unordered_map<std::string_view, SlotDescriptor*> _slot_desc_dict;
    // Maps each source slot id to its intermediate avro load type (see AvroScanner::_construct_avro_types).
    std::unordered_map<SlotId, TypeDescriptor> _slot_id_to_avro_type;
    // Hidden source-metadata slots (routine load), filled from the message's ByteBuffer meta rather than
    // the avro payload. _meta_col_by_slot_id keys by source slot id (the jsonpath path iterates slots);
    // _meta_col_by_index keys by chunk column index for the by-name null-fill path. Both empty otherwise.
    StreamSourceMetaColumns _meta_col_by_slot_id;
    std::unordered_map<int, TRoutineLoadMetaColumn> _meta_col_by_index;
    std::vector<bool> _found_columns;
    // Keyed by Confluent schema id. Used by one scanner (one task thread) only.
    std::unordered_map<int, std::unique_ptr<AvroDecodeEntry>> _decode_cache;

#if BE_TEST
public:
    // Test-only: inject the per-message source metadata that production reads from the pipe buffer.
    // BE_TEST reads avro from a file rather than the Kafka/Pulsar pipe, so the metadata-column fill path
    // is otherwise unreachable from a test; this lets AvroScannerTest exercise it.
    void set_test_stream_meta(const StreamMessageMeta* meta) { _test_meta = meta; }

    // Test-only: feed Confluent-framed messages through the same decode path production uses for the
    // Kafka/Pulsar pipe. The scanner takes ownership of `serdes` (schemas can be added to it locally
    // with serdes_schema_add, so no schema registry is needed). Must be called before open().
    void set_test_confluent_messages(serdes_t* serdes, std::vector<std::string> messages) {
        _serdes = serdes;
        _test_messages = std::move(messages);
        _test_use_confluent_messages = true;
    }
    size_t decode_cache_size_for_test() const { return _decode_cache.size(); }
    std::string last_decode_error_for_test() const { return std::string(_err_buf); }

private:
    avro_file_reader_t _dbreader = nullptr;
    const StreamMessageMeta* _test_meta = nullptr;
    // Used when the test reads avro from a data file instead of Confluent-framed messages.
    AvroFieldMapping _file_field_mapping;
    bool _test_use_confluent_messages = false;
    std::vector<std::string> _test_messages;
    size_t _test_message_idx = 0;
#endif
};

} // namespace starrocks
