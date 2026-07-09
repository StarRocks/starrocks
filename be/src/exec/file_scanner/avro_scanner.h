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

#include <string_view>

#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/file_scanner/file_scanner.h"
#include "exec/file_scanner/json_scanner.h"
<<<<<<< HEAD
=======
#include "exec/file_scanner/stream_source_meta.h"
>>>>>>> 0796ea6077 ([Enhancement] Expose Kafka/Pulsar message metadata via an INCLUDE METADATA clause in Routine Load (#73840))
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "util/raw_container.h"
#include "util/slice.h"
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
        SlotInfo() : id(-2) {}
        SlotId id;
        TypeDescriptor type;
        std::string key;
    };

private:
    Status _construct_avro_types();
    Status _construct_cast_exprs();
    StatusOr<ChunkPtr> _cast_chunk(const starrocks::ChunkPtr& src_chunk);
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _parse_avro(Chunk* chunk, const std::shared_ptr<SequentialFile>& file);
    void _report_error(const std::string& line, const std::string& err_msg);
    Status _construct_row(const avro_value_t& avro_value, Chunk* chunk, const StreamMessageMeta* meta);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);
    Status _construct_column(const avro_value_t& input_value, Column* column, const TypeDescriptor& type_desc,
                             const std::string& col_name);
    Status _extract_field(const avro_value_t& input_value, const std::vector<AvroPath>& paths,
                          avro_value_t* output_value);
    Status _handle_union(const avro_value_t* input_value, avro_value_t* branch);
    Status _get_array_element(const avro_value_t* cur_value, size_t idx, avro_value_t* element);
    std::string _preprocess_jsonpaths(std::string jsonpath);
    Status _construct_row_without_jsonpath(const avro_value_t& avro_value, Chunk* chunk, const StreamMessageMeta* meta);

    const TBrokerScanRange& _scan_range;
    serdes_t* _serdes;
    std::string _schema_text;
    bool _closed;
    char _err_buf[512];
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
    std::vector<SlotInfo> _data_idx_to_slot;
    std::vector<std::string> _data_idx_to_fieldname;
    bool _init_data_idx_to_slot_once;

#if BE_TEST
public:
    // Test-only: inject the per-message source metadata that production reads from the pipe buffer.
    // BE_TEST reads avro from a file rather than the Kafka/Pulsar pipe, so the metadata-column fill path
    // is otherwise unreachable from a test; this lets AvroScannerTest exercise it.
    void set_test_stream_meta(const StreamMessageMeta* meta) { _test_meta = meta; }

private:
    avro_file_reader_t _dbreader;
    const StreamMessageMeta* _test_meta = nullptr;
#endif
};

} // namespace starrocks