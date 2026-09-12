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
#include <utility>

#include "column/nullable_column.h"
#include "compute_env/load/stream_load_pipe.h"
#include "connector/file/scanner/file_scanner.h"
#include "connector/file/scanner/stream_source_meta.h"
#include "exprs/expr_context.h"
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "simdjson.h"
#include "types/simple_json_path.h"

namespace starrocks {

class JsonReader;
class JsonParser;
class JsonScanner : public FileScanner {
public:
    JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                ScannerCounter* counter);
    ~JsonScanner() override;

    // Open this scanner, will initialize information needed
    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    // Close this scanner
    void close() override;
    static Status parse_json_paths(const std::string& jsonpath, std::vector<std::vector<SimpleJsonPath>>* path_vecs);

#if BE_TEST
    // Test-only: inject the per-message source metadata that production reads from the pipe buffer.
    // BE_TEST feeds json from a file whose buffer carries no metadata, so the metadata-column fill path
    // is otherwise unreachable from a test; this lets JsonScannerTest exercise it.
    void set_test_stream_meta(const StreamMessageMeta* meta) { _test_meta = meta; }
#endif

private:
    Status _construct_json_types();
    Status _construct_cast_exprs();
    Status _construct_default_exprs_for_absent_key();
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _open_next_reader();
    StatusOr<ChunkPtr> _cast_chunk(const ChunkPtr& src_chunk);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    friend class JsonReader;

    const TBrokerScanRange& _scan_range;
    int _next_range{0};
    const uint64_t _max_chunk_size;

    bool _cur_file_eof{true}; // indicate the current file is eof

    std::vector<std::shared_ptr<SequentialFile>> _files;

    std::vector<TypeDescriptor> _json_types;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;

    // What to fill a column with when a row's JSON object has no key for it, keyed by source slot
    // id. Empty unless the load asked for it, which is the only way the FE sends any.
    //
    // The expression is the one the plan would have used had the column been left off the columns
    // list, so a filled value means exactly what a DEFAULT already means for a load.
    //
    // A constant default is evaluated once at open and the one row result is held here, because
    // the fill sits in the row loop and a per row evaluation would allocate a chunk and a column
    // for every absent key. The cached column also owns the bytes a string default hands out, so
    // it has to outlive every row that borrows them. A default that is not constant, which in
    // practice means uuid() or uuid_numeric(), keeps its context and is evaluated per absent key,
    // because those have to differ per row.
    struct DefaultOnAbsent {
        ExprContext* ctx = nullptr;
        ColumnPtr constant_value;
    };
    std::unordered_map<SlotId, DefaultOnAbsent> _default_expr_for_absent_key;
    // The contexts in a flat list, so they can be closed the way the scanner closes its others.
    std::vector<ExprContext*> _default_expr_ctxs;

    // Declared after _pool and the absent key defaults on purpose. Members are destroyed in
    // reverse declaration order, and the reader borrows both.
    // used to hold current StreamLoadPipe
    std::unique_ptr<JsonReader> _cur_file_reader;

    std::vector<std::vector<SimpleJsonPath>> _json_paths;
    std::vector<SimpleJsonPath> _root_paths;
    bool _strip_outer_array = false;

    // An empty chunk that can be reused as the container for the result of get_next().
    // It's mainly for optimizing the performance where get_next() returns Status::Timeout
    // frequently by avoiding creating a chunk in each call
    ChunkPtr _reusable_empty_chunk = nullptr;

#if BE_TEST
    const StreamMessageMeta* _test_meta = nullptr;
#endif
};

// Reader to parse the json.
// For most of its methods which return type is Status,
// return Status::OK() if process succeed or encounter data quality error.
// return other error Status if encounter other errors.
class JsonReader {
public:
    JsonReader(RuntimeState* state, ScannerCounter* counter, JsonScanner* scanner, std::shared_ptr<SequentialFile> file,
               bool strict_mode, std::vector<SlotDescriptor*> slot_descs, std::vector<TypeDescriptor> types,
               const TBrokerRangeDesc& range_desc);

    ~JsonReader();

    Status open();

    Status read_chunk(Chunk* chunk, int32_t rows_to_read);

    Status close();

    struct PreviousParsedItem {
        PreviousParsedItem(const std::string_view& key) : key(key), column_index(-1) {}
        PreviousParsedItem(const std::string_view& key, int column_index, TypeDescriptor type)
                : key(key), type(std::move(type)), column_index(column_index) {}

        std::string key;
        TypeDescriptor type;
        int column_index;
    };

private:
    Status _read_chunk_with_except(Chunk* chunk, int32_t rows_to_read);

    template <typename ParserType>
    Status _read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read);

    Status _read_and_parse_json();
    Status _read_file_stream();
    Status _read_file_broker();
    Status _read_seekable_stream(io::SeekableInputStream* seekable_stream);
    Status _read_non_seekable_stream();
    Status _parse_payload();

    Status _construct_row(simdjson::ondemand::object* row, Chunk* chunk);

    Status _construct_row_without_jsonpath(simdjson::ondemand::object* row, Chunk* chunk);
    Status _construct_row_with_jsonpath(simdjson::ondemand::object* row, Chunk* chunk);

    Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                             std::string_view col_name);

    Status _check_ndjson();

    void _append_error_msg(const std::string&, const std::string& error_msg);

    RuntimeState* _state = nullptr;
    ScannerCounter* _counter = nullptr;
    JsonScanner* _scanner = nullptr;
    bool _strict_mode = false;

    std::shared_ptr<SequentialFile> _file;
    bool _closed = false;
    std::vector<SlotDescriptor*> _slot_descs;
    std::vector<TypeDescriptor> _type_descs;
    //Attention: _slot_desc_dict's key is the string_view of the column of _slot_descs,
    // so the lifecycle of _slot_descs should be longer than _slot_desc_dict;
    std::unordered_map<std::string_view, SlotDescriptor*> _slot_desc_dict;
    std::unordered_map<std::string_view, TypeDescriptor> _type_desc_dict;

    // For performance reason, the simdjson parser should be reused over several files.
    //https://github.com/simdjson/simdjson/blob/master/doc/performance.md
    simdjson::ondemand::parser _simdjson_parser;
    bool _is_ndjson = false;

    std::unique_ptr<JsonParser> _parser;
    bool _empty_parser = true;

    // record the chunk column position for previous parsed json object
    std::vector<PreviousParsedItem> _prev_parsed_position;
    // record the parsed column index for current json object
    std::vector<uint8_t> _parsed_columns;
    // record the "__op" column's index
    int _op_col_index{-1};
    // Hidden source-metadata columns, filled from the message's ByteBuffer meta (by slot id) rather than
    // the JSON payload. _meta_col_by_slot_id keys by source slot id (used in the jsonpath path, which
    // iterates _slot_descs); _meta_col_by_index keys by chunk column index -- the running slot order used
    // for _op_col_index -- for the object-order null-fill path. Both empty for non-routine-load.
    StreamSourceMetaColumns _meta_col_by_slot_id;
    std::unordered_map<int, TRoutineLoadMetaColumn> _meta_col_by_index;

    // The scanner's absent key defaults, borrowed rather than owned: the scanner outlives every
    // reader it opens, and they are built once for the whole scan instead of once per file.
    const std::unordered_map<SlotId, JsonScanner::DefaultOnAbsent>* _default_expr_for_absent_key = nullptr;

    // Chunk column index -> slot id, for the fill site that only has the column index.
    std::vector<SlotId> _dense_index_to_slot_id;

    // Writes this slot's absent key default into column, or a null when the slot has none.
    Status _fill_default_or_null(SlotId slot_id, Column* column);

    ByteBufferPtr _file_stream_buffer;

    std::unique_ptr<char[]> _file_broker_buffer = nullptr;
    size_t _file_broker_buffer_size = 0;
    size_t _file_broker_buffer_capacity = 0;

    char* _payload = nullptr;
    size_t _payload_size = 0;
    size_t _payload_capacity = 0;

    TBrokerRangeDesc _range_desc;

    // CDC envelope type
    TEnvelopeType::type _envelope_type = TEnvelopeType::NONE;
    // CDC operation for current row: 0 = upsert, 1 = delete; 0xFF = sentinel (not yet set this row)
    uint8_t _cdc_op = 0xFF;
};

} // namespace starrocks
