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
#include "exec/file_scanner.h"
#include "exprs/json_functions.h"
#include "fs/fs.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "simdjson.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

struct SimpleJsonPath;
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

private:
    Status _construct_json_types();
    Status _construct_cast_exprs();
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _open_next_reader();
    StatusOr<ChunkPtr> _cast_chunk(const ChunkPtr& src_chunk);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    friend class JsonReader;

    const TBrokerScanRange& _scan_range;
    int _next_range;
    const uint64_t _max_chunk_size;

    // used to hold current StreamLoadPipe
    std::unique_ptr<JsonReader> _cur_file_reader;
    bool _cur_file_eof; // indicate the current file is eof

    std::vector<std::shared_ptr<SequentialFile>> _files;

    std::vector<TypeDescriptor> _json_types;
    std::vector<Expr*> _cast_exprs;
    ObjectPool _pool;

    std::vector<std::vector<SimpleJsonPath>> _json_paths;
    std::vector<SimpleJsonPath> _root_paths;
    bool _strip_outer_array = false;
};

// Reader to parse the json.
// For most of its methods which return type is Status,
// return Status::OK() if process succeed or encounter data quality error.
// return other error Status if encounter other errors.
class JsonReader {
public:
    JsonReader(RuntimeState* state, ScannerCounter* counter, JsonScanner* scanner, std::shared_ptr<SequentialFile> file,
               bool strict_mode, std::vector<SlotDescriptor*> slot_descs);

    ~JsonReader();

    Status open();

    Status read_chunk(Chunk* chunk, int32_t rows_to_read);

    Status close();

    struct PreviousParsedItem {
        PreviousParsedItem(const std::string_view& key) : key(key), column_index(-1) {}
        PreviousParsedItem(const std::string_view& key, int column_index, const TypeDescriptor& type)
                : key(key), type(type), column_index(column_index) {}

        std::string key;
        TypeDescriptor type;
        int column_index;
    };

private:
    template <typename ParserType>
    Status _read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read);

    Status _read_and_parse_json();
    Status _read_file_stream();
    Status _read_file_broker();
    Status _parse_payload();

    Status _construct_row(simdjson::ondemand::object* row, Chunk* chunk);

    Status _construct_row_without_jsonpath(simdjson::ondemand::object* row, Chunk* chunk);
    Status _construct_row_with_jsonpath(simdjson::ondemand::object* row, Chunk* chunk);

    Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                             const std::string& col_name);

    Status _check_ndjson();

private:
    RuntimeState* _state = nullptr;
    ScannerCounter* _counter = nullptr;
    JsonScanner* _scanner = nullptr;
    bool _strict_mode = false;

    std::shared_ptr<SequentialFile> _file;
    bool _closed = false;
    std::vector<SlotDescriptor*> _slot_descs;
    //Attention: _slot_desc_dict's key is the string_view of the column of _slot_descs,
    // so the lifecycle of _slot_descs should be longer than _slot_desc_dict;
    std::unordered_map<std::string_view, SlotDescriptor*> _slot_desc_dict;

    // For performance reason, the simdjson parser should be reused over several files.
    //https://github.com/simdjson/simdjson/blob/master/doc/performance.md
    simdjson::ondemand::parser _simdjson_parser;
    ByteBufferPtr _parser_buf;
    bool _is_ndjson = false;

    std::unique_ptr<JsonParser> _parser;
    bool _empty_parser = true;

    // record the chunk column position for previous parsed json object
    std::vector<PreviousParsedItem> _prev_parsed_position;
    // record the parsed column index for current json object
    std::vector<uint8_t> _parsed_columns;
    // record the "__op" column's index
    int _op_col_index;

    std::unique_ptr<char[]> _payload_buffer;
    size_t _payload_buffer_size;
    size_t _payload_buffer_capacity;
};

} // namespace starrocks
