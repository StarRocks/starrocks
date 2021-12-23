// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "env/env.h"
#include "env/env_stream_pipe.h"
#include "env/env_util.h"
#include "exec/vectorized/file_scanner.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "simdjson.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks::vectorized {

struct JsonPath;
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

private:
    Status _construct_json_types();
    Status _construct_cast_exprs();
    Status _parse_json_paths(const std::string& jsonpath, std::vector<std::vector<JsonPath>>* path_vecs);
    Status _create_src_chunk(ChunkPtr* chunk);
    Status _open_next_reader();
    ChunkPtr _cast_chunk(const ChunkPtr& src_chunk);

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

    std::vector<std::vector<JsonPath>> _json_paths;
    std::vector<JsonPath> _root_paths;
    bool _strip_outer_array = false;
};

// Reader to parse the json.
// For most of its methods which return type is Status,
// return Status::OK() if process succeed or encounter data quality error.
// return other error Status if encounter other errors.
class JsonReader {
public:
    JsonReader(RuntimeState* state, ScannerCounter* counter, JsonScanner* scanner, std::shared_ptr<SequentialFile> file,
               bool strict_mode);
    ~JsonReader();

    Status read_chunk(Chunk* chunk, int32_t rows_to_read, const std::vector<SlotDescriptor*>& slot_descs);

    Status close();

private:
    Status _read_and_parse_json();

    Status _construct_row(simdjson::ondemand::object* row, Chunk* chunk,
                          const std::vector<SlotDescriptor*>& slot_descs);

    Status _filter_row_with_jsonroot(simdjson::ondemand::object* row);

    Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                             const std::string& col_name);

    // Reorder column to accelerate simdjson iteration.
    void _reorder_column(std::vector<SlotDescriptor*>* slot_descs, simdjson::ondemand::object& obj);

private:
    RuntimeState* _state = nullptr;
    ScannerCounter* _counter = nullptr;
    JsonScanner* _scanner = nullptr;
    bool _strict_mode = false;

    std::shared_ptr<SequentialFile> _file;
    int _next_line;
    int _total_lines;
    bool _closed;
    bool _strip_outer_array;

    std::vector<std::vector<JsonPath>> _json_paths;
    std::vector<JsonPath> _root_paths;

    std::unique_ptr<uint8_t[]> _json_binary_ptr;

    std::unique_ptr<JsonParser> _parser;
    bool _empty_parser = true;
    // only used in unit test.
    // TODO: The semantics of Streaming Load And Routine Load is non-consistent.
    //       Import a json library supporting streaming parse.
#if BE_TEST
    size_t _buf_size = 1048576; // 1MB, the buf size for parsing json in unit test
    raw::RawVector<char> _buf;
#endif
};

class JsonParser {
public:
    JsonParser() = default;
    virtual ~JsonParser() = default;
    virtual Status parse(uint8_t* data, size_t len, size_t allocated) = 0;
    virtual Status get(simdjson::ondemand::object* row) = 0;
    virtual Status next() = 0;
};

class JsonDocumentStreamParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get(simdjson::ondemand::object* row) override;
    Status next() override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document_stream _doc_stream;
    simdjson::ondemand::document_stream::iterator _doc_stream_itr;
};

class JsonArrayParser : public JsonParser {
public:
    Status parse(uint8_t* data, size_t len, size_t allocated) override;
    Status get(simdjson::ondemand::object* row) override;
    Status next() override;

private:
    uint8_t* _data;
    simdjson::ondemand::parser _parser;

    simdjson::ondemand::document _doc;
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_itr;
};

} // namespace starrocks::vectorized
