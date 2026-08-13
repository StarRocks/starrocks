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

#include <utility>

#include "connector/hive/scanner/hdfs_scanner.h"
#include "formats/json/json_parser.h"
#include "runtime/byte_buffer.h"

namespace starrocks {
class HdfsJsonReader {
public:
    HdfsJsonReader(RandomAccessFile* file, const std::vector<SlotDescriptor*>& slot_descs,
                   const std::map<std::string, std::string>& serde_properties = {});
    Status init();
    Status next_record(Chunk* chunk, int32_t rows_to_read);

    // A single json key can be mapped to more than one Hive column (e.g. mapping.c1=x and
    // mapping.c2=x both point at json field "x"), so a key resolves to a list of targets.
    struct ColumnTarget {
        size_t column_index;
        TypeDescriptor type;
    };

    struct PreviousParsedItem {
        PreviousParsedItem(const std::string_view& key, std::vector<ColumnTarget> targets = {})
                : key(key), targets(std::move(targets)) {}

        std::string key;
        // Empty means this key (at this position in the row) matches no column.
        std::vector<ColumnTarget> targets;
    };

private:
    Status _read_and_parse_json();
    Status _read_file_stream() const;
    Status _read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read);
    Status _construct_row(simdjson::ondemand::object* row, Chunk* chunk);
    static Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                                    const std::string& col_name);
    // The OpenX JSON SerDe declares a column-to-json-field mapping via properties shaped like
    // "mapping.<column_name>" = "<json_field_name>". Parse those into column_name -> json_field_name,
    // i.e. the reverse of what a mapped column should be looked up by when it appears in the document.
    static std::map<std::string, std::string> _parse_column_name_mapping(
            const std::map<std::string, std::string>& serde_properties);
    // Multiple Hive columns fanning out from the same json key must share the same type: the
    // first target is decoded from json and the rest are cloned from it, so a type mismatch
    // would silently corrupt data instead of decoding independently.
    Status _validate_fan_out_targets() const;

#ifdef BE_TEST
    const int64_t INIT_BUF_SIZE = 1024;
#else
    const int64_t INIT_BUF_SIZE = 8 * 1024 * 1024;
#endif

    RandomAccessFile* _file = nullptr;
    // column_name -> json_field_name. Backing storage for the string_view keys of _desc_dict
    // that correspond to mapped columns; must outlive _desc_dict.
    std::map<std::string, std::string> _column_to_json_field;
    // A json key can resolve to more than one column; column_index isn't known until a
    // Chunk is available, so this stores (slot, type) and _prev_parsed_position caches the
    // resolved ColumnTarget list once column_index has been looked up.
    std::unordered_map<std::string_view, std::vector<std::pair<const SlotDescriptor*, TypeDescriptor>>> _desc_dict;
    std::vector<bool> _parsed_columns;
    std::vector<PreviousParsedItem> _prev_parsed_position;

    std::shared_ptr<ByteBuffer> _buf;

    simdjson::ondemand::parser _simdjson_parser;
    std::unique_ptr<JsonDocumentStreamParser> _parser;
    bool _empty_parser = true;
    // Number of trailing bytes of the current buffer that were held back from the parser
    // because they form an incomplete multi-byte UTF-8 character split across the read
    // boundary. They are carried over to the next read together with the truncated record.
    size_t _utf8_partial_tail = 0;
};

class HdfsJsonScanner final : public HdfsScanner {
public:
    HdfsJsonScanner() = default;
    explicit HdfsJsonScanner(const std::map<std::string, std::string>& serde_properties);
    ~HdfsJsonScanner() override = default;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerContext& scanner_ctx) override;
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;

private:
    Status _setup_compression_type(const TTextFileDesc& text_file_desc);

    bool _no_data = false;
    std::unique_ptr<HdfsJsonReader> _reader;
    std::map<std::string, std::string> _serde_properties;
};
} // namespace starrocks
