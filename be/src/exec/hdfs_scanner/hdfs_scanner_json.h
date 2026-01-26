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

#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exec/json_parser.h"
#include "util/byte_buffer.h"

namespace starrocks {
class HdfsJsonReader {
public:
    HdfsJsonReader(RandomAccessFile* file, const std::vector<SlotDescriptor*>& slot_descs);
    Status init();
    Status next_record(Chunk* chunk, int32_t rows_to_read);

    struct PreviousParsedItem {
        explicit PreviousParsedItem(const std::string_view& key) : key(key), column_index(-1) {}
        PreviousParsedItem(const std::string_view& key, int column_index, const TypeDescriptor& type)
                : key(key), type(type), column_index(column_index) {}

        std::string key;
        TypeDescriptor type;
        int column_index;
    };

private:
    Status _read_and_parse_json();
    Status _read_file_stream() const;
    Status _read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read);
    Status _construct_row(simdjson::ondemand::object* row, Chunk* chunk);
    static Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                                    const std::string& col_name);

#ifdef BE_TEST
    const int64_t INIT_BUF_SIZE = 1024;
#else
    const int64_t INIT_BUF_SIZE = 8 * 1024 * 1024;
#endif

    RandomAccessFile* _file = nullptr;
    std::unordered_map<std::string_view, std::pair<const SlotDescriptor*, TypeDescriptor>> _desc_dict;
    std::vector<bool> _parsed_columns;
    std::vector<PreviousParsedItem> _prev_parsed_position;

    std::shared_ptr<ByteBuffer> _buf;

    simdjson::ondemand::parser _simdjson_parser;
    std::unique_ptr<JsonDocumentStreamParser> _parser;
    bool _empty_parser = true;
};

class HdfsJsonScanner final : public HdfsScanner {
public:
    HdfsJsonScanner() = default;
    ~HdfsJsonScanner() override = default;

    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;

private:
    Status _setup_compression_type(const TTextFileDesc& text_file_desc);

    bool _no_data = false;
    std::unique_ptr<HdfsJsonReader> _reader;
};
} // namespace starrocks