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

#include "exec/hdfs_scanner.h"
#include "exec/json_parser.h"
#include "util/byte_buffer.h"

namespace starrocks {

class HdfsScannerJsonReader {
public:
    HdfsScannerJsonReader(RandomAccessFile* file, std::vector<SlotDescriptor*> slot_descs,
                          std::vector<TypeDescriptor> type_descs);

    Status init();

    Status next_record(Chunk* chunk, int32_t rows_to_read);

    struct PreviousParsedItem {
        PreviousParsedItem(const std::string_view& key) : key(key), column_index(-1) {}
        PreviousParsedItem(const std::string_view& key, int column_index, const TypeDescriptor& type)
                : key(key), type(type), column_index(column_index) {}

        std::string key;
        TypeDescriptor type;
        int column_index;
    };

private:
    Status _read_and_parse_json();
    Status _read_file_stream();
    Status _construct_row_without_jsonpath(simdjson::ondemand::object* row, Chunk* chunk);
    Status _read_rows(Chunk* chunk, int32_t rows_to_read, int32_t* rows_read);
    Status _construct_column(simdjson::ondemand::value& value, Column* column, const TypeDescriptor& type_desc,
                             const std::string& col_name);

    const size_t _init_buf_size = config::json_read_buf_size;

    RandomAccessFile* _file = nullptr;
    std::shared_ptr<ByteBufferV2> _buffer;
    simdjson::ondemand::parser _simdjson_parser;
    std::unique_ptr<JsonParser> _parser;
    std::vector<uint8_t> _parsed_columns;
    std::vector<PreviousParsedItem> _prev_parsed_position;
    std::vector<SlotDescriptor*> _slot_descs;
    std::vector<TypeDescriptor> _type_descs;
    std::unordered_map<std::string_view, SlotDescriptor*> _slot_desc_dict;
    std::unordered_map<std::string_view, TypeDescriptor> _type_desc_dict;
    bool _empty_parser = true;
};

class HdfsJsonScanner final : public HdfsScanner {
public:
    HdfsJsonScanner() = default;
    ~HdfsJsonScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScanProfile* profile) override {}
    void do_close(RuntimeState* runtime_state) noexcept override {}
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    int64_t estimated_mem_usage() const override { return 0; }

private:
    Status _construct_json_types();
    Status _create_csv_reader();
    Status _setup_compression_type(const TTextFileDesc& text_file_desc);
    Status _build_hive_column_name_2_index();

    bool _no_data = false;
    std::shared_ptr<HdfsScannerJsonReader> _reader = nullptr;
    std::vector<Column*> _column_raw_ptrs;
    std::vector<size_t> _materialize_slots_index_2_csv_column_index;
};
} // namespace starrocks
