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
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"

namespace starrocks {

const std::string DEFAULT_FIELD_DELIM = "\001";
const std::string DEFAULT_COLLECTION_DELIM = "\002";
const std::string DEFAULT_MAPKEY_DELIM = "\003";
// LF = Line Feed = '\n'
const std::string LINE_DELIM_LF = "\n";
// Most hive TextFile using LF as line delimiter
const std::string DEFAULT_LINE_DELIM = LINE_DELIM_LF;
// CR = Carriage Return = '\r'
const std::string LINE_DELIM_CR = "\r";
// TODO(SmithCruise) CR + LF, but we don't support it yet, because our code only support single char as line delimiter
const std::string LINE_DELIM_CR_LF = "\r\n";

// This class used by data lake(Hive, Iceberg,... etc), not for broker load.
// Broker load plz refer to csv_scanner.cpp
class HdfsTextScanner final : public HdfsScanner {
public:
    HdfsTextScanner() = default;
    ~HdfsTextScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScanProfile* profile) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    int64_t estimated_mem_usage() const override;

private:
    Status _create_csv_reader();
    Status _setup_compression_type(const TTextFileDesc& text_file_desc);
    Status _setup_delimiter(const TTextFileDesc& text_file_desc);
    StatusOr<bool> _has_utf8_bom() const;
    Status _build_hive_column_name_2_index();
    Status _parse_csv(int chunk_size, ChunkPtr* chunk);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    std::string _line_delimiter;
    std::string _field_delimiter;
    char _collection_delimiter;
    char _mapkey_delimiter;
    int32_t _skip_header_line_count = 0;
    bool _need_probe_line_delimiter = false;
    // Always set true in data lake now.
    // TODO(SmithCruise) use a hive catalog property to control this behavior
    bool _invalid_field_as_null = true;
    std::vector<Column*> _column_raw_ptrs;
    std::vector<ConverterPtr> _converters;
    std::shared_ptr<CSVReader> _reader = nullptr;
    // _materialize_slots_index_2_csv_column_index[0] = 5 means materialize_slots[0]->column index 5 in csv
    // materialize_slots is StarRocks' table definition, column index is the actual position in csv
    std::vector<size_t> _materialize_slots_index_2_csv_column_index;
    // (TODO) move compressed file don't split logic to FE, _no_data is prone to bugs
    bool _no_data = false;
};
} // namespace starrocks
