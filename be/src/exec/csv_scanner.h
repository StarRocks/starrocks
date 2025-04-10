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
#include <vector>

#include "exec/file_scanner.h"
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"
#include "util/logging.h"
#include "util/raw_container.h"

namespace starrocks {
class SequentialFile;
}

namespace starrocks {

class CSVScanner final : public FileScanner {
public:
    CSVScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
               ScannerCounter* counter);

    Status open() override;

    StatusOr<ChunkPtr> get_next() override;

    void close() override;

    // For test
    void use_v2(bool use_v2) { _use_v2 = use_v2; }

private:
    class ScannerCSVReader : public CSVReader {
    public:
        ScannerCSVReader(std::shared_ptr<SequentialFile> file, RuntimeState* state,
                         const CSVParseOptions& parse_options)
                : CSVReader(parse_options) {
            _file = std::move(file);
            _state = state;
        }

        void set_counter(ScannerCounter* counter) { _counter = counter; }

        Status _fill_buffer() override;

        char* _find_line_delimiter(CSVBuffer& buffer, size_t pos) override;

        const std::string& filename();

    private:
        std::shared_ptr<SequentialFile> _file;
        ScannerCounter* _counter = nullptr;
        RuntimeState* _state = nullptr;
    };

    ChunkPtr _create_chunk(const std::vector<SlotDescriptor*>& slots);

    Status _parse_csv(Chunk* chunk);
    Status _parse_csv_v2(Chunk* chunk);

    StatusOr<ChunkPtr> _materialize(ChunkPtr& src_chunk);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);
    void _report_error(const CSVReader::Record& record, const std::string& err_msg);
    void _report_rejected_record(const CSVReader::Record& record, const std::string& err_msg);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    using CSVReaderPtr = std::unique_ptr<ScannerCSVReader>;

    const TBrokerScanRange& _scan_range;
    std::vector<Column*> _column_raw_ptrs;
    CSVParseOptions _parse_options;
    int _num_fields_in_csv = 0;
    int _curr_file_index = -1;
    CSVReaderPtr _curr_reader;
    std::vector<ConverterPtr> _converters;
    bool _use_v2;
    CSVReader::Fields fields;
    CSVRow row;
};

} // namespace starrocks
