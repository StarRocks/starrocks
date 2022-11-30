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

#include "exec/vectorized/file_scanner.h"
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"
#include "util/logging.h"
#include "util/raw_container.h"

namespace starrocks {
class SequentialFile;
}

namespace starrocks::vectorized {

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
        ScannerCSVReader(std::shared_ptr<SequentialFile> file, const std::string& record_delimiter,
                         const std::string& field_delimiter, bool trim_space, char escape, char enclose)
                : CSVReader(record_delimiter, field_delimiter, trim_space, escape, enclose) {
            _file = std::move(file);
        }

        void set_counter(ScannerCounter* counter) { _counter = counter; }

        Status _fill_buffer() override;

    private:
        std::shared_ptr<SequentialFile> _file;
        ScannerCounter* _counter = nullptr;
    };

    ChunkPtr _create_chunk(const std::vector<SlotDescriptor*>& slots);

    Status _parse_csv(Chunk* chunk);
    Status _parse_csv_v2(Chunk* chunk);

    StatusOr<ChunkPtr> _materialize(ChunkPtr& src_chunk);
    void _report_error(const std::string& line, const std::string& err_msg);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    using CSVReaderPtr = std::unique_ptr<ScannerCSVReader>;

    const TBrokerScanRange& _scan_range;
    std::vector<Column*> _column_raw_ptrs;
    std::string _record_delimiter;
    std::string _field_delimiter;
    int _num_fields_in_csv = 0;
    int _curr_file_index = -1;
    CSVReaderPtr _curr_reader;
    std::vector<ConverterPtr> _converters;
    int64_t _skip_header;
    bool _trim_space;
    char _enclose;
    char _escape;
    bool _use_v2;
};

} // namespace starrocks::vectorized
