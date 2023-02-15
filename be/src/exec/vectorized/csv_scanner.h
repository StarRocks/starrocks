// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

private:
    class ScannerCSVReader : public CSVReader {
    public:
        ScannerCSVReader(std::shared_ptr<SequentialFile> file, const string& record_delimiter,
                         const string& field_delimiter)
                : CSVReader(record_delimiter, field_delimiter) {
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
    StatusOr<ChunkPtr> _materialize(ChunkPtr& src_chunk);
    void _materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);
    void _report_error(const std::string& line, const std::string& err_msg);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    using CSVReaderPtr = std::unique_ptr<ScannerCSVReader>;

    const TBrokerScanRange& _scan_range;
    std::vector<Column*> _column_raw_ptrs;
    string _record_delimiter;
    string _field_delimiter;
    int _num_fields_in_csv = 0;
    int _curr_file_index = -1;
    CSVReaderPtr _curr_reader;
    std::vector<ConverterPtr> _converters;
    CSVReader::Fields fields;
};

} // namespace starrocks::vectorized
