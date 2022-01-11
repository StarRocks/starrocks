// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/vectorized/hdfs_scanner.h"
#include "formats/csv/converter.h"
#include "formats/csv/csv_reader.h"

namespace starrocks::vectorized {

class HdfsTextScanner final : public HdfsScanner {
public:
    HdfsTextScanner() = default;
    ~HdfsTextScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    Status _parse_csv(int chunk_size, ChunkPtr* chunk);

private:
    class HdfsScannerCSVReader : public CSVReader {
    public:
        HdfsScannerCSVReader(std::shared_ptr<RandomAccessFile> file, char record_delimiter, string field_delimiter,
                             size_t offset, size_t remain_length, size_t file_length)
                : CSVReader(record_delimiter, field_delimiter) {
            _file = file;
            _offset = offset;
            _remain_length = remain_length;
            _file_length = file_length;
        }

        Status _fill_buffer() override;

    private:
        std::shared_ptr<RandomAccessFile> _file;
        size_t _offset = 0;
        size_t _remain_length = 0;
        size_t _file_length = 0;
        bool _should_stop_scan = false;
    };

    using ConverterPtr = std::unique_ptr<csv::Converter>;

    char _record_delimiter;
    string _field_delimiter;
    std::vector<Column*> _column_raw_ptrs;
    std::vector<ConverterPtr> _converters;
    std::shared_ptr<CSVReader> _reader = nullptr;
};
} // namespace starrocks::vectorized
