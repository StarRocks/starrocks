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
    Status parse_csv(int chunk_size, ChunkPtr* chunk);

private:
    using ConverterPtr = std::unique_ptr<csv::Converter>;
    char _record_delimiter;
    string _field_delimiter;
    std::map<std::string, Column*> _column_name_ptrs;
    std::vector<ConverterPtr> _converters;
    std::shared_ptr<CSVReader> _reader = nullptr;
};
} // namespace starrocks::vectorized
