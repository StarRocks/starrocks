// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    // create a reader or re init reader
    Status _create_or_reinit_reader();
    Status _get_hive_column_index(const std::string& column_name);

    using ConverterPtr = std::unique_ptr<csv::Converter>;
    string _record_delimiter;
    string _field_delimiter;
    char _collection_delimiter;
    char _mapkey_delimiter;
    std::vector<Column*> _column_raw_ptrs;
    std::vector<ConverterPtr> _converters;
    std::shared_ptr<CSVReader> _reader = nullptr;
    size_t _current_range_index = 0;
    std::unordered_map<std::string, int> _columns_index;
    bool _no_data = false;
};
} // namespace starrocks::vectorized
