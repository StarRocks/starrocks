// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/hdfs_scanner.h"

namespace starrocks::vectorized {

class HdfsParquetScanner final : public HdfsScanner {
public:
    HdfsParquetScanner() = default;
    ~HdfsParquetScanner() override = default;

    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    void do_update_counter(HdfsScanProfile* profile) override;

private:
    std::shared_ptr<parquet::FileReader> _reader = nullptr;
};

} // namespace starrocks::vectorized
