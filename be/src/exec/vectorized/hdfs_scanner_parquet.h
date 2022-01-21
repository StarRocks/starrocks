// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#pragma once

#include "exec/vectorized/hdfs_scanner.h"

namespace starrocks::vectorized {

class HdfsParquetProfile {
public:
    // read & decode
    RuntimeProfile::Counter* level_decode_timer = nullptr;
    RuntimeProfile::Counter* value_decode_timer = nullptr;
    RuntimeProfile::Counter* page_read_timer = nullptr;

    // reader init
    RuntimeProfile::Counter* footer_read_timer = nullptr;
    RuntimeProfile::Counter* column_reader_init_timer = nullptr;

    // dict filter
    RuntimeProfile::Counter* group_chunk_read_timer = nullptr;
    RuntimeProfile::Counter* group_dict_filter_timer = nullptr;
    RuntimeProfile::Counter* group_dict_decode_timer = nullptr;

    void init(RuntimeProfile* root);

private:
    RuntimeProfile::Counter* _toplev = nullptr;
};

class HdfsParquetScanner final : public HdfsScanner {
public:
    HdfsParquetScanner() = default;
    ~HdfsParquetScanner() override {
        if (_runtime_state != nullptr) {
            close(_runtime_state);
        }
    }

    void update_counter();
    Status do_open(RuntimeState* runtime_state) override;
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;

private:
    std::shared_ptr<parquet::FileReader> _reader = nullptr;
};

} // namespace starrocks::vectorized