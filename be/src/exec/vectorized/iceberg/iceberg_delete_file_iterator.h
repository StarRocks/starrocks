// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/vectorized/parquet_scanner.h"

namespace starrocks::vectorized {
class IcebergDeleteFileIterator {
public:
    IcebergDeleteFileIterator() = default;
    ~IcebergDeleteFileIterator() = default;

    Status init(FileSystem* fs, const std::string& timezone, std::string& file_path, int64_t file_length,
                const std::vector<SlotDescriptor*>& src_slot_descriptors, bool position_delete);

    bool has_next();

    std::shared_ptr<::arrow::RecordBatch> next();

private:
    std::shared_ptr<::arrow::RecordBatch> _batch;
    std::shared_ptr<ParquetChunkReader> _file_reader;
};
} // namespace starrocks::vectorized