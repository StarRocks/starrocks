// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_helper.h"
#include "common/status.h"
#include "exec/vectorized/parquet_scanner.h"

namespace starrocks::vectorized {
class PositionDeleteBuilder {
public:
    PositionDeleteBuilder() = default;
    virtual ~PositionDeleteBuilder() = default;

    virtual Status build(const std::string& timezone, std::set<std::int64_t> need_skip_rowids, std::string& file_path,
                         int64_t file_length) = 0;
};

class ParquetPositionDeleteBuilder final : public PositionDeleteBuilder {
public:
    ParquetPositionDeleteBuilder(FileSystem* fs, std::string datafile_path)
            : _fs(fs), _datafile_path(std::move(datafile_path)) {}
    ~ParquetPositionDeleteBuilder() override = default;

    Status build(const std::string& timezone, std::set<std::int64_t> need_skip_rowids, std::string& file_path,
                 int64_t file_length) override;

private:
    FileSystem* _fs;
    std::string _datafile_path;
};

class IcebergDeleteBuilder {
public:
    IcebergDeleteBuilder(FileSystem* fs, std::string datafile_path, std::vector<ExprContext*> conjunct_ctxs,
                         std::vector<SlotDescriptor*> materialize_slots, std::set<std::int64_t> need_skip_rowids)
            : _fs(fs),
              _datafile_path(std::move(datafile_path)),
              _conjunct_ctxs(conjunct_ctxs),
              _materialize_slots(materialize_slots),
              _need_skip_rowids(need_skip_rowids) {}
    ~IcebergDeleteBuilder() = default;

    Status build_parquet(const std::string& timezone, TIcebergDeleteFile* delete_file) {
        switch (delete_file->file_content) {
        case TIcebergFileContent::EQUALITY_DELETES:
            return Status::InvalidArgument("Equality delete file is not supported!");
        case TIcebergFileContent::POSITION_DELETES:
            (new ParquetPositionDeleteBuilder(_fs, _datafile_path))
                    ->build(timezone, _need_skip_rowids, delete_file->full_path, delete_file->length);
            break;
        default:
            LOG(WARNING) << "Hdfs parquet scanner has invalid delete file type!";
            break;
        }
    }

private:
    FileSystem* _fs;
    std::string _datafile_path;
    std::vector<ExprContext*> _conjunct_ctxs;
    std::vector<SlotDescriptor*> _materialize_slots;
    std::set<std::int64_t> _need_skip_rowids;
};
} // namespace starrocks::vectorized