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

#include <utility>

#include "block_cache/cache_options.h"
#include "common/status.h"
#include "exec/mor_processor.h"
#include "exec/parquet_scanner.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"

namespace starrocks {
struct IcebergColumnMeta;

class DeleteBuilder {
public:
    DeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options)
            : _fs(fs), _datacache_options(datacache_options){};
    virtual ~DeleteBuilder() = default;

protected:
    FileSystem* _fs;
    const DataCacheOptions& _datacache_options;
};

class PositionDeleteBuilder : public DeleteBuilder {
public:
    PositionDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options)
            : DeleteBuilder(fs, datacache_options) {}
    ~PositionDeleteBuilder() override = default;

    virtual Status build(const std::string& timezone, const std::string& file_path, int64_t file_length,
                         std::set<int64_t>* need_skip_rowids) = 0;
};

class EqualityDeleteBuilder : public DeleteBuilder {
public:
    EqualityDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options)
            : DeleteBuilder(fs, datacache_options) {}
    ~EqualityDeleteBuilder() override = default;

    virtual Status build(const std::string& timezone, const std::string& file_path, int64_t file_length,
                         std::shared_ptr<DefaultMORProcessor> mor_processor, std::vector<SlotDescriptor*> slots,
                         TupleDescriptor* delete_column_tuple_desc, const TIcebergSchema* iceberg_equal_delete_schema,
                         RuntimeState* state) = 0;
};

class ORCEqualityDeleteBuilder final : public EqualityDeleteBuilder {
public:
    ORCEqualityDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options, std::string datafile_path)
            : EqualityDeleteBuilder(fs, datacache_options), _datafile_path(std::move(datafile_path)) {}
    ~ORCEqualityDeleteBuilder() override = default;

    Status build(const std::string& timezone, const std::string& file_path, int64_t file_length,
                 std::shared_ptr<DefaultMORProcessor> mor_processor, std::vector<SlotDescriptor*> slots,
                 TupleDescriptor* delete_column_tuple_desc, const TIcebergSchema* iceberg_equal_delete_schema,
                 RuntimeState* stage) override;

private:
    std::string _datafile_path;
};

class ParquetEqualityDeleteBuilder final : public EqualityDeleteBuilder {
public:
    ParquetEqualityDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options, std::string datafile_path)
            : EqualityDeleteBuilder(fs, datacache_options), _datafile_path(std::move(datafile_path)) {}
    ~ParquetEqualityDeleteBuilder() override = default;

    Status build(const std::string& timezone, const std::string& file_path, int64_t file_length,
                 std::shared_ptr<DefaultMORProcessor> mor_processor, std::vector<SlotDescriptor*> slots,
                 TupleDescriptor* delete_column_tuple_desc, const TIcebergSchema* iceberg_equal_delete_schema,
                 RuntimeState* stage) override;

private:
    std::string _datafile_path;
    std::atomic<int32_t> _lazy_column_coalesce_counter = 0;
};

class ORCPositionDeleteBuilder final : public PositionDeleteBuilder {
public:
    ORCPositionDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options, std::string datafile_path)
            : PositionDeleteBuilder(fs, datacache_options), _datafile_path(std::move(datafile_path)) {}
    ~ORCPositionDeleteBuilder() override = default;

    Status build(const std::string& timezone, const std::string& delete_file_path, int64_t file_length,
                 std::set<int64_t>* need_skip_rowids) override;

private:
    std::string _datafile_path;
};

class ParquetPositionDeleteBuilder final : public PositionDeleteBuilder {
public:
    ParquetPositionDeleteBuilder(FileSystem* fs, const DataCacheOptions& datacache_options, std::string datafile_path)
            : PositionDeleteBuilder(fs, datacache_options), _datafile_path(std::move(datafile_path)) {}
    ~ParquetPositionDeleteBuilder() override = default;

    Status build(const std::string& timezone, const std::string& delete_file_path, int64_t file_length,
                 std::set<int64_t>* need_skip_rowids) override;

private:
    std::string _datafile_path;
};

class IcebergDeleteBuilder {
public:
    IcebergDeleteBuilder(FileSystem* fs, std::string datafile_path, std::set<int64_t>* need_skip_rowids,
                         const DataCacheOptions& datacache_options = DataCacheOptions())
            : _fs(fs),
              _datafile_path(std::move(datafile_path)),
              _need_skip_rowids(need_skip_rowids),
              _datacache_options(datacache_options) {}
    ~IcebergDeleteBuilder() = default;

    Status build_orc(const std::string& timezone, const TIcebergDeleteFile& delete_file,
                     const std::vector<SlotDescriptor*>& slots, RuntimeState* state,
                     std::shared_ptr<DefaultMORProcessor> mor_processor) const {
        if (delete_file.file_content == TIcebergFileContent::POSITION_DELETES) {
            return ORCPositionDeleteBuilder(_fs, _datacache_options, _datafile_path)
                    .build(timezone, delete_file.full_path, delete_file.length, _need_skip_rowids);
        } else if (delete_file.file_content == TIcebergFileContent::EQUALITY_DELETES) {
            return ORCEqualityDeleteBuilder(_fs, _datacache_options, _datafile_path)
                    .build(timezone, delete_file.full_path, delete_file.length, std::move(mor_processor),
                           std::move(slots), nullptr, nullptr, state);
        } else {
            const auto s = strings::Substitute("Unsupported iceberg file content: $0", delete_file.file_content);
            LOG(WARNING) << s;
            return Status::InternalError(s);
        }
    }

    Status build_parquet(const std::string& timezone, const TIcebergDeleteFile& delete_file,
                         const std::vector<SlotDescriptor*>& slots, TupleDescriptor* delete_column_tuple_desc,
                         const TIcebergSchema* iceberg_equal_delete_schema, RuntimeState* state,
                         std::shared_ptr<DefaultMORProcessor> mor_processor) const {
        if (delete_file.file_content == TIcebergFileContent::POSITION_DELETES) {
            return ParquetPositionDeleteBuilder(_fs, _datacache_options, _datafile_path)
                    .build(timezone, delete_file.full_path, delete_file.length, _need_skip_rowids);
        } else if (delete_file.file_content == TIcebergFileContent::EQUALITY_DELETES) {
            return ParquetEqualityDeleteBuilder(_fs, _datacache_options, _datafile_path)
                    .build(timezone, delete_file.full_path, delete_file.length, std::move(mor_processor),
                           std::move(slots), delete_column_tuple_desc, iceberg_equal_delete_schema, state);
        } else {
            auto s = strings::Substitute("Unsupported iceberg file content: $0", delete_file.file_content);
            LOG(WARNING) << s;
            return Status::InternalError(s);
        }
    }

private:
    FileSystem* _fs;
    std::string _datafile_path;
    std::set<int64_t>* _need_skip_rowids;
    const DataCacheOptions _datacache_options;
};

class IcebergDeleteFileMeta {
public:
    IcebergDeleteFileMeta() = default;
    ~IcebergDeleteFileMeta() = default;

    static SlotDescriptor& get_delete_file_path_slot();
    static SlotDescriptor& get_delete_file_pos_slot();

private:
    static SlotDescriptor gen_slot_helper(const IcebergColumnMeta& meta);
};
} // namespace starrocks