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

#include <string>

#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/schema.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

enum SortOrder {
    SIGNED,
    UNSIGNED,
    UNKNOWN,
};

// port from https://github.com/apache/arrow/blob/da6dbd48607089d716505054176e345b704570c5/cpp/src/parquet/metadata.h#L54
class ApplicationVersion {
public:
    // Known Versions with Issues
    static const ApplicationVersion& PARQUET_251_FIXED_VERSION();
    static const ApplicationVersion& PARQUET_816_FIXED_VERSION();
    static const ApplicationVersion& PARQUET_CPP_FIXED_STATS_VERSION();
    static const ApplicationVersion& PARQUET_MR_FIXED_STATS_VERSION();
    static const ApplicationVersion& PARQUET_CPP_10353_FIXED_VERSION();

    // Application that wrote the file. e.g. "IMPALA"
    std::string application_;
    // Build name
    std::string build_;

    // Version of the application that wrote the file, expressed as
    // (<major>.<minor>.<patch>). Unmatched parts default to 0.
    // "1.2.3"    => {1, 2, 3}
    // "1.2"      => {1, 2, 0}
    // "1.2-cdh5" => {1, 2, 0}
    struct {
        int major;
        int minor;
        int patch;
        std::string unknown;
        std::string pre_release;
        std::string build_info;
    } version;

    ApplicationVersion() = default;
    explicit ApplicationVersion(const std::string& created_by);
    ApplicationVersion(std::string application, int major, int minor, int patch);

    // Returns true if version is strictly less than other_version
    bool VersionLt(const ApplicationVersion& other_version) const;

    // Returns true if version is strictly equal with other_version
    bool VersionEq(const ApplicationVersion& other_version) const;

    // Checks if the Version has the correct statistics for a given column
    bool HasCorrectStatistics(const tparquet::ColumnMetaData& column_meta, const SortOrder& sort_order) const;
};

// Class corresponding to FileMetaData in thrift
class FileMetaData {
public:
    FileMetaData() = default;
    ~FileMetaData() = default;

    Status init(tparquet::FileMetaData& t_metadata, bool case_sensitive);

    uint64_t num_rows() const { return _num_rows; }

    std::string debug_string() const;

    const tparquet::FileMetaData& t_metadata() const { return _t_metadata; }

    const SchemaDescriptor& schema() const { return _schema; }

    const ApplicationVersion& writer_version() const { return _writer_version; }

private:
    tparquet::FileMetaData _t_metadata;
    uint64_t _num_rows{0};
    SchemaDescriptor _schema;
    ApplicationVersion _writer_version;
};

using FileMetaDataPtr = std::shared_ptr<FileMetaData>;

// FileMetaDataParser parse FileMetaData through below way:
// 1. try to reuse SplitContext's FileMetaData
// 2. if DataCache is enabled, retrieve FileMetaData from DataCache. Otherwise, parse FileMetaData normally
class FileMetaDataParser {
public:
    FileMetaDataParser(RandomAccessFile* file, const HdfsScannerContext* scanner_context, ObjectCache* cache,
                       const DataCacheOptions* datacache_options, uint64_t file_size)
            : _file(file),
              _scanner_ctx(scanner_context),
              _cache(cache),
              _datacache_options(datacache_options),
              _file_size(file_size) {}
    StatusOr<FileMetaDataPtr> get_file_metadata();

private:
    Status _parse_footer(FileMetaDataPtr* file_metadata_ptr, int64_t* file_metadata_size);
    StatusOr<uint32_t> _get_footer_read_size() const;
    StatusOr<uint32_t> _parse_metadata_length(const std::vector<char>& footer_buff) const;
    static std::string _build_metacache_key(const std::string& filename, int64_t modification_time, uint64_t file_size);
    RandomAccessFile* _file = nullptr;
    const HdfsScannerContext* _scanner_ctx = nullptr;
    ObjectCache* _cache = nullptr;
    const DataCacheOptions* _datacache_options = nullptr;
    uint64_t _file_size = 0;

    // contains magic number (4 bytes) and footer length (4 bytes)
    constexpr static const uint32_t PARQUET_FOOTER_SIZE = 8;
    constexpr static const uint64_t DEFAULT_FOOTER_BUFFER_SIZE = 48 * 1024;
    constexpr static const char* PARQUET_MAGIC_NUMBER = "PAR1";
    constexpr static const char* PARQUET_EMAIC_NUMBER = "PARE";
};

SortOrder sort_order_of_logical_type(LogicalType type);

} // namespace starrocks::parquet
