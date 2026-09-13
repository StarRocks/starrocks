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

#include <optional>
#include <string>

#include "cache/cache_options.h"
#include "cache/mem_cache/page_handle_fwd.h"
#include "common/status.h"
#include "formats/parquet/schema.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "types/logical_type.h"

namespace starrocks {
class StoragePageCache;
} // namespace starrocks

namespace starrocks {
class TParquetEncryptionInfo;
}

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

    // ARROW-17100: [C++][Parquet] Fix backwards compatibility for ParquetV2 data pages written prior to 3.0.0 per ARROW-10353 #13665
    // https://github.com/apache/arrow/pull/13665/files
    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    bool IsAlwaysCompressed() const;
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

    // Immutable Parquet Modular Encryption essentials for this file, populated when
    // the footer was encrypted (Iceberg encrypted table). Each reader builds its own
    // decryptor from these (the parquet Decryptor mutates AAD per page and must not be shared).
    //
    // Deliberately holds NO key material. This object is inserted into the process-wide
    // metadata cache (FileMetaDataParser::get_file_metadata), keyed only by path, mtime and
    // size -- so caching the DEK here would (a) retain plaintext key material in a shared,
    // LRU-evicted, never-zeroized cache for every encrypted file ever scanned, and (b) let a
    // cache hit skip the "no decryption key was provided by the planner" refusal, which lives
    // on the cache-miss path. The DEK travels per scan range instead and is read from
    // ColumnReaderOptions::parquet_encryption_info at decryptor-build time, so that check is
    // unconditional. Everything below is already recoverable from the file itself.
    struct EncryptionContext {
        std::string key_metadata;    // file's footer key metadata (from FileCryptoMetaData)
        std::string aad_file_unique; // file AAD token (from FileCryptoMetaData)
        std::string aad_prefix;      // effective prefix: stored in the file, or supplied by FE
        int32_t algorithm = 0;       // parquet::ParquetCipher::type as int
    };
    bool is_encrypted() const { return _encryption_ctx.has_value(); }
    const std::optional<EncryptionContext>& encryption_ctx() const { return _encryption_ctx; }
    void set_encryption_ctx(EncryptionContext ctx) { _encryption_ctx = std::move(ctx); }

private:
    tparquet::FileMetaData _t_metadata;
    uint64_t _num_rows{0};
    SchemaDescriptor _schema;
    ApplicationVersion _writer_version;
    std::optional<EncryptionContext> _encryption_ctx;
};

using FileMetaDataPtr = std::shared_ptr<FileMetaData>;

// Parquet Modular Encryption module types, mirroring arrow's encryption_internal.h. Duplicated as
// plain constants so callers can name a module without including arrow's private encryption headers
// (these values are fixed by parquet-format; they are part of the on-disk AAD, not an arrow detail).
constexpr int8_t kPmeModuleColumnIndex = 6;
constexpr int8_t kPmeModuleOffsetIndex = 7;
constexpr int8_t kPmeModuleBloomFilterHeader = 8;
constexpr int8_t kPmeModuleBloomFilterBitset = 9;

// Decrypt one Parquet Modular Encryption *metadata* module of this file -- the page index
// (kColumnIndex / kOffsetIndex) or a bloom filter part -- into `plaintext`.
//
// Each module carries its own AAD, derived from (module type, row group, column, page). Metadata
// modules use the file's footer key with GCM even in a GCM_CTR file, and pass kNonPageOrdinal, so
// they cannot be decrypted with the page-data decryptor. arrow writes them length-prefixed
// ([4-byte len][nonce][ciphertext][tag]) and `ciphertext_len` is the whole module, which the
// ColumnChunk metadata gives exactly (column_index_length / offset_index_length).
//
// Declared here, next to EncryptionContext, so callers do not need the private arrow encryption
// headers; the DEK comes from the scan range because it is deliberately not cached with the footer.
Status decrypt_metadata_module(const FileMetaData& file_metadata, const TParquetEncryptionInfo* encryption_info,
                               int8_t module_type, int16_t row_group_ordinal, int16_t column_ordinal,
                               const uint8_t* ciphertext, size_t ciphertext_len, std::string* plaintext);

// FileMetaDataParser parse FileMetaData through below way:
// 1. try to reuse SplitContext's FileMetaData
// 2. if DataCache is enabled, retrieve FileMetaData from DataCache. Otherwise, parse FileMetaData normally
class FileMetaDataParser {
public:
    FileMetaDataParser(RandomAccessFile* file, const FormatScanContext* scanner_context, StoragePageCache* cache,
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
    StatusOr<uint32_t> _parse_metadata_length(const std::vector<char>& footer_buff, bool* is_encrypted) const;
    // Decrypt and deserialize an encrypted (PARE) Parquet footer using the per-file
    // DEK supplied by FE on the scan range. footer_len is the combined length of the
    // FileCryptoMetaData plus the encrypted FileMetaData (the value before the magic).
    Status _decrypt_and_deserialize_footer(const std::vector<char>& footer_buffer, uint32_t footer_len,
                                           FileMetaDataPtr* file_metadata_ptr, int64_t* file_metadata_size);
    RandomAccessFile* _file = nullptr;
    const FormatScanContext* _scanner_ctx = nullptr;
    StoragePageCache* _cache = nullptr;
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
