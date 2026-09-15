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

#include "formats/parquet/metadata.h"

#include <arrow/memory_pool.h>
#include <arrow/util/secure_string.h>
#include <glog/logging.h>
#include <parquet/encryption/encryption.h>
#include <parquet/encryption/encryption_internal.h>
#include <parquet/encryption/internal_file_decryptor.h>
#include <parquet/metadata.h>

#include <cstdlib>
#include <span>
#include <sstream>
#include <string_view>
#include <utility>

#include "base/coding.h"
#include "cache/mem_cache/page_cache.h"
#include "common/util/thrift_util.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/split_context.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"

namespace starrocks::parquet {

Status FileMetaData::init(tparquet::FileMetaData& t_metadata, bool case_sensitive) {
    // construct schema from thrift
    RETURN_IF_ERROR(_schema.from_thrift(t_metadata.schema, case_sensitive));
    _num_rows = t_metadata.num_rows;
    tparquet::swap(_t_metadata, t_metadata);
    if (_t_metadata.__isset.created_by) {
        _writer_version = ApplicationVersion(_t_metadata.created_by);
    } else {
        _writer_version = ApplicationVersion("unknown 0.0.0");
    }
    return Status::OK();
}

std::string FileMetaData::debug_string() const {
    std::stringstream ss;
    ss << "schema=" << _schema.debug_string();
    return ss.str();
}

const ApplicationVersion& ApplicationVersion::PARQUET_251_FIXED_VERSION() {
    static ApplicationVersion version("parquet-mr", 1, 8, 0);
    return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_816_FIXED_VERSION() {
    static ApplicationVersion version("parquet-mr", 1, 2, 9);
    return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_CPP_FIXED_STATS_VERSION() {
    static ApplicationVersion version("parquet-cpp", 1, 3, 0);
    return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_MR_FIXED_STATS_VERSION() {
    static ApplicationVersion version("parquet-mr", 1, 10, 0);
    return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION() {
    // parquet-cpp versions released prior to Arrow 3.0 would write DataPageV2 pages
    // with is_compressed==0 but still write compressed data. (See: ARROW-10353).
    // Parquet 1.5.1 had this problem, and after that we switched to the
    // application name "parquet-cpp-arrow", so this version is fake.
    static ApplicationVersion version("parquet-cpp", 2, 0, 0);
    return version;
}

ApplicationVersion::ApplicationVersion(std::string application, int major, int minor, int patch)
        : application_(std::move(application)), version{major, minor, patch, "", "", ""} {}

namespace {
// Parse the application version format and set parsed values to
// ApplicationVersion.
//
// The application version format must be compatible parquet-mr's
// one. See also:
//   * https://github.com/apache/parquet-mr/blob/master/parquet-common/src/main/java/org/apache/parquet/VersionParser.java
//   * https://github.com/apache/parquet-mr/blob/master/parquet-common/src/main/java/org/apache/parquet/SemanticVersion.java
//
// The application version format:
//   "${APPLICATION_NAME}"
//   "${APPLICATION_NAME} version ${VERSION}"
//   "${APPLICATION_NAME} version ${VERSION} (build ${BUILD_NAME})"
//
// Eg:
//   parquet-cpp
//   parquet-cpp version 1.5.0ab-xyz5.5.0+cd
//   parquet-cpp version 1.5.0ab-xyz5.5.0+cd (build abcd)
//
// The VERSION format:
//   "${MAJOR}"
//   "${MAJOR}.${MINOR}"
//   "${MAJOR}.${MINOR}.${PATCH}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}-${PRE_RELEASE}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}-${PRE_RELEASE}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}-${PRE_RELEASE}"
//   "${MAJOR}.${MINOR}.${PATCH}-${PRE_RELEASE}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}+${BUILD_INFO}"
//
// Eg:
//   1
//   1.5
//   1.5.0
//   1.5.0ab
//   1.5.0ab-cdh5.5.0
//   1.5.0ab-cdh5.5.0+cd
//   1.5.0ab+cd
//   1.5.0-cdh5.5.0
//   1.5.0-cdh5.5.0+cd
//   1.5.0+cd
class ApplicationVersionParser {
public:
    ApplicationVersionParser(const std::string& created_by, ApplicationVersion& application_version)
            : created_by_(created_by),
              application_version_(application_version),
              spaces_(" \t\v\r\n\f"),
              digits_("0123456789") {}

    void Parse() {
        application_version_.application_ = "unknown";
        application_version_.version = {0, 0, 0, "", "", ""};

        if (!ParseApplicationName()) {
            return;
        }
        if (!ParseVersion()) {
            return;
        }
        if (!ParseBuildName()) {
            return;
        }
    }

private:
    bool IsSpace(const std::string& string, const size_t& offset) {
        auto target = ::std::string_view(string).substr(offset, 1);
        return target.find_first_of(spaces_) != ::std::string_view::npos;
    }

    void RemovePrecedingSpaces(const std::string& string, size_t& start, const size_t& end) {
        while (start < end && IsSpace(string, start)) {
            ++start;
        }
    }

    void RemoveTrailingSpaces(const std::string& string, const size_t& start, size_t& end) {
        while (start < (end - 1) && (end - 1) < string.size() && IsSpace(string, end - 1)) {
            --end;
        }
    }

    bool ParseApplicationName() {
        std::string version_mark(" version ");
        auto version_mark_position = created_by_.find(version_mark);
        size_t application_name_end;
        // No VERSION and BUILD_NAME.
        if (version_mark_position == std::string::npos) {
            version_start_ = std::string::npos;
            application_name_end = created_by_.size();
        } else {
            version_start_ = version_mark_position + version_mark.size();
            application_name_end = version_mark_position;
        }

        size_t application_name_start = 0;
        RemovePrecedingSpaces(created_by_, application_name_start, application_name_end);
        RemoveTrailingSpaces(created_by_, application_name_start, application_name_end);
        application_version_.application_ =
                created_by_.substr(application_name_start, application_name_end - application_name_start);

        return true;
    }

    bool ParseVersion() {
        // No VERSION.
        if (version_start_ == std::string::npos) {
            return false;
        }

        RemovePrecedingSpaces(created_by_, version_start_, created_by_.size());
        version_end_ = created_by_.find(" (", version_start_);
        // No BUILD_NAME.
        if (version_end_ == std::string::npos) {
            version_end_ = created_by_.size();
        }
        RemoveTrailingSpaces(created_by_, version_start_, version_end_);
        // No VERSION.
        if (version_start_ == version_end_) {
            return false;
        }
        version_string_ = created_by_.substr(version_start_, version_end_ - version_start_);

        if (!ParseVersionMajor()) {
            return false;
        }
        if (!ParseVersionMinor()) {
            return false;
        }
        if (!ParseVersionPatch()) {
            return false;
        }
        if (!ParseVersionUnknown()) {
            return false;
        }
        if (!ParseVersionPreRelease()) {
            return false;
        }
        if (!ParseVersionBuildInfo()) {
            return false;
        }

        return true;
    }

    bool ParseVersionMajor() {
        size_t version_major_start = 0;
        auto version_major_end = version_string_.find_first_not_of(digits_);
        // MAJOR only.
        if (version_major_end == std::string::npos) {
            version_major_end = version_string_.size();
            version_parsing_position_ = version_major_end;
        } else {
            // No ".".
            if (version_string_[version_major_end] != '.') {
                return false;
            }
            // No MAJOR.
            if (version_major_end == version_major_start) {
                return false;
            }
            version_parsing_position_ = version_major_end + 1; // +1 is for '.'.
        }
        auto version_major_string =
                version_string_.substr(version_major_start, version_major_end - version_major_start);
        application_version_.version.major = atoi(version_major_string.c_str());
        return true;
    }

    bool ParseVersionMinor() {
        auto version_minor_start = version_parsing_position_;
        auto version_minor_end = version_string_.find_first_not_of(digits_, version_minor_start);
        // MAJOR.MINOR only.
        if (version_minor_end == std::string::npos) {
            version_minor_end = version_string_.size();
            version_parsing_position_ = version_minor_end;
        } else {
            // No ".".
            if (version_string_[version_minor_end] != '.') {
                return false;
            }
            // No MINOR.
            if (version_minor_end == version_minor_start) {
                return false;
            }
            version_parsing_position_ = version_minor_end + 1; // +1 is for '.'.
        }
        auto version_minor_string =
                version_string_.substr(version_minor_start, version_minor_end - version_minor_start);
        application_version_.version.minor = atoi(version_minor_string.c_str());
        return true;
    }

    bool ParseVersionPatch() {
        auto version_patch_start = version_parsing_position_;
        auto version_patch_end = version_string_.find_first_not_of(digits_, version_patch_start);
        // No UNKNOWN, PRE_RELEASE and BUILD_INFO.
        if (version_patch_end == std::string::npos) {
            version_patch_end = version_string_.size();
        }
        // No PATCH.
        if (version_patch_end == version_patch_start) {
            return false;
        }
        auto version_patch_string =
                version_string_.substr(version_patch_start, version_patch_end - version_patch_start);
        application_version_.version.patch = atoi(version_patch_string.c_str());
        version_parsing_position_ = version_patch_end;
        return true;
    }

    bool ParseVersionUnknown() {
        // No UNKNOWN.
        if (version_parsing_position_ == version_string_.size()) {
            return true;
        }
        auto version_unknown_start = version_parsing_position_;
        auto version_unknown_end = version_string_.find_first_of("-+", version_unknown_start);
        // No PRE_RELEASE and BUILD_INFO
        if (version_unknown_end == std::string::npos) {
            version_unknown_end = version_string_.size();
        }
        application_version_.version.unknown =
                version_string_.substr(version_unknown_start, version_unknown_end - version_unknown_start);
        version_parsing_position_ = version_unknown_end;
        return true;
    }

    bool ParseVersionPreRelease() {
        // No PRE_RELEASE.
        if (version_parsing_position_ == version_string_.size() || version_string_[version_parsing_position_] != '-') {
            return true;
        }

        auto version_pre_release_start = version_parsing_position_ + 1; // +1 is for '-'.
        auto version_pre_release_end = version_string_.find_first_of('+', version_pre_release_start);
        // No BUILD_INFO
        if (version_pre_release_end == std::string::npos) {
            version_pre_release_end = version_string_.size();
        }
        application_version_.version.pre_release =
                version_string_.substr(version_pre_release_start, version_pre_release_end - version_pre_release_start);
        version_parsing_position_ = version_pre_release_end;
        return true;
    }

    bool ParseVersionBuildInfo() {
        // No BUILD_INFO.
        if (version_parsing_position_ == version_string_.size() || version_string_[version_parsing_position_] != '+') {
            return true;
        }

        auto version_build_info_start = version_parsing_position_ + 1; // +1 is for '+'.
        application_version_.version.build_info = version_string_.substr(version_build_info_start);
        return true;
    }

    bool ParseBuildName() {
        std::string build_mark(" (build ");
        auto build_mark_position = created_by_.find(build_mark, version_end_);
        // No BUILD_NAME.
        if (build_mark_position == std::string::npos) {
            return false;
        }
        auto build_name_start = build_mark_position + build_mark.size();
        RemovePrecedingSpaces(created_by_, build_name_start, created_by_.size());
        auto build_name_end = created_by_.find_first_of(')', build_name_start);
        // No end ")".
        if (build_name_end == std::string::npos) {
            return false;
        }
        RemoveTrailingSpaces(created_by_, build_name_start, build_name_end);
        application_version_.build_ = created_by_.substr(build_name_start, build_name_end - build_name_start);

        return true;
    }

    const std::string& created_by_;
    ApplicationVersion& application_version_;

    // For parsing.
    std::string spaces_;
    std::string digits_;
    size_t version_parsing_position_;
    size_t version_start_;
    size_t version_end_;
    std::string version_string_;
};
} // namespace

ApplicationVersion::ApplicationVersion(const std::string& created_by) {
    ApplicationVersionParser parser(created_by, *this);
    parser.Parse();
}

bool ApplicationVersion::VersionLt(const ApplicationVersion& other_version) const {
    if (application_ != other_version.application_) return false;

    if (version.major < other_version.version.major) return true;
    if (version.major > other_version.version.major) return false;
    DCHECK_EQ(version.major, other_version.version.major);
    if (version.minor < other_version.version.minor) return true;
    if (version.minor > other_version.version.minor) return false;
    DCHECK_EQ(version.minor, other_version.version.minor);
    return version.patch < other_version.version.patch;
}

bool ApplicationVersion::VersionEq(const ApplicationVersion& other_version) const {
    return application_ == other_version.application_ && version.major == other_version.version.major &&
           version.minor == other_version.version.minor && version.patch == other_version.version.patch;
}

bool ApplicationVersion::HasCorrectStatistics(const tparquet::ColumnMetaData& column_meta,
                                              const SortOrder& sort_order) const {
    // parquet-cpp version 1.3.0 and parquet-mr 1.10.0 onwards stats are computed
    // correctly for all types
    if (VersionLt(ApplicationVersion::PARQUET_MR_FIXED_STATS_VERSION()) ||
        VersionLt(ApplicationVersion::PARQUET_CPP_FIXED_STATS_VERSION())) {
        // Only SIGNED are valid unless max and min are the same
        // (in which case the sort order does not matter)
        auto min_equals_max = (column_meta.statistics.__isset.min_value && column_meta.statistics.__isset.max_value &&
                               column_meta.statistics.min_value == column_meta.statistics.max_value) ||
                              (column_meta.statistics.__isset.min && column_meta.statistics.__isset.max &&
                               column_meta.statistics.min == column_meta.statistics.max);
        if (SortOrder::SIGNED != sort_order && !min_equals_max) {
            return false;
        }

        auto col_type = column_meta.type;
        // Statistics of other types are OK
        if (col_type != ::tparquet::Type::FIXED_LEN_BYTE_ARRAY && col_type != ::tparquet::Type::BYTE_ARRAY) {
            return true;
        }
    }

    // created_by is not populated, which could have been caused by
    // parquet-mr during the same time as PARQUET-251, see PARQUET-297
    if (application_ == "unknown") {
        return true;
    }

    if (SortOrder::UNKNOWN == sort_order) {
        return false;
    }

    // PARQUET-251
    if (VersionLt(ApplicationVersion::PARQUET_251_FIXED_VERSION())) {
        return false;
    }

    return true;
}

bool ApplicationVersion::IsAlwaysCompressed() const {
    return VersionLt(PARQUET_CPP_10353_FIXED_VERSION());
}

StatusOr<FileMetaDataPtr> FileMetaDataParser::get_file_metadata() {
    // return from split_context directly
    if (_scanner_ctx->split_context != nullptr) {
        auto split_ctx = down_cast<const SplitContext*>(_scanner_ctx->split_context);
        return split_ctx->file_metadata;
    }

    // parse FileMetadata from remote
    if (!_cache) {
        int64_t file_metadata_size = 0;
        FileMetaDataPtr file_metadata_ptr = nullptr;
        RETURN_IF_ERROR(_parse_footer(&file_metadata_ptr, &file_metadata_size));
        return file_metadata_ptr;
    }

    PageCacheHandle cache_handle;
    std::string metacache_key = ParquetUtils::get_file_cache_key(CacheType::META, _file->filename(),
                                                                 _datacache_options->modification_time, _file_size);
    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_cache_read_ns);
        bool ret = _cache->lookup(metacache_key, &cache_handle);
        if (ret) {
            _scanner_ctx->stats->footer_cache_read_count += 1;
            return *(reinterpret_cast<const FileMetaDataPtr*>(cache_handle.data()));
        }
    }

    FileMetaDataPtr file_metadata = nullptr;
    int64_t file_metadata_size = 0;
    RETURN_IF_ERROR(_parse_footer(&file_metadata, &file_metadata_size));
    if (file_metadata_size > 0) {
        auto deleter = [](const starrocks::CacheKey& key, void* value) { delete (FileMetaDataPtr*)value; };
        MemCacheWriteOptions options;
        options.evict_probability = _datacache_options->datacache_evict_probability;
        auto capture = std::make_unique<FileMetaDataPtr>(file_metadata);
        Status st = _cache->insert(metacache_key, (void*)(capture.get()), file_metadata_size, deleter, options,
                                   &cache_handle);
        if (st.ok()) {
            _scanner_ctx->stats->footer_cache_write_bytes += file_metadata_size;
            _scanner_ctx->stats->footer_cache_write_count += 1;
            capture.release();
            return file_metadata;
        } else {
            _scanner_ctx->stats->footer_cache_write_fail_count += 1;
            return file_metadata;
        }
    } else {
        return Status::InternalError(
                fmt::format("Parsing unexpected parquet file metadata size {}", file_metadata_size));
    }
}

Status decrypt_metadata_module(const FileMetaData& file_metadata, const TParquetEncryptionInfo* encryption_info,
                               int8_t module_type, int16_t row_group_ordinal, int16_t column_ordinal,
                               const uint8_t* ciphertext, size_t ciphertext_len, std::string* plaintext) {
    if (!file_metadata.is_encrypted()) {
        return Status::InternalError("decrypt_metadata_module called for a file that is not encrypted");
    }
    // Same rule as the page path: the key travels per scan range, so an encrypted file with no
    // planner-supplied key is refused rather than read.
    if (encryption_info == nullptr || !encryption_info->__isset.file_dek || encryption_info->file_dek.empty()) {
        return Status::NotSupported(
                "cannot read encrypted Parquet file: no decryption key was provided by the planner");
    }
    if (ciphertext_len == 0) {
        return Status::Corruption("empty encrypted Parquet metadata module");
    }

    const auto& ctx = *file_metadata.encryption_ctx();
    try {
        const auto cipher = static_cast<::parquet::ParquetCipher::type>(ctx.algorithm);
        auto props = ::parquet::FileDecryptionProperties::Builder()
                             .footer_key(::arrow::util::SecureString(std::string(encryption_info->file_dek)))
                             ->build();
        const std::string file_aad = ctx.aad_prefix + ctx.aad_file_unique;
        auto file_decryptor = std::make_shared<::parquet::InternalFileDecryptor>(
                props, file_aad, cipher, ctx.key_metadata, ::arrow::default_memory_pool());
        const auto& footer_key = file_decryptor->GetFooterKey();
        const auto key_len = static_cast<int32_t>(footer_key.size());
        // metadata=true forces GCM, which is what the writer used for these modules even in a
        // GCM_CTR file. The aad passed here is a placeholder; UpdateAad below sets the real one.
        auto decryptor = std::make_shared<::parquet::Decryptor>(
                ::parquet::encryption::AesDecryptor::Make(cipher, key_len, /*metadata=*/true), footer_key, file_aad,
                /*aad=*/"", ::arrow::default_memory_pool());
        decryptor->UpdateAad(::parquet::encryption::CreateModuleAad(
                decryptor->file_aad(), module_type, row_group_ordinal, column_ordinal, ::parquet::kNonPageOrdinal));

        const int32_t plain_cap = decryptor->PlaintextLength(static_cast<int32_t>(ciphertext_len));
        plaintext->resize(static_cast<size_t>(plain_cap));
        const int plain_len = decryptor->Decrypt(
                std::span<const uint8_t>(ciphertext, ciphertext_len),
                std::span<uint8_t>(reinterpret_cast<uint8_t*>(plaintext->data()), static_cast<size_t>(plain_cap)));
        plaintext->resize(static_cast<size_t>(plain_len));
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption(fmt::format("failed to decrypt Parquet metadata module {}: {}",
                                              static_cast<int>(module_type), e.what()));
    }
    return Status::OK();
}

Status FileMetaDataParser::_parse_footer(FileMetaDataPtr* file_metadata_ptr, int64_t* file_metadata_size) {
    std::vector<char> footer_buffer;
    ASSIGN_OR_RETURN(uint32_t footer_read_size, _get_footer_read_size());
    footer_buffer.resize(footer_read_size);

    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
        RETURN_IF_ERROR(_file->read_at_fully(_file_size - footer_read_size, footer_buffer.data(), footer_read_size));
    }

    bool is_encrypted_footer = false;
    ASSIGN_OR_RETURN(uint32_t metadata_length, _parse_metadata_length(footer_buffer, &is_encrypted_footer));

    _scanner_ctx->stats->request_bytes_read += metadata_length + PARQUET_FOOTER_SIZE;
    _scanner_ctx->stats->request_bytes_read_uncompressed += metadata_length + PARQUET_FOOTER_SIZE;

    if (footer_read_size < (metadata_length + PARQUET_FOOTER_SIZE)) {
        // footer_buffer's size is not enough to read the whole metadata, we need to re-read for larger size
        size_t re_read_size = metadata_length + PARQUET_FOOTER_SIZE;
        footer_buffer.resize(re_read_size);
        {
            SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
            RETURN_IF_ERROR(_file->read_at_fully(_file_size - re_read_size, footer_buffer.data(), re_read_size));
        }
    }

    if (is_encrypted_footer) {
        return _decrypt_and_deserialize_footer(footer_buffer, metadata_length, file_metadata_ptr, file_metadata_size);
    }

    // NOTICE: When you need to modify the logic within this scope (including the subfuctions), you should be
    // particularly careful to ensure that it does not affect the correctness of the footer's memory statistics.
    {
        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        tparquet::FileMetaData t_metadata;
        // deserialize footer
        RETURN_IF_ERROR(deserialize_thrift_msg(reinterpret_cast<const uint8*>(footer_buffer.data()) +
                                                       footer_buffer.size() - PARQUET_FOOTER_SIZE - metadata_length,
                                               &metadata_length, TProtocolType::COMPACT, &t_metadata));

        *file_metadata_ptr = std::make_shared<FileMetaData>();
        FileMetaData* file_metadata = file_metadata_ptr->get();
        RETURN_IF_ERROR(file_metadata->init(t_metadata, _scanner_ctx->options.case_sensitive));
        *file_metadata_size = CurrentThread::current().get_consumed_bytes() - before_bytes;
    }
#if defined(BE_TEST) || defined(__SANITIZE_ADDRESS__) || defined(ADDRESS_SANITIZER)
    *file_metadata_size = sizeof(FileMetaData);
#endif
    return Status::OK();
}

namespace {
// Hand-rolled compact-thrift parse of parquet's FileCryptoMetaData.
//
// We deliberately avoid parquet-cpp's FileCryptoMetaData::Make: it deserializes via
// the arrow-bundled thrift, whose TCompactProtocol ABI differs from the thrift the
// StarRocks BE binary links, so a virtual call inside readStructEnd() segfaults even
// on valid input. The struct is tiny and fixed, so we parse the few fields we need
// directly. Validated byte-for-byte against FileCryptoMetaData::Make (algorithm,
// aad_file_unique, key_metadata, and consumed length) for AES_GCM_V1 and AES_GCM_CTR_V1.
//
// FileCryptoMetaData { 1: EncryptionAlgorithm encryption_algorithm; 2: binary key_metadata }
// EncryptionAlgorithm union { 1: AesGcmV1; 2: AesGcmCtrV1 }
// AesGcm*    { 1: binary aad_prefix; 2: binary aad_file_unique; 3: bool supply_aad_prefix }
struct ParsedCryptoMetadata {
    ::parquet::ParquetCipher::type algorithm = ::parquet::ParquetCipher::AES_GCM_V1;
    std::string aad_prefix;
    // True when the writer deliberately kept the AAD prefix OUT of the file, so the reader must
    // supply it. Iceberg's standard writer does this and records the prefix in key_metadata
    // instead; that is what makes whole-file substitution detectable.
    bool supply_aad_prefix = false;
    std::string aad_file_unique;
    std::string key_metadata;
    size_t consumed = 0;
};

// Bounds-checked compact-thrift reader; throws std::runtime_error on overrun so the
// caller can convert to a clean Status instead of crashing.
class CompactThriftReader {
public:
    CompactThriftReader(const uint8_t* data, size_t n) : _p(data), _end(data + n), _begin(data) {}

    // Struct nesting is bounded because skip(type=12) and read_struct() are mutually recursive with
    // one input byte per level: FileCryptoMetaData is plaintext and sits OUTSIDE the AEAD, so a
    // corrupt or hostile footer can nest as deeply as it has bytes and exhaust the stack -- an
    // uncatchable SIGSEGV on the scan thread, needing no key to trigger. FileCryptoMetaData nests
    // three deep; 16 is generous and still far from any stack limit.
    static constexpr int kMaxStructDepth = 16;

    uint8_t read_byte() {
        if (_p >= _end) throw std::runtime_error("compact thrift overrun");
        return *_p++;
    }
    uint64_t read_varint() {
        uint64_t r = 0;
        int shift = 0;
        while (true) {
            uint8_t b = read_byte();
            r |= static_cast<uint64_t>(b & 0x7f) << shift;
            if (!(b & 0x80)) break;
            shift += 7;
            if (shift > 63) throw std::runtime_error("compact thrift varint too long");
        }
        return r;
    }
    std::string read_binary() {
        uint64_t len = read_varint();
        if (_p + len > _end) throw std::runtime_error("compact thrift binary overrun");
        std::string s(reinterpret_cast<const char*>(_p), len);
        _p += len;
        return s;
    }
    void skip(uint8_t type) {
        switch (type) {
        case 1:
        case 2:
            break; // bool: value encoded in the type nibble
        case 3:
            read_byte();
            break; // i8
        case 4:
        case 5:
        case 6:
            read_varint();
            break; // i16/i32/i64 (zigzag varint)
        case 7:
            for (int i = 0; i < 8; i++) read_byte();
            break; // double
        case 8:
            read_binary();
            break; // binary/string
        case 9: {  // list
            uint8_t size_and_type = read_byte();
            uint32_t sz = (size_and_type >> 4) & 0x0f;
            if (sz == 0x0f) sz = static_cast<uint32_t>(read_varint());
            uint8_t elem_type = size_and_type & 0x0f;
            // A count bigger than the bytes that are left cannot be honest, and it has to be
            // rejected up front rather than discovered by read_byte(): the bool element types
            // consume NOTHING, so they never reach a bounds check and a 6-byte input could
            // otherwise spin this loop ~2^32 times and wedge the scan thread.
            if (sz > static_cast<uint64_t>(_end - _p)) {
                throw std::runtime_error("compact thrift list longer than the remaining input");
            }
            for (uint32_t i = 0; i < sz; i++) skip(elem_type);
            break;
        }
        case 12:
            skip_struct();
            break; // struct
        default:
            throw std::runtime_error("compact thrift unhandled type");
        }
    }
    // Reads struct field headers, calling handler(field_id, type). handler returns true
    // if it consumed the field's value, false to skip it. Stops at the 0x00 stop byte.
    template <class Handler>
    void read_struct(Handler&& handler) {
        DepthGuard guard(this);
        int last_id = 0;
        while (true) {
            uint8_t h = read_byte();
            if (h == 0x00) break;
            uint8_t type = h & 0x0f;
            int delta = (h & 0xf0) >> 4;
            int field_id;
            if (delta != 0) {
                field_id = last_id + delta;
            } else {
                uint64_t zz = read_varint();
                field_id = static_cast<int>((zz >> 1) ^ -static_cast<int64_t>(zz & 1));
            }
            last_id = field_id;
            if (!handler(field_id, type)) skip(type);
        }
    }
    size_t consumed() const { return static_cast<size_t>(_p - _begin); }

private:
    const uint8_t* _p;
    const uint8_t* _end;
    const uint8_t* _begin;
    int _depth = 0;
    void skip_struct() {
        read_struct([](int, uint8_t) { return false; });
    }

    // RAII so the counter unwinds correctly when a nested read throws.
    class DepthGuard {
    public:
        explicit DepthGuard(CompactThriftReader* r) : _r(r) {
            if (++_r->_depth > kMaxStructDepth) {
                throw std::runtime_error("compact thrift struct nesting too deep");
            }
        }
        ~DepthGuard() { --_r->_depth; }
        DepthGuard(const DepthGuard&) = delete;
        DepthGuard& operator=(const DepthGuard&) = delete;

    private:
        CompactThriftReader* _r;
    };
};

Status parse_file_crypto_metadata(const uint8_t* data, size_t n, ParsedCryptoMetadata* out) {
    try {
        CompactThriftReader r(data, n);
        r.read_struct([&](int fid, uint8_t type) {
            if (fid == 1 && type == 12) { // encryption_algorithm union
                r.read_struct([&](int afid, uint8_t atype) {
                    if (atype == 12 && (afid == 1 || afid == 2)) {
                        out->algorithm = (afid == 1) ? ::parquet::ParquetCipher::AES_GCM_V1
                                                     : ::parquet::ParquetCipher::AES_GCM_CTR_V1;
                        r.read_struct([&](int gfid, uint8_t gtype) {
                            if (gfid == 1 && gtype == 8) {
                                out->aad_prefix = r.read_binary();
                                return true;
                            }
                            if (gfid == 2 && gtype == 8) {
                                out->aad_file_unique = r.read_binary();
                                return true;
                            }
                            if (gfid == 3 && (gtype == 1 || gtype == 2)) {
                                out->supply_aad_prefix = (gtype == 1);
                                return true;
                            }
                            return false;
                        });
                        return true;
                    }
                    return false;
                });
                return true;
            }
            if (fid == 2 && type == 8) { // key_metadata
                out->key_metadata = r.read_binary();
                return true;
            }
            return false;
        });
        out->consumed = r.consumed();
    } catch (const std::exception& e) {
        return Status::Corruption(fmt::format("failed to parse Parquet FileCryptoMetaData: {}", e.what()));
    }
    return Status::OK();
}
} // namespace

Status FileMetaDataParser::_decrypt_and_deserialize_footer(const std::vector<char>& footer_buffer, uint32_t footer_len,
                                                           FileMetaDataPtr* file_metadata_ptr,
                                                           int64_t* file_metadata_size) {
    // The per-file DEK is recovered by FE from the Iceberg file's key_metadata and delivered on
    // the scan range.
    const auto* enc_info = _scanner_ctx->parquet_encryption_info;
    if (enc_info == nullptr || !enc_info->__isset.file_dek) {
        return Status::NotSupported(
                "encountered an encrypted Parquet file but no decryption key was provided by the planner");
    }
    const std::string& dek = enc_info->file_dek;

    // Footer tail layout (Parquet Modular Encryption, encrypted footer):
    //   [FileCryptoMetaData][encrypted FileMetaData][4-byte footer_len][PARE]
    // footer_len (passed in) covers FileCryptoMetaData + the encrypted FileMetaData.
    const size_t size = footer_buffer.size();
    // Guard against an implausible footer_len before computing the offset.
    if (footer_len < 4 || size < static_cast<size_t>(PARQUET_FOOTER_SIZE) + footer_len) {
        return Status::Corruption(
                fmt::format("encrypted Parquet footer length {} inconsistent with buffer size {}", footer_len, size));
    }
    const uint8_t* crypto_md_start =
            reinterpret_cast<const uint8_t*>(footer_buffer.data()) + size - PARQUET_FOOTER_SIZE - footer_len;

    // Parse the plaintext FileCryptoMetaData (our own compact-thrift reader; see note
    // on parse_file_crypto_metadata for why we avoid arrow's FileCryptoMetaData::Make).
    ParsedCryptoMetadata crypto_md;
    RETURN_IF_ERROR(parse_file_crypto_metadata(crypto_md_start, footer_len, &crypto_md));
    const uint32_t crypto_metadata_len = static_cast<uint32_t>(crypto_md.consumed);
    const std::string& key_metadata = crypto_md.key_metadata;
    // File AAD = aad_prefix + the file's unique token. The prefix comes from one of two places:
    //   - stored in the file, when the writer chose to store it, or
    //   - supplied by the reader, when the writer set supply_aad_prefix to keep it out of the file.
    //     Iceberg's standard writer does that and records the prefix in key_metadata, so FE parses
    //     it out of StandardKeyMetadata and sends it on the scan range. StarRocks writes files this
    //     way too.
    // Getting this wrong is not a soft failure: a mismatched prefix fails GCM verification and the
    // file reads as corrupt.
    std::string aad_prefix = crypto_md.aad_prefix;
    if (aad_prefix.empty() && crypto_md.supply_aad_prefix) {
        if (!enc_info->__isset.aad_prefix || enc_info->aad_prefix.empty()) {
            return Status::NotSupported(
                    "encrypted Parquet file requires an externally supplied AAD prefix "
                    "(supply_aad_prefix is set) but the planner did not provide one");
        }
        aad_prefix = enc_info->aad_prefix;
    }
    const std::string file_aad = aad_prefix + crypto_md.aad_file_unique;

    // Build a transient decryptor with the footer key (DEK) just to decrypt the footer.
    std::shared_ptr<::parquet::FileDecryptionProperties> decryption_props;
    std::shared_ptr<::parquet::InternalFileDecryptor> file_decryptor;
    std::vector<uint8_t> plaintext;
    int plaintext_len = 0;
    try {
        decryption_props = ::parquet::FileDecryptionProperties::Builder()
                                   .footer_key(::arrow::util::SecureString(std::string(dek)))
                                   ->build();
        file_decryptor = std::make_shared<::parquet::InternalFileDecryptor>(
                decryption_props, file_aad, crypto_md.algorithm, key_metadata, ::arrow::default_memory_pool());

        const uint8_t* enc_metadata = crypto_md_start + crypto_metadata_len;
        const int enc_metadata_len = static_cast<int>(footer_len - crypto_metadata_len);

        // GetFooterDecryptor() sets the footer AAD itself, which is what we want here --
        // unlike the per-page decryptors in PageReader, this one is used exactly once.
        auto footer_decryptor = file_decryptor->GetFooterDecryptor();
        // The encrypted footer module is length-prefixed and the decryptor reads that
        // prefix, so size the output via PlaintextLength() (arrow 24 removed
        // CiphertextSizeDelta).
        const int32_t plain_cap = footer_decryptor->PlaintextLength(enc_metadata_len);
        plaintext.resize(plain_cap);
        plaintext_len =
                footer_decryptor->Decrypt(std::span<const uint8_t>(enc_metadata, static_cast<size_t>(enc_metadata_len)),
                                          std::span<uint8_t>(plaintext.data(), static_cast<size_t>(plain_cap)));
    } catch (const ::parquet::ParquetException& e) {
        return Status::Corruption(fmt::format("failed to decrypt Parquet footer: {}", e.what()));
    }

    int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
    tparquet::FileMetaData t_metadata;
    auto pt_len = static_cast<uint32_t>(plaintext_len);
    RETURN_IF_ERROR(deserialize_thrift_msg(plaintext.data(), &pt_len, TProtocolType::COMPACT, &t_metadata));

    *file_metadata_ptr = std::make_shared<FileMetaData>();
    FileMetaData* file_metadata = file_metadata_ptr->get();
    RETURN_IF_ERROR(file_metadata->init(t_metadata, _scanner_ctx->options.case_sensitive));

    // Record the immutable crypto essentials so each column reader can build its own
    // decryptor for page decryption (the parquet Decryptor mutates AAD per page and
    // must not be shared across concurrent column readers). The DEK is deliberately NOT
    // stored here: this object goes into the shared metadata cache. See EncryptionContext.
    FileMetaData::EncryptionContext enc_ctx;
    enc_ctx.key_metadata = key_metadata;
    enc_ctx.aad_file_unique = crypto_md.aad_file_unique;
    enc_ctx.aad_prefix = aad_prefix; // effective prefix: from the file, or supplied by FE
    enc_ctx.algorithm = static_cast<int32_t>(crypto_md.algorithm);
    file_metadata->set_encryption_ctx(std::move(enc_ctx));

    *file_metadata_size = CurrentThread::current().get_consumed_bytes() - before_bytes;
#ifdef BE_TEST
    *file_metadata_size = sizeof(FileMetaData);
#endif
    return Status::OK();
}

StatusOr<uint32_t> FileMetaDataParser::_get_footer_read_size() const {
    if (_file_size == 0) {
        return Status::Corruption("Parquet file size is 0 bytes");
    } else if (_file_size < PARQUET_FOOTER_SIZE) {
        return Status::Corruption(strings::Substitute(
                "Parquet file size is $0 bytes, smaller than the minimum parquet file footer ($1 bytes)", _file_size,
                PARQUET_FOOTER_SIZE));
    }
    return std::min(_file_size, DEFAULT_FOOTER_BUFFER_SIZE);
}

StatusOr<uint32_t> FileMetaDataParser::_parse_metadata_length(const std::vector<char>& footer_buff,
                                                              bool* is_encrypted) const {
    size_t size = footer_buff.size();
    *is_encrypted = false;
    if (memequal(footer_buff.data() + size - 4, 4, PARQUET_EMAIC_NUMBER, 4)) {
        // 'PARE' magic: Parquet Modular Encryption with an encrypted footer. The 4
        // bytes before the magic hold the combined length of FileCryptoMetaData plus
        // the encrypted FileMetaData; decryption is handled by the caller using the
        // per-file DEK from the scan range.
        *is_encrypted = true;
    } else if (!memequal(footer_buff.data() + size - 4, 4, PARQUET_MAGIC_NUMBER, 4)) {
        return Status::Corruption("Parquet file magic not matched");
    }

    uint32_t metadata_length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(footer_buff.data()) + size - 8);
    if (metadata_length > _file_size - PARQUET_FOOTER_SIZE) {
        return Status::Corruption(strings::Substitute(
                "Parquet file size is $0 bytes, smaller than the size reported by footer's ($1 bytes)", _file_size,
                metadata_length));
    }
    return metadata_length;
}

// reference both be/src/formats/parquet/column_converter.cpp
// and https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
SortOrder sort_order_of_logical_type(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_TIME:
        return SortOrder::SIGNED;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return SortOrder::UNSIGNED;
    default:
        return SortOrder::UNKNOWN;
    }
}

} // namespace starrocks::parquet
