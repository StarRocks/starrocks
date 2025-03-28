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

#include <glog/logging.h>

#include <cstdlib>
#include <sstream>
#include <string_view>
#include <utility>

#include "formats/parquet/file_reader.h"
#include "formats/parquet/schema.h"
#include "util/thrift_util.h"

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

    ObjectCacheHandle* cache_handle = nullptr;
    std::string metacache_key =
            _build_metacache_key(_file->filename(), _datacache_options->modification_time, _file_size);
    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_cache_read_ns);
        Status st = _cache->lookup(metacache_key, &cache_handle);
        if (st.ok()) {
            auto file_metadata = *(static_cast<const FileMetaDataPtr*>(_cache->value(cache_handle)));
            _scanner_ctx->stats->footer_cache_read_count += 1;
            _cache->release(cache_handle);
            return file_metadata;
        }
    }

    FileMetaDataPtr file_metadata = nullptr;
    int64_t file_metadata_size = 0;
    RETURN_IF_ERROR(_parse_footer(&file_metadata, &file_metadata_size));
    if (file_metadata_size > 0) {
        // cache does not understand shared ptr at all.
        // so we have to new an object to hold this shared ptr.
        FileMetaDataPtr* capture = new FileMetaDataPtr(file_metadata);
        Status st = Status::InternalError("write footer cache failed");
        DeferOp op([&st, this, capture, file_metadata_size, &cache_handle]() {
            if (st.ok()) {
                _scanner_ctx->stats->footer_cache_write_bytes += file_metadata_size;
                _scanner_ctx->stats->footer_cache_write_count += 1;
                _cache->release(cache_handle);
            } else {
                _scanner_ctx->stats->footer_cache_write_fail_count += 1;
                delete capture;
            }
        });
        auto deleter = [](const CacheKey& key, void* value) { delete (FileMetaDataPtr*)value; };
        ObjectCacheWriteOptions options;
        options.evict_probability = _datacache_options->datacache_evict_probability;
        st = _cache->insert(metacache_key, capture, file_metadata_size, file_metadata_size, deleter, &cache_handle,
                            &options);
    } else {
        LOG(ERROR) << "Parsing unexpected parquet file metadata size";
    }
    return file_metadata;
}

Status FileMetaDataParser::_parse_footer(FileMetaDataPtr* file_metadata_ptr, int64_t* file_metadata_size) {
    std::vector<char> footer_buffer;
    ASSIGN_OR_RETURN(uint32_t footer_read_size, _get_footer_read_size());
    footer_buffer.resize(footer_read_size);

    {
        SCOPED_RAW_TIMER(&_scanner_ctx->stats->footer_read_ns);
        RETURN_IF_ERROR(_file->read_at_fully(_file_size - footer_read_size, footer_buffer.data(), footer_read_size));
    }

    ASSIGN_OR_RETURN(uint32_t metadata_length, _parse_metadata_length(footer_buffer));

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
        RETURN_IF_ERROR(file_metadata->init(t_metadata, _scanner_ctx->case_sensitive));
        *file_metadata_size = CurrentThread::current().get_consumed_bytes() - before_bytes;
    }
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

StatusOr<uint32_t> FileMetaDataParser::_parse_metadata_length(const std::vector<char>& footer_buff) const {
    size_t size = footer_buff.size();
    if (memequal(footer_buff.data() + size - 4, 4, PARQUET_EMAIC_NUMBER, 4)) {
        return Status::NotSupported("StarRocks parquet reader not support encrypted parquet file yet");
    }

    if (!memequal(footer_buff.data() + size - 4, 4, PARQUET_MAGIC_NUMBER, 4)) {
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

std::string FileMetaDataParser::_build_metacache_key(const std::string& filename, int64_t modification_time,
                                                     uint64_t file_size) {
    std::string metacache_key;
    metacache_key.resize(14);
    char* data = metacache_key.data();
    const std::string footer_suffix = "ft";
    uint64_t hash_value = HashUtil::hash64(filename.data(), filename.size(), 0);
    memcpy(data, &hash_value, sizeof(hash_value));
    memcpy(data + 8, footer_suffix.data(), footer_suffix.length());
    // The modification time is more appropriate to indicate the different file versions.
    // While some data source, such as Hudi, have no modification time because their files
    // cannot be overwritten. So, if the modification time is unsupported, we use file size instead.
    // Also, to reduce memory usage, we only use the high four bytes to represent the second timestamp.
    if (modification_time > 0) {
        uint32_t mtime_s = (modification_time >> 9) & 0x00000000FFFFFFFF;
        memcpy(data + 10, &mtime_s, sizeof(mtime_s));
    } else {
        uint32_t size = file_size;
        memcpy(data + 10, &size, sizeof(size));
    }
    return metacache_key;
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
