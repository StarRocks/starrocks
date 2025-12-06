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

#include <fmt/format.h>

#include <optional>
#include <string_view>

#include "gen_cpp/Types_types.h" // for PUniqueId
#include "gutil/strings/util.h"
#include "util/string_parser.hpp"
#include "util/uid_util.h"

namespace starrocks::lake {

constexpr static const int kTabletMetadataFilenameLength = 38;
constexpr static const int kTxnLogFilenameLength = 37;
constexpr static const int kTabletMetadataLockFilenameLength = 55;

constexpr static const int64 kInitialVersion = 1;

constexpr static const char* const kGCFileName = "GC.json";

inline bool is_segment(std::string_view file_name) {
    return HasSuffixString(file_name, ".dat");
}

inline bool is_del(std::string_view file_name) {
    return HasSuffixString(file_name, ".del");
}

inline bool is_delvec(std::string_view file_name) {
    return HasSuffixString(file_name, ".delvec");
}

inline bool is_txn_log(std::string_view file_name) {
    return HasSuffixString(file_name, ".log");
}

inline bool is_txn_slog(std::string_view file_name) {
    return HasSuffixString(file_name, ".slog");
}

inline bool is_txn_vlog(std::string_view file_name) {
    return HasSuffixString(file_name, ".vlog");
}

inline bool is_tablet_metadata(std::string_view file_name) {
    return HasSuffixString(file_name, ".meta");
}

inline bool is_tablet_initial_metadata(std::string_view file_name) {
    return HasPrefixString(file_name, "0000000000000000_");
}

inline bool is_tablet_metadata_lock(std::string_view file_name) {
    return HasSuffixString(file_name, ".lock");
}

inline bool is_sst(std::string_view file_name) {
    return HasSuffixString(file_name, ".sst");
}

inline bool is_cols(std::string_view file_name) {
    return HasSuffixString(file_name, ".cols");
}

inline std::string tablet_metadata_filename(int64_t tablet_id, int64_t version) {
    return fmt::format("{:016X}_{:016X}.meta", tablet_id, version);
}

inline std::string tablet_initial_metadata_filename() {
    return tablet_metadata_filename(0, kInitialVersion);
}

inline std::string gen_delvec_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.delvec", txn_id, generate_uuid_string());
}

inline std::string txn_log_filename(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{:016X}_{:016X}.log", tablet_id, txn_id);
}

inline std::string txn_log_filename(int64_t tablet_id, int64_t txn_id, const PUniqueId& load_id) {
    return fmt::format("{:016X}_{:016X}_{:016X}_{:016X}.log", tablet_id, txn_id, load_id.hi(), load_id.lo());
}

inline std::string txn_slog_filename(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{:016X}_{:016X}.slog", tablet_id, txn_id);
}

inline std::string txn_vlog_filename(int64_t tablet_id, int64_t version) {
    return fmt::format("{:016X}_{:016X}.vlog", tablet_id, version);
}

inline std::string combined_txn_log_filename(int64_t txn_id) {
    return fmt::format("{:016X}.logs", txn_id);
}

inline bool is_combined_txn_log(std::string_view file_name) {
    return HasSuffixString(file_name, ".logs");
}

inline int64_t parse_combined_txn_log_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    CHECK_EQ(21, file_name.size());
    StringParser::ParseResult res;
    auto txn_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return txn_id;
}

inline std::string tablet_metadata_lock_filename(int64_t tablet_id, int64_t version, int64_t expire_time) {
    return fmt::format("{:016X}_{:016X}_{:016X}.lock", tablet_id, version, expire_time);
}

inline std::string gen_segment_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.dat", txn_id, generate_uuid_string());
}

// Helper function to extract uuid from filename, which is used in shared-data cross cluster migration
inline std::string extract_uuid_from(std::string_view file_name) {
    if (file_name.empty()) {
        return {};
    }

    const size_t dot_pos = file_name.find_last_of('.');
    if (dot_pos == std::string_view::npos) {
        return {};
    }

    std::string_view extension = file_name.substr(dot_pos);

    // sst file: uuid.sst
    if (extension == ".sst") {
        return std::string(file_name.substr(0, dot_pos));
    }

    // normal case：{:016x}_uuid.ext, with txn_id (16bit) as prefix
    constexpr size_t TXN_ID_LENGTH = 16;
    if (file_name.size() < TXN_ID_LENGTH + 2) {
        return {};
    }

    if (file_name[TXN_ID_LENGTH] != '_') {
        return {};
    }

    // check extension
    if (extension != ".dat" && extension != ".del" && extension != ".delvec" && extension != ".cols") {
        return {};
    }

    const size_t uuid_start = TXN_ID_LENGTH + 1;
    const size_t uuid_length = dot_pos - uuid_start;

    // standard UUID format: 8-4-4-4-12 = 36 bit
    if (uuid_length != 36) {
        LOG(WARNING) << "Invalid UUID length: " << uuid_length << " in file: " << file_name;
    }

    return std::string(file_name.substr(uuid_start, uuid_length));
}

// Helper function to generate a new filename from old filename, which is used in shared-data cross cluster migration
inline std::string gen_filename_from(int64_t txn_id, std::string_view old_file_name) {
    if (is_sst(old_file_name)) {
        // sst file's name will keep no change,
        return std::string(old_file_name);
    }

    if (UNLIKELY(!is_segment(old_file_name) && !is_del(old_file_name) && !is_delvec(old_file_name)) &&
        !is_cols(old_file_name)) {
        // not a valid file
        return {};
    }

    auto uuid = extract_uuid_from(old_file_name);
    if (UNLIKELY(uuid.empty())) {
        return {};
    }

    size_t dot_pos = old_file_name.find_last_of('.');
    std::string_view extension = std::string_view(old_file_name).substr(dot_pos);
    return fmt::format("{:016x}_{}{}", txn_id, uuid, extension);
}

inline std::string gen_cols_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.cols", txn_id, generate_uuid_string());
}

inline std::string gen_del_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.del", txn_id, generate_uuid_string());
}

inline std::string gen_sst_filename() {
    return fmt::format("{}.sst", generate_uuid_string());
}

inline std::optional<int64_t> extract_txn_id_prefix(std::string_view file_name) {
    constexpr static int kBase = 16;
    if (UNLIKELY(file_name.size() < 17 || file_name[16] != '_')) {
        return {};
    }
    StringParser::ParseResult res;
    auto txn_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    if (UNLIKELY(res != StringParser::PARSE_SUCCESS)) {
        return {};
    }
    return txn_id;
}

inline std::string schema_filename(int64_t schema_id) {
    return fmt::format("SCHEMA_{:016X}", schema_id);
}

// Return value: <tablet id, tablet version>
inline std::pair<int64_t, int64_t> parse_tablet_metadata_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    CHECK_EQ(kTabletMetadataFilenameLength, file_name.size()) << file_name;
    StringParser::ParseResult res;
    auto tablet_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto version = StringParser::string_to_int<int64_t>(file_name.data() + 17, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return {tablet_id, version};
}

// Return value: <tablet id, txn id>
inline std::pair<int64_t, int64_t> parse_txn_log_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    StringParser::ParseResult res;
    auto tablet_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto txn_id = StringParser::string_to_int<int64_t>(file_name.data() + 17, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return {tablet_id, txn_id};
}

inline std::pair<int64_t, int64_t> parse_txn_slog_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    StringParser::ParseResult res;
    auto tablet_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto txn_id = StringParser::string_to_int<int64_t>(file_name.data() + 17, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return {tablet_id, txn_id};
}

// Return value: <tablet id, version number>
inline std::pair<int64_t, int64_t> parse_txn_vlog_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    StringParser::ParseResult res;
    auto tablet_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto version = StringParser::string_to_int<int64_t>(file_name.data() + 17, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return {tablet_id, version};
}

// Return value: <tablet id, version, expire time>
inline std::tuple<int64_t, int64_t, int64_t> parse_tablet_metadata_lock_filename(std::string_view file_name) {
    constexpr static int kBase = 16;
    CHECK_EQ(kTabletMetadataLockFilenameLength, file_name.size()) << file_name;
    StringParser::ParseResult res;
    auto tablet_id = StringParser::string_to_int<int64_t>(file_name.data(), 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto version = StringParser::string_to_int<int64_t>(file_name.data() + 17, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    auto expire_time = StringParser::string_to_int<int64_t>(file_name.data() + 34, 16, kBase, &res);
    CHECK_EQ(StringParser::PARSE_SUCCESS, res) << file_name;
    return std::make_tuple(tablet_id, version, expire_time);
}

inline std::string_view basename(std::string_view path) {
    return path.substr(path.find_last_of('/') + 1);
}

// get prefix name
inline std::string_view prefix_name(std::string_view path) {
    auto pos = path.find_last_of('/');
    if (pos == std::string_view::npos) {
        return path;
    }
    return path.substr(0, pos);
}

} // namespace starrocks::lake
