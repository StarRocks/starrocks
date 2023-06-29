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

#include "gutil/strings/util.h"
#include "util/string_parser.hpp"
#include "util/uid_util.h"

namespace starrocks::lake {

constexpr static const int kTabletMetadataFilenameLength = 38;
constexpr static const int kTxnLogFilenameLength = 37;
constexpr static const int kTabletMetadataLockFilenameLength = 55;

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

inline bool is_txn_vlog(std::string_view file_name) {
    return HasSuffixString(file_name, ".vlog");
}

inline bool is_tablet_metadata(std::string_view file_name) {
    return HasSuffixString(file_name, ".meta");
}

inline bool is_tablet_metadata_lock(std::string_view file_name) {
    return HasSuffixString(file_name, ".lock");
}

inline std::string tablet_metadata_filename(int64_t tablet_id, int64_t version) {
    return fmt::format("{:016X}_{:016X}.meta", tablet_id, version);
}

inline std::string gen_delvec_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.delvec", txn_id, generate_uuid_string());
}

inline std::string txn_log_filename(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{:016X}_{:016X}.log", tablet_id, txn_id);
}

inline std::string txn_vlog_filename(int64_t tablet_id, int64_t version) {
    return fmt::format("{:016X}_{:016X}.vlog", tablet_id, version);
}

inline std::string tablet_metadata_lock_filename(int64_t tablet_id, int64_t version, int64_t expire_time) {
    return fmt::format("{:016X}_{:016X}_{:016X}.lock", tablet_id, version, expire_time);
}

inline std::string gen_segment_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.dat", txn_id, generate_uuid_string());
}

inline std::string gen_del_filename(int64_t txn_id) {
    return fmt::format("{:016x}_{}.del", txn_id, generate_uuid_string());
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
    CHECK_EQ(kTxnLogFilenameLength, file_name.size()) << file_name;
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

} // namespace starrocks::lake
