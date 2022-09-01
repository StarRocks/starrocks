// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

#include <string_view>

#include "gutil/strings/util.h"
#include "util/string_parser.hpp"

namespace starrocks::lake {

constexpr static const int kTabletMetadataFilenameLength = 38;
constexpr static const int kTxnLogFilenameLength = 37;
constexpr static const int kTabletMetadataLockFilenameLength = 55;

inline bool is_segment(std::string_view file_name) {
    return HasSuffixString(file_name, ".dat");
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

inline std::string txn_log_filename(int64_t tablet_id, int64_t txn_id) {
    return fmt::format("{:016X}_{:016X}.log", tablet_id, txn_id);
}

inline std::string txn_vlog_filename(int64_t tablet_id, int64_t version) {
    return fmt::format("{:016X}_{:016X}.vlog", tablet_id, version);
}

inline std::string tablet_metadata_lock_filename(int64_t tablet_id, int64_t version, int64_t expire_time) {
    return fmt::format("{:016X}_{:016X}_{:016X}.lock", tablet_id, version, expire_time);
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
