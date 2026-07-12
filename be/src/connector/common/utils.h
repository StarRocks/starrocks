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

#include <map>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::connector {

class PathUtils {
public:
    // requires: path contains "/"
    static std::string get_parent_path(const std::string& path) {
        std::size_t i = path.find_last_of('/');
        CHECK_NE(i, std::string::npos);
        return path.substr(0, i);
    }

    // requires: path contains "/"
    static std::string get_filename(const std::string& path) {
        std::size_t i = path.find_last_of('/');
        CHECK_NE(i, std::string::npos);
        return path.substr(i + 1);
    }

    static std::string remove_trailing_slash(const std::string& path) {
        if (path.ends_with("/")) {
            return path.substr(0, path.size() - 1);
        }
        return path;
    }
};

// Normalize a raw format string (e.g. "csv.gz.csv", "  .Csv  ") to a canonical
// lowercase base name (e.g. "csv").
std::string normalize_format_name(std::string format);

// Build canonical output file suffix for connector file sinks.
// Expects a normalized format name (from normalize_format_name).
// For CSV, append compression suffix (e.g. csv.gz); for self-describing formats
// like parquet/orc, keep the format suffix only.
StatusOr<std::string> build_canonical_file_suffix(const std::string& format, TCompressionType::type compression_type);

// Location provider provides file location for every output file. The name format depends on if the write is partitioned or not.
class LocationProvider {
public:
    // file_name_prefix = {query_id}_{be_number}_{driver_id}
    // or {query_id}_{be_number}_{writer_tag}_{driver_id} when writer_tag is non-empty
    LocationProvider(const std::string& base_path, const std::string& query_id, int be_number, int driver_id,
                     std::string file_suffix, std::string writer_tag = "")
            : _base_path(PathUtils::remove_trailing_slash(base_path)),
              _file_name_prefix(writer_tag.empty()
                                        ? fmt::format("{}_{}_{}", query_id, be_number, driver_id)
                                        : fmt::format("{}_{}_{}_{}", query_id, be_number, writer_tag, driver_id)),
              _file_name_suffix(std::move(file_suffix)) {}

    // location = base_path/partition/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get(const std::string& partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, PathUtils::remove_trailing_slash(partition), _file_name_prefix,
                           _partition2index[partition]++, _file_name_suffix);
    }

    // location = base_path/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get() { return fmt::format("{}/{}_{}.{}", _base_path, _file_name_prefix, _index++, _file_name_suffix); }

    std::string root_location(const std::string& partition) {
        return fmt::format("{}/{}", _base_path, PathUtils::remove_trailing_slash(partition));
    }

    std::string root_location() { return fmt::format("{}", PathUtils::remove_trailing_slash(_base_path)); }

private:
    const std::string _base_path;
    const std::string _file_name_prefix;
    const std::string _file_name_suffix;
    int _index = 0;
    std::map<std::string, int> _partition2index;
};

} // namespace starrocks::connector
