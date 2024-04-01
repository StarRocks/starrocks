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
#include <vector>

#include "common/statusor.h"
#include "connector_chunk_sink.h"
#include "exprs/expr_context.h"
#include "fmt/format.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "runtime/types.h"

namespace starrocks::connector {

class LocationProvider;

class HiveUtils {
public:
    static StatusOr<std::string> make_partition_name(
            const std::vector<std::string>& column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators, Chunk* chunk);

    static StatusOr<std::string> make_partition_name_nullable(
            const std::vector<std::string>& column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators, Chunk* chunk);

    static StatusOr<ConnectorChunkSink::Futures> hive_style_partitioning_write_chunk(
            const ChunkPtr& chunk, bool partitioned, const std::string& partition, int64_t max_file_size,
            const formats::FileWriterFactory* file_writer_factory, LocationProvider* location_provider,
            std::map<std::string, std::shared_ptr<formats::FileWriter>>& partition_writers);

private:
    static StatusOr<std::string> column_value(const TypeDescriptor& type_desc, const ColumnPtr& column);
};

class IcebergUtils {
public:
    static std::vector<formats::FileColumnId> generate_parquet_field_ids(
            const std::vector<TIcebergSchemaField>& fields);

    inline const static std::string DATA_DIRECTORY = "/data";
};

class PathUtils {
public:
    // requires: path contains "/"
    static std::string get_parent_path(const std::string& path) {
        std::size_t i = path.find_last_of("/");
        CHECK_NE(i, std::string::npos);
        return path.substr(0, i);
    }

    // requires: path contains "/"
    static std::string get_filename(const std::string& path) {
        std::size_t i = path.find_last_of("/");
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

// Location provider provides file location for every output file. The name format depends on if the write is partitioned or not.
class LocationProvider {
public:
    // file_name_prefix = {query_id}_{be_number}_{driver_id}
    LocationProvider(const std::string& base_path, const std::string& query_id, int be_number, int driver_id,
                     const std::string& file_suffix)
            : _base_path(PathUtils::remove_trailing_slash(base_path)),
              _file_name_prefix(fmt::format("{}_{}_{}", query_id, be_number, driver_id)),
              _file_name_suffix(file_suffix) {}

    // location = base_path/partition/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get(const std::string& partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, PathUtils::remove_trailing_slash(partition), _file_name_prefix,
                           _partition2index[partition]++, _file_name_suffix);
    }

    // location = base_path/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get() { return fmt::format("{}/{}_{}.{}", _base_path, _file_name_prefix, _index++, _file_name_suffix); }

private:
    const std::string _base_path;
    const std::string _file_name_prefix;
    const std::string _file_name_suffix;
    int _index = 0;
    std::map<std::string, int> _partition2index;
};

} // namespace starrocks::connector
