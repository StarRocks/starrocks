//
// Created by Letian Jiang on 2024/1/29.
//

#pragma once

#include <string>
#include <vector>

#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "fmt/format.h"
#include "formats/column_evaluator.h"
#include "runtime/types.h"

namespace starrocks::connector {

class HiveUtils {
public:
    static StatusOr<std::string> make_partition_name(
            const std::vector<std::string>& column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators, ChunkPtr chunk);

private:
    static StatusOr<std::string> column_value(const TypeDescriptor& type_desc, const ColumnPtr& column);
};

// Location provider provides file location for every output file. The name format depends on if the write is partitioned or not.
class LocationProvider {
public:
    // file_name_prefix = {query_id}_{be_number}_{driver_id}
    LocationProvider(const std::string& base_path, const std::string& query_id, int be_number, int driver_id,
                     const std::string& file_suffix)
            : _base_path(base_path),
              _file_name_prefix(fmt::format("{}_{}_{}", query_id, be_number, driver_id)),
              _file_name_suffix(file_suffix) {}

    // location = base_path/partition/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get(const std::string& partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, partition, _file_name_prefix, _partition2index[partition]++,
                           _file_name_suffix);
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
