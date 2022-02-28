// This file is made available under Elastic License 2.0.

#pragma once
#include <string>
#include <unordered_map>

namespace starrocks {
namespace config {

struct JDBCDriverInfo {
    std::string driver_path;
    std::string class_name;
};

class JDBCDriverConfig {
public:
    static JDBCDriverConfig& getInstance();

    bool load_from_json_file(const std::string& file_name);

    bool get_driver_info(const std::string& driver_name, JDBCDriverInfo* result);

private:
    JDBCDriverConfig() = default;

    std::unordered_map<std::string, JDBCDriverInfo> _configs;
};

} // namespace config
} // namespace starrocks