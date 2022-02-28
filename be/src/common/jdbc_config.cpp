#include "common/jdbc_config.h"

#include <filesystem>
#include <fstream>
#include <iostream>

#include "common/logging.h"
#include "fmt/format.h"
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/rapidjson.h"

namespace starrocks {
namespace config {

JDBCDriverConfig& JDBCDriverConfig::getInstance() {
    static std::unique_ptr<JDBCDriverConfig> config;
    if (config == nullptr) {
        config.reset(new JDBCDriverConfig());
    }
    return *config;
}

bool JDBCDriverConfig::load_from_json_file(const std::string& file_name) {
    std::ifstream ifs(file_name);
    if (!ifs) {
        LOG(ERROR) << fmt::format("config file[{}] is not found");
        return false;
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    std::string content = oss.str();

    rapidjson::Document doc;
    doc.Parse(content.data(), content.size());
    if (doc.HasParseError()) {
        LOG(ERROR) << "parse json file failed, error: " << RAPIDJSON_ERROR_STRING(doc.GetParseError());
        return false;
    }
    // parse data
    for (rapidjson::Value::ConstMemberIterator iter = doc.MemberBegin(); iter != doc.MemberEnd(); iter++) {
        rapidjson::Value key;
        rapidjson::Value value;
        rapidjson::Document::AllocatorType allocator;
        key.CopyFrom(iter->name, allocator);
        value.CopyFrom(iter->value, allocator);
        if (!key.IsString()) {
            LOG(ERROR) << "parse config error, driver name must be string";
            return false;
        }
        std::string driver_name = key.GetString();

        JDBCDriverInfo driver_info;
        if (!value.IsObject()) {
            LOG(ERROR) << "parse config error, driver info must be object";
            return false;
        }
        if (!value.HasMember("driver_path")) {
            LOG(ERROR) << fmt::format("parse config error, cannot find `driver_path` item under driver[{}]",
                                      key.GetString());
            return false;
        }
        if (!value["driver_path"].IsString()) {
            LOG(ERROR) << "parse config error, `driver_path` must be string";
            return false;
        }
        driver_info.driver_path = value["driver_path"].GetString();

        if (!value.HasMember("class_name")) {
            LOG(ERROR) << fmt::format("parse config error, cannot find `class_name` item under driver[{}]",
                                      key.GetString());
            return false;
        }
        if (!value["class_name"].IsString()) {
            LOG(ERROR) << "parse config error, `class_name` must be string";
            return false;
        }
        driver_info.class_name = value["class_name"].GetString();

        _configs[driver_name] = driver_info;
        LOG(INFO) << fmt::format("add jdbc driver, name[{}], path[{}], class[{}]", driver_name, driver_info.driver_path,
                                 driver_info.class_name);
    }

    return true;
}

bool JDBCDriverConfig::get_driver_info(const std::string& driver_name, JDBCDriverInfo* result) {
    auto iter = _configs.find(driver_name);
    if (iter == _configs.end()) {
        return false;
    }
    *result = iter->second;
    return true;
}

} // namespace config
} // namespace starrocks