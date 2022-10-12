// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"

namespace starrocks {

struct JDBCDriverEntry;

// JDBCDriverManager is responsible for managing jdbc driver jar files.
// All jar files will be placed in `${STARROCKS_HOME}/lib/jdbc_drivers` and named in the format of ${name}_${checksum}_${first_access_ts}.jar
// `first_access_ts` represents the time when the driver is accessed for the first time on this node.
// Drivers with the same name maybe created repeatedly.
// We use `first_access_ts` simply identify the version. The later the access time, the newer the version.
//
// The jar file is uniquely identified by the name plus the checksum.
// If there is a driver with the same name but different checksum, it will be rewritten. make a download and delete the old one
//
// Each time the server starts, it will scan `${STARROCKS_HOME}/lib/jdbc_drivers` directory,
// automatically delete the leftover temporary files and load driver informations into memory.
// If there are multiple jar files with the same name, the one with the latest access time will be used, and the others will be deleted.
class JDBCDriverManager {
public:
    using JDBCDriverEntryPtr = std::shared_ptr<JDBCDriverEntry>;

    JDBCDriverManager();
    ~JDBCDriverManager();

    static JDBCDriverManager* getInstance();

    Status init(const std::string& driver_dir);

    Status get_driver_location(const std::string& name, const std::string& url, const std::string& checksum,
                               std::string* location);

private:
    Status _download_driver(const std::string& url, JDBCDriverEntryPtr& entry);

    bool _parse_from_file_name(std::string_view file_name, std::string* name, std::string* checksum,
                               int64_t* frist_access_ts);

    std::string _generate_driver_location(const std::string& name, const std::string& checksum,
                                          int64_t first_access_ts);

    std::string _driver_dir;

    std::mutex _lock;
    std::unordered_map<std::string, JDBCDriverEntryPtr> _entry_map;

    static constexpr const char* TMP_FILE_SUFFIX = ".tmp";
    static constexpr const char* JAR_FILE_SUFFIX = ".jar";
};
} // namespace starrocks