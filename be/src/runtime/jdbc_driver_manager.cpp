// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "runtime/jdbc_driver_manager.h"

#include <atomic>
#include <boost/algorithm/string/predicate.hpp>      // boost::algorithm::ends_with
#include <chrono>

#include "env/env.h"
#include "gutil/strings/split.h"
#include "http/http_client.h"
#include "util/defer_op.h"
#include "util/dynamic_util.h"
#include "util/file_utils.h"
#include "util/md5.h"
#include "util/slice.h"
#include "fmt/format.h"

namespace starrocks {

struct JDBCDriverEntry {
    JDBCDriverEntry(const std::string& name_, const std::string& checksum_)
        : name(name_), checksum(checksum_) {}

    ~JDBCDriverEntry();

    inline bool is_expected(const std::string& name, const std::string& checksum) {
        return this->name == name && this->checksum == checksum;
    }

    std::string name;
    std::string checksum;
    int64_t first_access_ts;
    // ${driver_dir}/${name}_${checksum}_${first_access_ts}.jar
    std::string location;

    std::atomic<bool> is_available{false};

    std::atomic<bool> should_delete{false};

    std::mutex download_lock;
    bool is_downloaded = false;
};

JDBCDriverEntry::~JDBCDriverEntry() {
    if (should_delete.load()) {
        LOG(INFO) << fmt::format("try to delete jdbc driver {}", location);
        FileUtils::remove(location);
    }
}


JDBCDriverManager::JDBCDriverManager() {

}

JDBCDriverManager::~JDBCDriverManager() {
    std::unique_lock<std::mutex> l(_lock);
    _entry_map.clear();
}

JDBCDriverManager* JDBCDriverManager::getInstance() {
    static std::unique_ptr<JDBCDriverManager> manager;
    if (manager == nullptr) {
        manager.reset(new JDBCDriverManager());
    }
    return manager.get();
}


Status JDBCDriverManager::init(const std::string& driver_dir) {
    std::unique_lock<std::mutex> l(_lock);

    _driver_dir = driver_dir;
    RETURN_IF_ERROR(FileUtils::create_dir(_driver_dir));
    std::vector<std::string> driver_files;
    RETURN_IF_ERROR(FileUtils::list_files(Env::Default(), _driver_dir, &driver_files));
    // load jdbc drivers from file
    for (auto& file : driver_files) {
        std::string target_file = fmt::format("{}/{}", _driver_dir, file);
        // remove all tmporary files
        if (boost::algorithm::ends_with(file, TMP_FILE_SUFFIX)) {
            LOG(INFO) << fmt::format("try to remove tmporary file {}", target_file);
            FileUtils::remove(target_file);
            continue;
        }
        // try to load drivers from jar file
        if (boost::algorithm::ends_with(file, JAR_FILE_SUFFIX)) {
            std::string name;
            std::string checksum;
            int64_t first_access_ts;
            if (!_parse_from_file_name(file, &name, &checksum, &first_access_ts)) {
                LOG(WARNING) << fmt::format("cannot parse jdbc driver info from file {}, try to remove it", file);
                FileUtils::remove(target_file);
                continue;
            }

            JDBCDriverEntryPtr entry;
            auto iter = _entry_map.find(name);
            if (iter == _entry_map.end()) {
                entry = std::make_shared<JDBCDriverEntry>(name, checksum);
                entry->first_access_ts = first_access_ts;
                entry->location = target_file;
                entry->is_downloaded = true;
                entry->is_available.store(true);
                _entry_map[name] = entry;
                LOG(INFO) << fmt::format("load jdbc driver from file[{}], name[{}], checksum[{}], first_access_ts[{}]", file, entry->name, entry->checksum, entry->first_access_ts);
            } else {
                entry = iter->second;
                // replace old with new and delete the old driver
                if (first_access_ts > entry->first_access_ts) {
                    // delete the old one
                    entry->should_delete.store(true);
                    // create the new one
                    entry = std::make_shared<JDBCDriverEntry>(name, checksum);
                    entry->first_access_ts = first_access_ts;
                    entry->location = target_file;
                    entry->is_downloaded = true;
                    entry->is_available.store(true);
                    _entry_map[name] = entry;
                    LOG(INFO) << fmt::format("load jdbc driver from file[{}], name[{}], checksum[{}], first_access_ts[{}]", file, entry->name, entry->checksum, entry->first_access_ts);
                } else {
                    // this driver is old, just remove
                    LOG(INFO) << fmt::format("try to remove an old jdbc driver, name[{}], file[{}]", name, target_file);
                    FileUtils::remove(target_file);
                }
            }
        }
    }
    return Status::OK();
}

Status JDBCDriverManager::get_driver_location(const std::string& name, const std::string& url, const std::string& checksum, std::string* location) {
    JDBCDriverEntryPtr entry;
    {
        using namespace std::chrono;
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _entry_map.find(name);
        if (iter == _entry_map.end()) {
            entry = std::make_shared<JDBCDriverEntry>(name, checksum);
            entry->first_access_ts = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            entry->location = _generate_driver_location(entry->name, entry->checksum, entry->first_access_ts);
            _entry_map[name] = entry;
        } else {
            entry = iter->second;
        }
        if (entry->is_expected(name, checksum)) {
            if (entry->is_available.load()) {
                LOG(INFO) << fmt::format("driver[{}] already exists", name);
                *location = entry->location;
                return Status::OK();
            }
        } else {
            // checksum mismatch, replace old with new
            // mark the old one for deletion
            entry->should_delete.store(true);
            // create a new one
            entry = std::make_shared<JDBCDriverEntry>(name, checksum);
            entry->first_access_ts = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            entry->location = _generate_driver_location(entry->name, entry->checksum, entry->first_access_ts);
            _entry_map[name] = entry;
        }
    }
    RETURN_IF_ERROR(_download_driver(url, entry));
    *location = entry->location;
    return Status::OK();
}

Status JDBCDriverManager::_download_driver(const std::string& url, JDBCDriverEntryPtr& entry) {
    std::unique_lock<std::mutex> l (entry->download_lock);
    if (entry->is_downloaded) {
        return Status::OK();
    }
    LOG(INFO) << fmt::format("download jdbc driver {} from url {}, expected checksum is: {}", entry->name, url, entry->checksum);
    std::string tmp_file = fmt::format("{}/{}_{}.tmp", _driver_dir, entry->name, entry->checksum);
    auto fp = fopen(tmp_file.c_str(), "w");
    DeferOp defer([&] () {
        fclose(fp);
    });

    if (fp == nullptr) {
        LOG(ERROR) << fmt::format("fail to open file {}, error={}", tmp_file, ferror(fp));
        return Status::InternalError("fail to open tmp file when downloading jdbc driver");
    }

    Md5Digest digest;
    HttpClient client;
    client.set_ssl_verify_peer(false);
    RETURN_IF_ERROR(client.init(url));
    Status status;

    auto download_cb = [&status, &tmp_file, fp, &digest] (const void* data, size_t length) {
        digest.update(data, length);
        auto res = fwrite(data, length, 1, fp);
        if (res != 1) {
            LOG(ERROR) << fmt::format("fail to write data to file {}, error={}", tmp_file, ferror(fp));
            status = Status::InternalError("fail to write data when downloading jdbc driver");
            return false;
        }
        return true;
    };
    RETURN_IF_ERROR(client.execute(download_cb));
    RETURN_IF_ERROR(status);

    digest.digest();
    if (!boost::iequals(digest.hex(), entry->checksum)) {
        LOG(ERROR) << fmt::format("JDBC Driver's checksum is not equal, expected={}, actual={}", entry->checksum, digest.hex());
        return Status::InternalError("JDBC Driver's checksum is not match");
    }

    // rename tmporary file to driver file
    auto ret = rename(tmp_file.c_str(), entry->location.c_str());
    if (ret != 0) {
        LOG(ERROR) << fmt::format("fail to rename file {} to {}", tmp_file, entry->location);
        return Status::InternalError("fail to rename file");
    }
    entry->is_downloaded = true;
    entry->is_available.store(true);
    return Status::OK();
}

bool JDBCDriverManager::_parse_from_file_name(std::string_view file_name, std::string* name, std::string* checksum, int64_t* first_access_ts) {
    // remove '.jar' suffix
    file_name.remove_suffix(std::strlen(JAR_FILE_SUFFIX));

    // parse first_access_ts
    size_t pos = file_name.find_last_of('_');
    if (pos == std::string::npos) {
        return false;
    }
    std::string str = std::string(file_name.substr(pos + 1));
    try {
        *first_access_ts = std::stol(str);
    } catch (std::exception& e) {
        return false;
    }

    // parse checksum
    file_name.remove_suffix(str.size() + 1);
    pos = file_name.find_last_of('_');
    if (pos == std::string::npos) {
        return false;
    }
    str = std::string(file_name.substr(pos + 1));
    *checksum = str;

    // parse name
    file_name.remove_suffix(str.size() + 1);
    pos = file_name.find_last_of('_');
    if (pos == std::string::npos) {
        return false;
    }
    str = std::string(file_name);
    *name = str;

    return true;
}

std::string JDBCDriverManager::_generate_driver_location(const std::string& name, const std::string& checksum, int64_t first_access_ts) {
    return fmt::format("{}/{}_{}_{}.jar", _driver_dir, name, checksum, first_access_ts);
}


}
