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

#include "udf_downloader.h"

#include <fmt/format.h>
#include <fs/fs.h>

#include <boost/algorithm/string/predicate.hpp>
#include <memory>

#include "common/config.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "http/http_client.h"
#include "util/defer_op.h"
#include "util/md5.h"

namespace starrocks {
class UDFDownLoader {
public:
    virtual ~UDFDownLoader() = default;
    virtual Status do_download(std::string& dst_path, const std::string& remote_path, const std::string& md5sum,
                               const FSOptions& options) = 0;
};
namespace detail {

class HttpUDFDownLoader : public UDFDownLoader {
public:
    Status do_download(std::string& dst_path, const std::string& remote_path, const std::string& md5sum,
                       const FSOptions& options) override {
        auto success = false;
        auto fp = fopen(dst_path.c_str(), "w");
        DeferOp defer([&]() {
            if (fp != nullptr) {
                fclose(fp);
            }
            if (!success) {
                // delete tmp file
                (void)remove(dst_path.c_str());
            }
        });

        if (fp == nullptr) {
            std::string errmsg = strerror(errno);
            LOG(ERROR) << fmt::format("fail to open file. file = {}, error = {}", dst_path, errmsg);
            return Status::InternalError(fmt::format("fail to open tmp file when downloading file from {}. error = {}",
                                                     remote_path, errmsg));
        }
        Md5Digest digest;
        HttpClient client;
        RETURN_IF_ERROR(client.init(remote_path));
        Status status;

        auto download_cb = [&status, &dst_path, fp, &digest, &remote_path](const void* data, size_t length) {
            digest.update(data, length);
            auto res = fwrite(data, length, 1, fp);
            if (res != 1) {
                LOG(ERROR) << fmt::format("fail to write data to file {}, error={}", dst_path, ferror(fp));
                status = Status::InternalError(
                        fmt::format("file to write data when downloading file from {}", remote_path));
                return false;
            }
            return true;
        };
        RETURN_IF_ERROR(client.execute(download_cb));
        RETURN_IF_ERROR(status);

        digest.digest();
        if (!boost::iequals(digest.hex(), md5sum)) {
            LOG(ERROR) << fmt::format("Download file's checksum is not equal, expected={}, actual={}", md5sum,
                                      digest.hex());
            return Status::InternalError("Download file's checksum is not match");
        }

        success = true;
        return Status::OK();
    }
};

class S3UDFDownLoader : public UDFDownLoader {
public:
    Status do_download(std::string& dst_path, const std::string& remote_path, const std::string& md5sum,
                       const FSOptions& options) override {
        LOG(INFO) << fmt::format("Downloading udf file from {}", remote_path);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(remote_path, options));
        if (!fs) {
            LOG(ERROR) << fmt::format("No matching filesystem for {}", remote_path);
            return Status::NotFound(fmt::format("No matching filesystem available for {}", remote_path));
        }
        ASSIGN_OR_RETURN(auto source_file, fs->new_sequential_file(remote_path));
        ASSIGN_OR_RETURN(auto local_file, FileSystem::Default()->new_writable_file(dst_path));
        auto res = fs::copy(source_file.get(), local_file.get(), 1024 * 1024);
        if (!res.ok()) {
            return res.status();
        }
        RETURN_IF_ERROR(local_file->close());
        LOG(INFO) << fmt::format("Successfully downloaded udf file from {} to {}", remote_path, dst_path);
        ASSIGN_OR_RETURN(auto check_sum, fs::md5sum(dst_path));
        if (!boost::iequals(check_sum, md5sum)) {
            LOG(ERROR) << fmt::format("Download file's checksum is not equal, expected={}, actual={}", md5sum,
                                      check_sum);
            return Status::InternalError("Download file's checksum is not match");
        }
        return Status::OK();
    }
};
} // namespace detail

static std::string get_scheme(const std::string& url) {
    auto pos = url.find("://");
    if (pos == std::string::npos) {
        return "";
    }
    return url.substr(0, pos + 3);
}

std::unique_ptr<UDFDownLoader> get_downloader(const std::string& url) {
    auto schema = get_scheme(url);
    for (auto match : config::s3_compatible_fs_list) {
        if (schema == match) {
            return std::make_unique<detail::S3UDFDownLoader>();
        }
    }
    return std::make_unique<detail::HttpUDFDownLoader>();
}

Status udf_downloader::do_download(std::string& localPath, const std::string& remotePath, const std::string& md5sum,
                                   const FSOptions& options) {
    auto downloader = get_downloader(remotePath);
    RETURN_IF_ERROR(downloader->do_download(localPath, remotePath, md5sum, options));
    return Status::OK();
}
} // namespace starrocks
