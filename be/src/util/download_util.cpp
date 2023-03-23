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

#include "util/download_util.h"

#include <boost/algorithm/string/predicate.hpp>

#include "common/status.h"
#include "fmt/format.h"
#include "http/http_client.h"
#include "util/defer_op.h"
#include "util/filesystem_util.h"
#include "util/md5.h"
#include "util/path_util.h"

namespace starrocks {

Status DownloadUtil::download(const std::string& url, const std::string& tmp_file, const std::string& target_file,
                              const std::string& expected_checksum) {
    auto directory = path_util::dir_name(tmp_file);
    auto create_directory_status = FileSystemUtil::create_directory(directory);

    if (!create_directory_status.ok()) {
        std::string error_msg = create_directory_status.get_error_msg();
        LOG(ERROR) << fmt::format("fail to create directory {}, error_msg {}.", directory, error_msg);
        return create_directory_status;
    }

    auto fp = fopen(tmp_file.c_str(), "w");
    DeferOp defer([&]() {
        if (fp != nullptr) {
            fclose(fp);
        }
    });

    if (fp == nullptr) {
        LOG(ERROR) << fmt::format("fail to open file {}", tmp_file);
        return Status::InternalError(fmt::format("fail to open tmp file when downloading file from {}", url));
    }

    Md5Digest digest;
    HttpClient client;
    RETURN_IF_ERROR(client.init(url));
    Status status;

    auto download_cb = [&status, &tmp_file, fp, &digest, &url](const void* data, size_t length) {
        digest.update(data, length);
        auto res = fwrite(data, length, 1, fp);
        if (res != 1) {
            LOG(ERROR) << fmt::format("fail to write data to file {}, error={}", tmp_file, ferror(fp));
            status = Status::InternalError(fmt::format("file to write data when downloading file from {}" + url));
            return false;
        }
        return true;
    };
    RETURN_IF_ERROR(client.execute(download_cb));
    RETURN_IF_ERROR(status);

    digest.digest();
    if (!boost::iequals(digest.hex(), expected_checksum)) {
        LOG(ERROR) << fmt::format("Download file's checksum is not equal, expected={}, actual={}", expected_checksum,
                                  digest.hex());
        return Status::InternalError("Download file's checksum is not match");
    }

    // rename tmporary file to target file
    auto ret = rename(tmp_file.c_str(), target_file.c_str());
    if (ret != 0) {
        LOG(ERROR) << fmt::format("fail to rename file {} to {}", tmp_file, target_file);
        return Status::InternalError(fmt::format("fail to rename file from {} to {}", tmp_file, target_file));
    }
    return Status::OK();
}
} // namespace starrocks
