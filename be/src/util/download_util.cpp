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

#include "fmt/format.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "udf/udf_downloader.h"
#include "util/defer_op.h"
#include "util/md5.h"
#include "util/uuid_generator.h"

namespace starrocks {

Status DownloadUtil::download(const std::string& url, const std::string& target_file,
                              const std::string& expected_checksum, const TCloudConfiguration& cloud_configuration) {
    auto tmp_file = fmt::format("{}_{}", target_file, ThreadLocalUUIDGenerator::next_uuid_string());
    udf_downloader downloader;
    FSOptions options(&cloud_configuration);
    RETURN_IF_ERROR(downloader.do_download(tmp_file, url, expected_checksum, options));
    // rename temporary file to target file
    auto ret = rename(tmp_file.c_str(), target_file.c_str());
    if (ret != 0) {
        (void)remove(tmp_file.c_str());
        auto err = fmt::format("fail to rename file {} to {}", tmp_file, target_file);
        LOG(ERROR) << err;
        return Status::InternalError(err);
    }
    return Status::OK();
}

} // namespace starrocks
