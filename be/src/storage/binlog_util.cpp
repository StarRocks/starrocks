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

#include "storage/binlog_util.h"

#include <re2/re2.h>

#include "fs/fs_util.h"

namespace starrocks {

std::string BinlogUtil::file_meta_to_string(BinlogFileMetaPB* file_meta) {
    std::stringstream ss;
    ss << "{"
       << "id: " << file_meta->id() << ", start_version: " << file_meta->start_version()
       << ", start_seq_id: " << file_meta->start_seq_id()
       << ", start_timestamp_in_us: " << file_meta->start_timestamp_in_us()
       << ", end_version: " << file_meta->end_version() << ", end_seq_id: " << file_meta->end_seq_id()
       << ", end_timestamp_in_us: " << file_meta->end_timestamp_in_us() << ", num_pages: " << file_meta->num_pages()
       << ", file_size: " << file_meta->file_size() << "}";
    return ss.str();
}

bool BinlogUtil::get_file_id_from_name(const std::string& file_name, int64_t* file_id) {
    static re2::RE2 re(R"((\d+)\.binlog)", re2::RE2::Quiet);
    std::string fild_id_str;
    bool ret = RE2::PartialMatch(file_name, re, &fild_id_str);
    if (!ret) {
        return false;
    }

    ret = safe_strto64(fild_id_str, file_id);
    if (!ret) {
        LOG(WARNING) << "Invalid binlog file name, file_name: " << file_name << ", file_id_str: " << fild_id_str;
        return false;
    }
    return true;
}

Status BinlogUtil::list_binlog_file_ids(std::string& binlog_dir, std::set<int64_t>* binlog_file_ids) {
    std::set<std::string> file_names;
    RETURN_IF_ERROR(fs::list_dirs_files(binlog_dir, nullptr, &file_names));
    int64_t file_id;
    for (auto& name : file_names) {
        bool ret = get_file_id_from_name(name, &file_id);
        if (ret) {
            binlog_file_ids->emplace(file_id);
        }
    }
    return Status::OK();
}

} // namespace starrocks