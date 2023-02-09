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

#include <re2/re2.h>

#include "common/status.h"
#include "gen_cpp/binlog.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/uint24.h"

namespace starrocks {

using BinlogFileMetaPBPtr = std::shared_ptr<BinlogFileMetaPB>;

class BinlogUtil {
public:
    static std::string binlog_file_path(std::string& binlog_dir, int64_t file_id) {
        return strings::Substitute("$0/$1.binlog", binlog_dir, file_id);
    }

    static int128_t get_lsn(int64_t version, int64_t seq_id) { return (((int128_t)version) << 64) | seq_id; }

    static std::string file_meta_to_string(BinlogFileMetaPB* file_meta);

    static bool get_file_id_from_name(const std::string& file_name, int64_t* file_id);

    static Status list_binlog_file_ids(std::string& binlog_dir, std::set<int64_t>* binlog_file_ids);
};

} // namespace starrocks
