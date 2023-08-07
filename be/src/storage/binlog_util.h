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

#include <list>

#include "common/status.h"
#include "gen_cpp/binlog.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/uint24.h"

namespace starrocks {

using BinlogFileMetaPBPtr = std::shared_ptr<BinlogFileMetaPB>;

// The log sequence number of the change event which is unique in all ingestion. It's a combination of
// version(int64_t) and seq_id(int64_t). The version is the publish version of the ingestion to generate
// the change event, and the seq_id is the sequence number of the change event in the ingestion.
struct BinlogLsn {
    uint128_t lsn = 0;

    BinlogLsn(int64_t version, int64_t seq_id) : lsn((((uint128_t)version) << 64) | seq_id) {}
    BinlogLsn() = default;

    int64_t version() const { return (int64_t)(lsn >> 64); }
    int64_t seq_id() const { return (int64_t)(lsn & 0xffffffffUL); }
    bool operator!=(const BinlogLsn& rhs) const { return lsn != rhs.lsn; }
    bool operator==(const BinlogLsn& rhs) const { return lsn == rhs.lsn; }
    bool operator<(const BinlogLsn& rhs) const { return lsn < rhs.lsn; }
    bool operator<=(const BinlogLsn& rhs) const { return lsn <= rhs.lsn; }
    std::string to_string() const;
    friend std::ostream& operator<<(std::ostream& os, const BinlogLsn& lsn);
};

inline std::ostream& operator<<(std::ostream& os, const BinlogLsn& lsn) {
    return os << lsn.to_string();
}

class BinlogUtil {
public:
    static std::string binlog_file_path(std::string& binlog_dir, int64_t file_id) {
        return strings::Substitute("$0/$1.binlog", binlog_dir, file_id);
    }

    static std::string file_meta_to_string(BinlogFileMetaPB* file_meta);

    static std::string page_header_to_string(PageHeaderPB* page_header);

    static bool get_file_id_from_name(const std::string& file_name, int64_t* file_id);

    static Status list_binlog_file_ids(std::string& binlog_dir, std::list<int64_t>* binlog_file_ids);
};

} // namespace starrocks
