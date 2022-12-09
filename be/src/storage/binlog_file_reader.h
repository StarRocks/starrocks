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

#pragma

#include "gen_cpp/binlog.pb.h"

namespace starrocks {

struct LogEntryInfo {
    LogEntryPB* log_entry;
    int64_t version;
    int64_t start_changelog_id;
    int64_t end_changelog_id;
    FileIdPB* file_id;
    int32_t start_row_id;
    bool last_log_entry_in_version;
};

// TODO add metrics for read
class BinlogFileReader final {
public:
    BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta);

    Status seek(int64_t version, int64_t changelog_id);

    Status next();

    LogEntryInfo* log_entry();

private:
    Status _seek_to_page(int64_t version, int64_t changelog_id);
    Status _seek_to_log_entry(int64_t changelog_id);
    Status _seek(int64_t version, int64_t changelog_id);
    Status _parse_file_header();
    Status _read_page_header();
    Status _read_page();
    void _advance_log_entry();

    std::shared_ptr<BinlogFileMetaPB> _file_meta;
    std::string _file_name;
    std::unique_ptr<RandomAccessFile> _file;
    int64_t _file_size;
    int64_t _current_file_pos;

    std::unique_ptr<BinlogFileHeaderPB> _file_header;
    int32_t _current_page_index;
    std::unique_ptr<PageHeaderPB> _current_page_header;
    std::unique_ptr<PageContentPB> _current_page_content;
    bool _is_last_page;
    int32_t _log_entry_index;
    int32_t _log_entry_start_changelog_id;
    int32_t _log_entry_num_changelogs;
    // owned by _current_page_content
    FileIdPB* _log_entry_file_id;
    int32_t _log_entry_start_row_id;
    int32_t _log_entry_num_rows;

    std::unique_ptr<LogEntryInfo> _current_log_entry_info;
};

} // namespace starrocks
