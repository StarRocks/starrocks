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

// Information of a log entry
struct LogEntryInfo {
    LogEntryPB* log_entry;
    int64_t version;
    int64_t start_seq_id;
    int64_t end_seq_id;
    // file id for INSERT_RANGE and UPDATE_AFTER, because the
    // log entry may share it with the previous, and not record
    // it in LogEntryPB
    FileIdPB* file_id;
    // start row id for INSERT_RANGE and UPDATE_AFTER, because
    // the log entry may not record it in the LogEntryPB if rows
    // are continuous between the adjacent log entries
    int32_t start_row_id;
    int32_t num_rows;
    // whether this is the last log entry of the version
    bool end_of_version;
};

struct PageContext {
    // index of next log entry to read
    int32_t next_log_entry_index;
    PageHeaderPB page_header;
    PageContentPB page_content;
};

// Read log entries in the binlog file.
//
// How to use
//  std::shared_ptr<BinlogFileReader> file_reader;
//  Status st = file_reader->seek();
//  while (st.ok()) {
//      LogEntryInfo* log_entry = file_reader->log_entry();
//      // do something
//      st = file_reader->next();
//  }
class BinlogFileReader final {
public:
    BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta);

    // Seek to the log entry containing the <version, seq_id>.
    // This method only can be called once for a file reader.
    // Return Status::Ok() if find the entry, Status::NotFound
    // if not found, and other status if there is other error
    Status seek(int64_t version, int64_t seq_id);

    // Move to the next log entry.
    // Return Status::Ok() if find the entry, Status::EndOfFile
    // if there is no next, and other status if there is other error
    Status next();

    // Get the information of the current log entry
    LogEntryInfo* log_entry();

private:
    Status _seek(int64_t version, int64_t seq_id);
    // seek to the page containing the change event <version, seq_id>
    Status _seek_to_page(int64_t version, int64_t seq_id);
    // seek to the log entry containing the seq_id in a page
    Status _seek_to_log_entry(int64_t seq_id);
    Status _read_file_header();
    Status _read_next_page();
    Status _read_page_header(int page_index, PageHeaderPB* page_header_pb);
    Status _read_page_content(int page_index, PageHeaderPB* page_header_pb, PageContentPB* page_content_pb);
    void _advance_log_entry();

    void _reset_current_page_context() {
        PageContext* page_context = _current_page_context.get();
        page_context->next_log_entry_index = 0;
        page_context->page_header.Clear();
        page_context->page_content.Clear();
    }

    // init current log entry after switching to the next page
    void _init_current_log_entry() {
        _current_log_entry->log_entry = nullptr;
        _current_log_entry->start_seq_id = _current_page_context->page_header.start_seq_id();
        _current_log_entry->end_seq_id = _current_page_context->page_header.start_seq_id() - 1;
        _current_log_entry->file_id = nullptr;
    }

    bool _is_end_of_file() {
        PageContext* page_context;
        return _next_page_index == _file_meta->num_pages() &&
               page_context->next_log_entry_index == page_context->page_header.num_log_entries();
    }

    std::shared_ptr<BinlogFileMetaPB> _file_meta;
    std::string _file_path;
    std::unique_ptr<RandomAccessFile> _file;
    int64_t _file_size;
    int64_t _current_file_pos;
    std::unique_ptr<BinlogFileHeaderPB> _file_header;

    // the index of next page to read
    int32_t _next_page_index;
    std::unique_ptr<LogEntryInfo> _current_log_entry;
    std::unique_ptr<PageContext> _current_page_context;
};

} // namespace starrocks
