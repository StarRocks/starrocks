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

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_util.h"

namespace starrocks {

// Information of a log entry
struct LogEntryInfo {
    LogEntryPB* log_entry;
    int64_t version;
    int64_t start_seq_id;
    // -1 for EMPTY log entry
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
    int64_t timestamp_in_us;
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
class BinlogFileReader {
public:
    BinlogFileReader(std::string file_name, std::shared_ptr<BinlogFileMetaPB> file_meta);

    // Seek to the log entry containing the <version, seq_id>. This method
    // only can be called once for a file reader. Return Status::Ok() if find
    // the entry, Status::NotFound if there is no such change event, and other
    // status if there is other errors
    Status seek(int64_t version, int64_t seq_id);

    // Move to the next log entry.
    // Return Status::Ok() if find the entry, Status::EndOfFile
    // if there is no next, and other status if there is other errors
    Status next();

    // Get the information of the current log entry. The reader is
    // responsible for the memory allocation and release used by
    // the LogEntryInfo, and the memory also can be reused when next()
    // is called. The caller should not modify the data, and dependent
    // on it's state across two next().
    LogEntryInfo* log_entry();

    static Status parse_file_header(RandomAccessFile* read_file, int64_t file_size, BinlogFileHeaderPB* file_header,
                                    int64_t* read_file_size);

    static Status parse_file_footer(RandomAccessFile* read_file, int64_t file_size, BinlogFileMetaPB* file_meta);

    static Status parse_page_header(RandomAccessFile* read_file, int64_t file_size, int64_t file_pos,
                                    int64_t page_index, PageHeaderPB* page_header_pb, int64_t* read_file_size);

    // Load the file meta by scanning pages. Scan from the beginning of the file until to the first
    // page that is corrupted, or is no less than the parameter *max_lsn_exclusive*, and construct the
    // file meta according to those scanned pages. Returns Status::OK() and the file meta if there
    // is valid data. Returns Status::NotFound() if there is no valid data. Other status will be
    // returned if unexpected error happens.
    //
    // Why is it possible to have corrupted pages, and need to filter pages by max_lsn_exclusive?
    // A binlog file is appendable and can be shared by multiple ingestion. Considering a possible
    // process to fail to generate binlog for an ingestion
    // 1. the file already contains binlog for some successful ingestion
    // 2. a new ingestion comes, and appends a page to the binlog file, but not committed (fsync)
    // 4. BE exits because of core dump, or temporary disk error, the ingestion fails to publish,
    //    and the appended page is useless, but there is no chance to truncate it
    // 5. restart BE, and load binlog file meta by scanning pages
    // In this case, the last page can be
    // 1. corrupted because there may be only part of data written to the disk
    // 2. persisted successfully, but should be filtered by max_lsn_exclusive because failed to publish
    // So stop to scan pages until meeting the first corrupted or filtered page.
    static StatusOr<BinlogFileMetaPBPtr> load_meta_by_scan_pages(int64_t file_id, RandomAccessFile* read_file,
                                                                 int64_t file_size, BinlogLsn& max_lsn_exclusive);

    // Load the file meta from the binlog file. First try to get the file meta from the file footer.
    // If the footer not exists, or is invalid, scan pages to get the file meta. Returns Status::OK()
    // and the file meta if there is valid data. Returns Status::NotFound() if there is no valid data.
    // Other status will be returned if unexpected error happens.
    static StatusOr<BinlogFileMetaPBPtr> load_meta(int64_t file_id, std::string& file_path,
                                                   BinlogLsn& max_lsn_exclusive);

private:
    Status _seek(int64_t version, int64_t seq_id);
    // seek to the page containing the change event <version, seq_id>
    Status _seek_to_page(int64_t version, int64_t seq_id);
    // seek to the log entry containing the seq_id in a page
    Status _seek_to_log_entry(int64_t seq_id);
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
        PageHeaderPB& page_header = _current_page_context->page_header;
        LogEntryInfo* log_entry_info = _current_log_entry.get();
        log_entry_info->log_entry = nullptr;
        log_entry_info->version = page_header.version();
        log_entry_info->start_seq_id = page_header.start_seq_id();
        log_entry_info->end_seq_id = page_header.start_seq_id() - 1;
        log_entry_info->file_id = nullptr;
        log_entry_info->start_row_id = 0;
        log_entry_info->num_rows = 0;
        log_entry_info->end_of_version = false;
    }

    std::string _file_path;
    std::shared_ptr<BinlogFileMetaPB> _file_meta;
    std::unique_ptr<RandomAccessFile> _file;
    int64_t _file_size;
    int64_t _current_file_pos;
    std::unique_ptr<BinlogFileHeaderPB> _file_header;

    // the index of next page to read
    int64_t _next_page_index;
    std::unique_ptr<LogEntryInfo> _current_log_entry;
    std::unique_ptr<PageContext> _current_page_context;
};

} // namespace starrocks
