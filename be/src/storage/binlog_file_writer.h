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

#include "fs/fs.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_util.h"
#include "storage/rowset/rowset.h"
#include "util/compression/block_compression.h"

namespace starrocks {

struct RowsetSegInfo {
    RowsetSegInfo(int64_t rid, int32_t sidx) {
        rowset_id = rid;
        seg_index = sidx;
    }

    int64_t rowset_id;
    int32_t seg_index;
};

// Binlog context for the current page to write
struct PendingPageContext {
    int64_t start_seq_id;
    int64_t end_seq_id;
    int32_t num_log_entries;
    int32_t last_segment_index;
    int32_t last_row_id;
    int32_t estimated_page_size;
    std::unordered_set<int64_t> rowsets;
    PageHeaderPB page_header;
    PageContentPB page_content;
};

// Binlog context for the current version to write
struct PendingVersionContext {
    int64_t version;
    int64_t start_seq_id;
    int64_t change_event_timestamp_in_us;
    int64_t num_pages;
    std::unordered_set<int64_t> rowsets;
};

extern const char* const k_binlog_magic_number;
extern const uint32_t k_binlog_magic_number_length;
extern const int32_t k_binlog_format_version;

enum WriterState {
    // waiting for initialization
    WAITING_INIT,
    // waiting for begin
    WAITING_BEGIN,
    // writing binlog data
    WRITING,
    // the writer is closed
    CLOSED
};

// Build the binlog file which records the information of change events (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE).
// Change events from multiple ingestion are written to the file sequentially and the order is the same as that
// ingestion is published. A change event has several metas
// +-----------+----------------------------------------------------------------------------------------------+
// | Name      | Description                                                                                  |
// +===========+==============================================================================================+
// | op        | Operation type of the change event, including INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE    |
// +-----------+----------------------------------------------------------------------------------------------+
// | version   | The version of ingestion to generate the change event, it's same as the publish version      |
// +-----------+----------------------------------------------------------------------------------------------+
// | seq_id    | The unique sequence number of the change event in the ingestion generating it                |
// +-----------+----------------------------------------------------------------------------------------------+
// | timestamp | The timestamp to generate the change event                                                   |
// +-----------+----------------------------------------------------------------------------------------------+
// | lsn       | The unique sequence number of the change event across all the ingestion. It's a combination  |
// |           | of <version, seq_id>                                                                         |
// +-----------+----------------------------------------------------------------------------------------------+
//
// A binlog file contains multiple pages, each page contains multiple log entries, and each entry contains one or
// more change events. The file layout is as following
//  +----------------------+------+--------+------+------------------------+
//  |      File Header     | Page | ...... | Page | File Footer [optional] |
//  | (BinlogFileHeaderPB) |      |        |      |   (BinlogFileMetaPB)   |
//  +----------------------+------+--------+------+------------------------+
//  The page layout is as following
//  +----------------------------+----------------------------------------------------------+
//  | Page Header (PageHeaderPB) |               Page Content (PageContentPB)               |
//  |                            +------------------------+--------+------------------------+
//  |                            | Log Entry (LogEntryPB) | ...... | Log Entry (LogEntryPB) |
//  +----------------------------+------------------------+--------+------------------------+
// A page only contains change events from one version, and can not be shared among versions. Page is the unit of I/O
// and compression. The log entry is defined as a protobuf message, and currently there are four types of log entries.
//    +--------------+------------------------------------------------------------------------------------------+
//    | Type         | Description                                                                              |
//    +==============+==========================================================================================+
//    | INSERT_RANGE | Insert a batch of new rows into the table, and they are continuous in a segment file.    |
//    |              | It will generate multiple INSERT change events                                           |
//    +--------------+------------------------------------------------------------------------------------------+
//    | UPDATE       | Update an existing row. It will generate a UPDATE_BEFORE and a UPDATE_AFTER.             |
//    +--------------+------------------------------------------------------------------------------------------+
//    | DELETE       | Delete an existing row. It will generate a DELETE change event                           |
//    +--------------+------------------------------------------------------------------------------------------+
//    | EMPTY        | There is no data change in an ingestion                                                        |
//    +--------------+------------------------------------------------------------------------------------------+
//
// How to use the writer
//  std::shared_ptr<BinlogFileWriter> file_writer;
//  file_writer->init();
//  // prepare when starting to write data of a new version
//  file_writer->begin();
//  // add multiple items
//  file_writer->add_insert_range():
//  file_writer->add_update();
//  file_writer->add_delete();
//  // commit the binlog data in this version
//  file_writer->commit();
//  // abort this version if there is any error
//  file_writer->abort();
class BinlogFileWriter {
public:
    BinlogFileWriter(int64_t file_id, std::string path, int32_t page_size, CompressionTypePB compression_type);

    // Initialize the writer as a newly created file.
    Status init();

    // Initialize the writer as an already existed file, and reset the writer to
    // the state described by the *previous_meta*. The meta can be a valid result
    // for a previously successful ingestion. The binlog file maybe truncated to
    // the previous size, and if init successfully, new data can be appended to the
    // binlog file
    Status init(BinlogFileMetaPB* previous_meta);

    // Begin to write binlog for a new version.
    Status begin(int64_t version, int64_t start_seq_id, int64_t change_event_timestamp_in_us);

    // Add an empty log entry when there is no data in an ingestion.
    // An empty version can only have one log entry, and it must be
    // the typ of EMPTY. abort() should be called if the returned
    // status is not Status::OK().
    Status add_empty();

    // Add an INSERT_RANGE log entry. abort() should be called if
    // the returned status is not Status::OK().
    Status add_insert_range(const RowsetSegInfo& seg_info, int32_t start_row_id, int32_t num_rows);

    // Add an UPDATE log entry. abort() should be called if
    // the returned status is not Status::OK().
    Status add_update(const RowsetSegInfo& before_info, int32_t before_row_id, const RowsetSegInfo& after_info,
                      int after_row_id);

    // Add an DELETE log entry. abort() should be called if
    // the returned status is not Status::OK().
    Status add_delete(const RowsetSegInfo& delete_info, int32_t row_id);

    // Commit the data of the version, and tell the binlog file
    // whether this is the end the version. abort() should be
    // called if the returned status is not Status::OK().
    Status commit(bool end_of_version);

    // Abort the write process for current version if there happens any
    // error when adding data or committing. If the returned status is
    // not Status::OK(), can't append data to this writer anymore because
    // the writer's state is unknown, but the committed binlog is still
    // available.
    Status abort();

    // Reset the binlog file writer to the state described by the *previous_meta*.
    // The meta should be a valid result for a previously successful ingestion.
    // The reset operation will try to truncate the binlog file to the previous size,
    // and update the meta of the writer. If reset successfully, new data can append
    // to the binlog file, otherwise it's unsafe to append, and should close it,
    // but the data before *previous_meta* is still available.
    Status reset(BinlogFileMetaPB* previous_meta);

    // Close the file writer
    Status close(bool append_file_meta);

    bool closed() { return _writer_state == CLOSED; }

    bool is_writing() { return _writer_state == WRITING; }

    bool is_closed() { return _writer_state == CLOSED; }

    // Actual file size currently, including the data committed
    // and pending to commit
    int64_t file_size() { return _file->size(); }

    int64_t file_id() { return _file_id; }

    std::string file_path() { return _file_path; }

    void copy_file_meta(BinlogFileMetaPB* new_file_meta) { new_file_meta->CopyFrom(*_file_meta.get()); }

    // For testing
    std::unordered_set<int64_t>& rowsets() { return _rowsets; }

    // Reopen an existed binlog file to append data. The writer will be reset
    // to the state described by *previous_meta*.
    static StatusOr<std::shared_ptr<BinlogFileWriter>> reopen(int64_t file_id, const std::string& file_path,
                                                              int32_t page_size, CompressionTypePB compression_type,
                                                              BinlogFileMetaPB* previous_meta);

private:
    Status _check_state(WriterState expect_state);
    Status _switch_page_if_full();
    Status _flush_page(bool end_of_version);
    Status _append_page(bool end_of_version);
    void _append_file_meta();
    Status _truncate_file(int64_t file_size);
    void _reset_pending_context();

    void _set_file_id_pb(int64_t rowset_id, int seg_index, FileIdPB* file_id_pb) {
        file_id_pb->set_rowset_id(rowset_id);
        file_id_pb->set_segment_index(seg_index);
    }

    int64_t _file_id;
    std::string _file_path;
    int32_t _max_page_size;
    CompressionTypePB _compression_type;
    const BlockCompressionCodec* _compress_codec = nullptr;

    std::unique_ptr<WritableFile> _file;
    WriterState _writer_state;
    // file meta for committed data
    std::unique_ptr<BinlogFileMetaPB> _file_meta;
    // rowsets used by committed data
    std::unordered_set<int64_t> _rowsets;

    // context for the version pending to commit
    std::unique_ptr<PendingVersionContext> _pending_version_context;
    // context for the page pending to flush
    std::unique_ptr<PendingPageContext> _pending_page_context;
};

using BinlogFileWriterPtr = std::shared_ptr<BinlogFileWriter>;

} // namespace starrocks
