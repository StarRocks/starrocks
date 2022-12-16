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
#include "storage/rowset/rowset.h"
#include "util/compression/block_compression.h"

namespace starrocks {

struct RowsetSegInfo {
    RowsetId& rowset_id;
    int seg_index;
};

// Binlog context for the current page to write
struct PendingPageContext {
    int64_t start_seq_id;
    int64_t end_seq_id;
    int32_t num_log_entries;
    int32_t last_segment_index;
    int32_t last_row_id;
    int32_t estimated_page_size;
    std::unordered_set<RowsetId, HashOfRowsetId> rowsets;
    PageHeaderPB page_header;
    PageContentPB page_content;
};

// Binlog context for the current version to write
struct PendingVersionContext {
    int64_t version;
    RowsetId rowset_id;
    int64_t start_seq_id;
    int64_t change_event_timestamp_in_us;
    int32_t num_pages;
    std::unordered_set<RowsetId, HashOfRowsetId> rowsets;
};

extern const char* const k_binlog_magic_number;
extern const uint32_t k_binlog_magic_number_length;
extern const int32_t k_binlog_format_version;

enum WriterState { WAIT_INIT, WAIT_WRITE, PREPARE, CLOSED };

// Build the binlog file. A binlog file contains multiple pages, each page contains multiple log entries,
// and each entry contains one or more change events(INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE). Binlog
// from multiple version are written to the file sequentially. A page only contains change events from one
// version, and can not be shared among version. The log entry is defined as a protobuf message, and
// currently there are four types of log entries, and you can refer to binlog.proto for details.
//    +--------------+------------------------------------------------------------------------------------------+
//    | Type         | Description                                                                              |
//    +==============+==========================================================================================+
//    | INSERT_RANGE | Insert a batch of new rows into the table, and they are continuous in a segment file.    |
//    |              | It will generate multiple INSERT change events                                           |
//    +--------------+------------------------------------------------------------------------------------------+
//    | UPDATE       | Update an existing row. It will generate two UPDATE_BEFORE and UPDATE_AFTER change events|
//    +--------------+------------------------------------------------------------------------------------------+
//    | DELETE       | Delete an existing row. It will generate a DELETE change event                           |
//    +--------------+------------------------------------------------------------------------------------------+
//    | EMPTY        | There is no data change in an ingestion                                                        |
//    +--------------+------------------------------------------------------------------------------------------+
//
// How to use
//  std::shared_ptr<BinlogFileWriter> file_writer;
//  file_writer->init();
//  // prepare when starting to write data of a new version
//  file_writer->prepare();
//  // add multiple items
//  file_writer->add_insert_range():
//  file_writer->add_update();
//  file_writer->add_delete();
//  // commit the binlog data in this version
//  file_writer->commit();
//  // or abort for this version if there is any error
//  file_writer->abort();
class BinlogFileWriter {
public:
    BinlogFileWriter(int64_t file_id, std::string path, int32_t page_size, CompressionTypePB compression_type);

    // Initialize the file writer.
    Status init();

    // Prepare to write binlog for a new version.
    Status prepare(int64_t version, const RowsetId& rowset_id, int64_t start_seq_id,
                   int64_t change_event_timestamp_in_us);

    // Add an empty log entry when there is no data in an ingestion.
    // An empty version can only have one log entry, and it must be
    // the typ of EMPTY. abort() should be called if the returned
    // status is not Status::OK().
    Status add_empty();

    // Add an INSERT_RANGE log entry. abort() should be called if
    // the returned status is not Status::OK().
    Status add_insert_range(int32_t seg_index, int32_t start_row_id, int32_t num_rows);

    // Add an UPDATE log entry. abort() should be called if
    // the returned status is not Status::OK().
    Status add_update(const RowsetSegInfo& before_info, int32_t before_row_id, int32_t after_seg_index,
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

    // Close the file writer
    Status close();

    // Binlog file size for committed data
    int64_t committed_file_size() { return _file_meta->file_size(); }

    // Actual file size currently, and include the data committed
    // and pending to commit
    int64_t file_size() { return _file->size(); }

    // The sequence number of the last change event pending to commit
    int64_t pending_end_seq_id() { return _pending_page_context->end_seq_id; }

    int64_t file_id() { return _file_id; }

    std::string file_name() { return _file_path; }

    void copy_file_meta(BinlogFileMetaPB* new_file_meta) { new_file_meta->CopyFrom(*_file_meta.get()); }

    static std::string binlog_file_path(std::string& dir, int64_t file_id) {
        return strings::Substitute("$0/$1.binlog", dir, file_id);
    }

private:
    Status _switch_page_if_full();
    Status _flush_page(bool end_of_version);
    void _append_file_meta();
    void _reset_pending_context();

    void _set_row_id_pb(const RowsetId& rowset_id, RowsetIdPB* rowset_id_pb) {
        rowset_id_pb->set_lo(rowset_id.lo);
        rowset_id_pb->set_mi(rowset_id.mi);
        rowset_id_pb->set_hi(rowset_id.hi);
    }

    void _set_file_id_pb(const RowsetId& rowset_id, int seg_index, FileIdPB* file_id_pb) {
        _set_row_id_pb(rowset_id, file_id_pb->mutable_rowset_id());
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
    std::unordered_set<RowsetId, HashOfRowsetId> _rowsets;

    // context for the version pending to commit
    std::unique_ptr<PendingVersionContext> _pending_version_context;
    // context for the page pending to flush
    std::unique_ptr<PendingPageContext> _pending_page_context;
};

} // namespace starrocks
