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

struct PendingPageContext {
    int64_t start_seq_id;
    int64_t start_changelog_id;
    int64_t end_seq_id;
    int64_t end_changelog_id;
    int32_t num_log_entries;
    int32_t last_segment_index;
    int32_t last_row_id;
    int32_t estimated_page_size;
    std::unordered_set<RowsetId, HashOfRowsetId> rowsets;
    PageHeaderPB page_header;
    PageContentPB page_content;
};

struct PendingWriteContext {
    int64_t tablet_version;
    RowsetId rowset_id;
    int64_t start_seq_id;
    int64_t start_changelog_id;
    int64_t rowset_creation_time_in_sec;
    std::unordered_set<RowsetId, HashOfRowsetId> rowsets;
};

extern const char* const k_binlog_magic_number;
extern const uint32_t k_binlog_magic_number_length;
extern const int32_t k_binlog_format_version;

enum WriterState { WAIT_INIT, WAIT_WRITE, PREPARE, CLOSED };

// TODO add metrics for write
class BinlogFileWriter {
public:
    BinlogFileWriter(int64_t file_id, std::string path, int32_t page_size, CompressionTypePB compression_type);

    Status init();

    Status prepare(int64_t tablet_version, const RowsetId& rowset_id, int64_t start_seq_id, int64_t start_changelog_id,
                   int64_t rowset_creation_time_in_sec);

    Status add_empty();

    Status add_insert_range(int32_t seg_index, int32_t start_row_id, int32_t end_row_id);

    Status add_update(const RowsetSegInfo& before_info, int32_t before_row_id, int32_t after_seg_index,
                      int after_row_id);

    Status add_delete(const RowsetSegInfo& delete_info, int32_t row_id);

    Status commit(bool end_of_tablet_version);

    Status abort();

    Status rollback(int64_t tablet_version);

    Status close();

    int64_t committed_file_size() {
        // TODO should include rowset size
        return _file_meta->file_size();
    }

    int64_t pending_file_size() {
        // TODO should include rowset size
        return _file->size();
    }

    int64_t pending_end_seq_id() { return _pending_page_context->end_seq_id; }

    int64_t pending_end_changelog_id() { return _pending_page_context->end_changelog_id; }

    int64_t file_id() { return _file_id; }

    std::string file_name() { return _file_name; }

    void copy_file_meta(BinlogFileMetaPB* new_file_meta) { new_file_meta->CopyFrom(*_file_meta.get()); }

private:
    Status _switch_page_if_full();
    Status _flush_page(bool is_end_page);
    void _try_append_file_meta();
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
    std::string _file_name;
    int32_t _max_page_size;
    CompressionTypePB _compression_type;
    const BlockCompressionCodec* _compress_codec = nullptr;

    std::unique_ptr<WritableFile> _file;
    std::unique_ptr<BinlogFileMetaPB> _file_meta;
    std::unordered_set<RowsetId, HashOfRowsetId> _rowsets;

    std::unique_ptr<PendingWriteContext> _pending_write_context;
    std::unique_ptr<PendingPageContext> _pending_page_context;

    WriterState _writer_state;
};

} // namespace starrocks
