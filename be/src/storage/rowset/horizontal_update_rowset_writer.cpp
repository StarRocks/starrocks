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

#include "storage/rowset/horizontal_update_rowset_writer.h"

#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {

HorizontalUpdateRowsetWriter::HorizontalUpdateRowsetWriter(const RowsetWriterContext& context)
        : RowsetWriter(context), _update_file_writer(nullptr) {}

HorizontalUpdateRowsetWriter::~HorizontalUpdateRowsetWriter() {
    if (!_already_built) {           // abnormal exit, remove all files generated
        _update_file_writer.reset(); // ensure all files are closed
        for (auto i = 0; i < _num_uptfile; ++i) {
            auto path = Rowset::segment_upt_file_path(_context.rowset_path_prefix, _context.rowset_id, i);
            auto st = _fs->delete_file(path);
            LOG_IF(WARNING, !(st.ok() || st.is_not_found()))
                    << "Fail to delete file=" << path << ", " << st.to_string();
        }
        // if _already_built is false, we need to release rowset_id to avoid rowset_id leak
        StorageEngine::instance()->release_rowset_id(_context.rowset_id);
    }
}

StatusOr<std::unique_ptr<SegmentWriter>> HorizontalUpdateRowsetWriter::_create_update_file_writer() {
    std::lock_guard<std::mutex> l(_lock);
    std::string path = Rowset::segment_upt_file_path(_context.rowset_path_prefix, _context.rowset_id, _num_uptfile);
    ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(path));
    const auto* schema = _context.tablet_schema;
    auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), _num_uptfile, schema, _writer_options);
    RETURN_IF_ERROR(segment_writer->init());
    return std::move(segment_writer);
}

Status HorizontalUpdateRowsetWriter::add_chunk(const Chunk& chunk) {
    if (_update_file_writer == nullptr) {
        ASSIGN_OR_RETURN(_update_file_writer, _create_update_file_writer());
    } else if (_update_file_writer->estimate_segment_size() >= config::max_segment_file_size) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_update_file_writer->finalize(&segment_size, &index_size, &footer_position));
        {
            std::lock_guard<std::mutex> l(_lock);
            _num_rows_upt += _update_file_writer->num_rows_written();
            _num_uptfile++;
            _total_update_row_size += static_cast<int64_t>(chunk.bytes_usage());
        }
        ASSIGN_OR_RETURN(_update_file_writer, _create_update_file_writer());
    }

    return _update_file_writer->append_chunk(chunk);
}

// flush and generate `.upt` file
Status HorizontalUpdateRowsetWriter::flush_chunk(const Chunk& chunk, SegmentPB* seg_info) {
    auto segment_writer = _create_update_file_writer();
    if (!segment_writer.ok()) {
        return segment_writer.status();
    }
    RETURN_IF_ERROR((*segment_writer)->append_chunk(chunk));
    uint64_t segment_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize(&segment_size, &index_size, &footer_position));
    if (seg_info) {
        seg_info->set_update_num_rows(static_cast<int64_t>(chunk.num_rows()));
        seg_info->set_update_id(_num_uptfile);
        seg_info->set_update_data_size(segment_size);
        seg_info->set_update_path((*segment_writer)->segment_path());
        seg_info->set_update_row_size(static_cast<int64_t>(chunk.bytes_usage()));
    }
    {
        std::lock_guard<std::mutex> l(_lock);
        _num_rows_upt += chunk.num_rows();
        _num_uptfile++;
        _total_update_row_size += static_cast<int64_t>(chunk.bytes_usage());
    }
    (*segment_writer).reset();
    return Status::OK();
}

// flush and generate `.upt` file
Status HorizontalUpdateRowsetWriter::flush() {
    if (_update_file_writer != nullptr) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_update_file_writer->finalize(&segment_size, &index_size, &footer_position));
        {
            std::lock_guard<std::mutex> l(_lock);
            _num_rows_upt += _update_file_writer->num_rows_written();
            _num_uptfile++;
        }
        _update_file_writer.reset();
    }
    return Status::OK();
}

} // namespace starrocks