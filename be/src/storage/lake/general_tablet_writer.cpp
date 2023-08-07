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

#include "storage/lake/general_tablet_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "common/config.h"
#include "fs/fs_util.h"
#include "serde/column_array_serde.h"
#include "storage/lake/filenames.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

HorizontalGeneralTabletWriter::HorizontalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema,
                                                             int64_t txn_id)
        : TabletWriter(tablet, std::move(schema), txn_id) {}

HorizontalGeneralTabletWriter::~HorizontalGeneralTabletWriter() = default;

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status HorizontalGeneralTabletWriter::open() {
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::write(const starrocks::Chunk& data) {
    if (_seg_writer == nullptr || _seg_writer->estimate_segment_size() >= config::max_segment_file_size ||
        _seg_writer->num_rows_written() + data.num_rows() >= INT32_MAX /*TODO: configurable*/) {
        RETURN_IF_ERROR(flush_segment_writer());
        RETURN_IF_ERROR(reset_segment_writer());
    }
    RETURN_IF_ERROR(_seg_writer->append_chunk(data));
    _num_rows += data.num_rows();
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::flush() {
    return flush_segment_writer();
}

Status HorizontalGeneralTabletWriter::finish() {
    RETURN_IF_ERROR(flush_segment_writer());
    _finished = true;
    return Status::OK();
}

void HorizontalGeneralTabletWriter::close() {
    if (!_finished && !_files.empty()) {
        // Delete files
        auto maybe_fs = FileSystem::CreateSharedFromString(_tablet.root_location());
        if (maybe_fs.ok()) {
            auto fs = std::move(maybe_fs).value();
            for (const auto& name : _files) {
                auto path = _tablet.segment_location(name);
                (void)fs->delete_file(path);
            }
        }
    }
    _files.clear();
}

Status HorizontalGeneralTabletWriter::reset_segment_writer() {
    DCHECK(_schema != nullptr);
    auto name = gen_segment_filename(_txn_id);
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(_tablet.segment_location(name)));
    SegmentWriterOptions opts;
    auto w = std::make_unique<SegmentWriter>(std::move(of), _seg_id++, _schema.get(), opts);
    RETURN_IF_ERROR(w->init());
    _seg_writer = std::move(w);
    _files.emplace_back(std::move(name));
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::flush_segment_writer() {
    if (_seg_writer != nullptr) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_seg_writer->finalize(&segment_size, &index_size, &footer_position));
        _data_size += segment_size;
        _seg_writer.reset();
    }
    return Status::OK();
}

VerticalGeneralTabletWriter::VerticalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema,
                                                         int64_t txn_id, uint32_t max_rows_per_segment)
        : TabletWriter(tablet, std::move(schema), txn_id), _max_rows_per_segment(max_rows_per_segment) {}

VerticalGeneralTabletWriter::~VerticalGeneralTabletWriter() = default;

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status VerticalGeneralTabletWriter::open() {
    return Status::OK();
}

Status VerticalGeneralTabletWriter::write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes,
                                                  bool is_key) {
    const size_t chunk_num_rows = data.num_rows();
    if (_segment_writers.empty()) {
        DCHECK(is_key);
        auto segment_writer = create_segment_writer(column_indexes, is_key);
        if (!segment_writer.ok()) return segment_writer.status();
        _segment_writers.emplace_back(std::move(segment_writer).value());
        _current_writer_index = 0;
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(data));
    } else if (is_key) {
        // key columns
        if (_segment_writers[_current_writer_index]->num_rows_written() + chunk_num_rows >= _max_rows_per_segment) {
            RETURN_IF_ERROR(flush_columns(&_segment_writers[_current_writer_index]));
            auto segment_writer = create_segment_writer(column_indexes, is_key);
            if (!segment_writer.ok()) return segment_writer.status();
            _segment_writers.emplace_back(std::move(segment_writer).value());
            ++_current_writer_index;
        }
        RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(data));
    } else {
        // non key columns
        uint32_t num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
        uint32_t segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
        DCHECK_LE(num_rows_written, segment_num_rows);

        if (_current_writer_index == 0 && num_rows_written == 0) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
        }

        if (num_rows_written + chunk_num_rows <= segment_num_rows) {
            RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(data));
        } else {
            // split into multi chunks and write into multi segments
            auto write_chunk = data.clone_empty();
            size_t num_left_rows = chunk_num_rows;
            size_t offset = 0;
            while (num_left_rows > 0) {
                if (segment_num_rows == num_rows_written) {
                    RETURN_IF_ERROR(flush_columns(&_segment_writers[_current_writer_index]));
                    ++_current_writer_index;
                    RETURN_IF_ERROR(_segment_writers[_current_writer_index]->init(column_indexes, is_key));
                    num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
                    segment_num_rows = _segment_writers[_current_writer_index]->num_rows();
                }

                size_t write_size = segment_num_rows - num_rows_written;
                if (write_size > num_left_rows) {
                    write_size = num_left_rows;
                }
                write_chunk->append(data, offset, write_size);
                RETURN_IF_ERROR(_segment_writers[_current_writer_index]->append_chunk(*write_chunk));
                write_chunk->reset();
                num_left_rows -= write_size;
                offset += write_size;
                num_rows_written = _segment_writers[_current_writer_index]->num_rows_written();
            }
            DCHECK_EQ(0, num_left_rows);
            DCHECK_EQ(offset, chunk_num_rows);
        }
    }

    if (is_key) {
        _num_rows += chunk_num_rows;
    }
    return Status::OK();
}

Status VerticalGeneralTabletWriter::flush() {
    return Status::OK();
}

Status VerticalGeneralTabletWriter::flush_columns() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_segment_writers[_current_writer_index]);
    RETURN_IF_ERROR(flush_columns(&_segment_writers[_current_writer_index]));
    _current_writer_index = 0;
    return Status::OK();
}

Status VerticalGeneralTabletWriter::finish() {
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        uint64_t footer_position = 0;
        if (auto st = segment_writer->finalize_footer(&segment_size, &footer_position); !st.ok()) {
            LOG(WARNING) << "Fail to finalize segment footer, " << st;
            return st;
        }
        _data_size += segment_size;
        segment_writer.reset();
    }
    _segment_writers.clear();
    _finished = true;
    return Status::OK();
}

void VerticalGeneralTabletWriter::close() {
    if (!_finished && !_files.empty()) {
        // Delete files
        auto maybe_fs = FileSystem::CreateSharedFromString(_tablet.root_location());
        if (maybe_fs.ok()) {
            auto fs = std::move(maybe_fs).value();
            for (const auto& name : _files) {
                auto path = _tablet.segment_location(name);
                (void)fs->delete_file(path);
            }
        }
    }
    _files.clear();
}

StatusOr<std::unique_ptr<SegmentWriter>> VerticalGeneralTabletWriter::create_segment_writer(
        const std::vector<uint32_t>& column_indexes, bool is_key) {
    DCHECK(_schema != nullptr);
    auto name = gen_segment_filename(_txn_id);
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(_tablet.segment_location(name)));
    SegmentWriterOptions opts;
    auto w = std::make_unique<SegmentWriter>(std::move(of), _seg_id++, _schema.get(), opts);
    RETURN_IF_ERROR(w->init(column_indexes, is_key));
    _files.emplace_back(std::move(name));
    return w;
}

Status VerticalGeneralTabletWriter::flush_columns(std::unique_ptr<SegmentWriter>* segment_writer) {
    uint64_t index_size = 0;
    RETURN_IF_ERROR((*segment_writer)->finalize_columns(&index_size));
    return Status::OK();
}

} // namespace starrocks::lake
