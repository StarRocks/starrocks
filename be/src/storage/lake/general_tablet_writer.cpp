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
#include "runtime/current_thread.h"
#include "serde/column_array_serde.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/vacuum.h"
#include "storage/rowset/segment_writer.h"
#include "util/threadpool.h"

namespace starrocks::lake {

HorizontalGeneralTabletWriter::HorizontalGeneralTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                             std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                             ThreadPool* flush_pool)
        : TabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, flush_pool) {}

HorizontalGeneralTabletWriter::~HorizontalGeneralTabletWriter() = default;

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status HorizontalGeneralTabletWriter::open() {
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::write(const starrocks::Chunk& data, SegmentPB* segment) {
    if (_seg_writer == nullptr || _seg_writer->estimate_segment_size() >= config::max_segment_file_size ||
        _seg_writer->num_rows_written() + data.num_rows() >= INT32_MAX /*TODO: configurable*/) {
        RETURN_IF_ERROR(flush_segment_writer(segment));
        RETURN_IF_ERROR(reset_segment_writer());
    }
    RETURN_IF_ERROR(_seg_writer->append_chunk(data));
    _num_rows += data.num_rows();
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::flush(SegmentPB* segment) {
    return flush_segment_writer(segment);
}

Status HorizontalGeneralTabletWriter::finish(SegmentPB* segment) {
    RETURN_IF_ERROR(flush_segment_writer(segment));
    _finished = true;
    return Status::OK();
}

void HorizontalGeneralTabletWriter::close() {
    if (!_finished && !_files.empty()) {
        std::vector<std::string> full_paths_to_delete;
        full_paths_to_delete.reserve(_files.size());
        for (const auto& f : _files) {
            full_paths_to_delete.emplace_back(_tablet_mgr->segment_location(_tablet_id, f.path));
        }
        delete_files_async(std::move(full_paths_to_delete));
    }
    _files.clear();
}

Status HorizontalGeneralTabletWriter::reset_segment_writer() {
    DCHECK(_schema != nullptr);
    auto name = gen_segment_filename(_txn_id);
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, name)));
    SegmentWriterOptions opts;
    auto w = std::make_unique<SegmentWriter>(std::move(of), _seg_id++, _schema, opts);
    RETURN_IF_ERROR(w->init());
    _seg_writer = std::move(w);
    return Status::OK();
}

Status HorizontalGeneralTabletWriter::flush_segment_writer(SegmentPB* segment) {
    if (_seg_writer != nullptr) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_seg_writer->finalize(&segment_size, &index_size, &footer_position));
        const std::string& segment_path = _seg_writer->segment_path();
        std::string segment_name = std::string(basename(segment_path));
        _files.emplace_back(FileInfo{segment_name, segment_size});
        _data_size += segment_size;
        if (segment) {
            segment->set_data_size(segment_size);
            segment->set_index_size(index_size);
            segment->set_path(segment_path);
        }
        _seg_writer.reset();
    }
    return Status::OK();
}

VerticalGeneralTabletWriter::VerticalGeneralTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                         std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                         uint32_t max_rows_per_segment, ThreadPool* flush_pool)
        : TabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, flush_pool),
          _max_rows_per_segment(max_rows_per_segment) {}

VerticalGeneralTabletWriter::~VerticalGeneralTabletWriter() {
    auto st = wait_futures_finish();
    if (!st.ok()) {
        LOG(WARNING) << "Fail to finalize segment, tablet_id: " << _tablet_id << ", txn_id: " << _txn_id
                     << ", status:" << st;
    }
}

// To developers: Do NOT perform any I/O in this method, because this method may be invoked
// in a bthread.
Status VerticalGeneralTabletWriter::open() {
    if (_flush_pool != nullptr) {
        // Use CONCURRENT mode to ensure segments can finalize in parallel
        _segment_writer_finalize_token =
                std::make_unique<ConcurrencyLimitedThreadPoolToken>(_flush_pool, _flush_pool->max_threads() * 2);
    }
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
            RETURN_IF_ERROR(flush_columns(_segment_writers[_current_writer_index]));
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
                    RETURN_IF_ERROR(flush_columns(_segment_writers[_current_writer_index]));
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

Status VerticalGeneralTabletWriter::flush(SegmentPB* segment) {
    return Status::OK();
}

Status VerticalGeneralTabletWriter::flush_columns() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_segment_writers[_current_writer_index]);
    RETURN_IF_ERROR(flush_columns(_segment_writers[_current_writer_index]));
    _current_writer_index = 0;

    if (_segment_writer_finalize_token != nullptr) {
        return wait_futures_finish();
    }
    return Status::OK();
}

Status VerticalGeneralTabletWriter::finish(SegmentPB* segment) {
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(segment_writer->finalize_footer(&segment_size, &footer_position));
        const std::string& segment_path = segment_writer->segment_path();
        std::string segment_name = std::string(basename(segment_path));
        _files.emplace_back(FileInfo{segment_name, segment_size});
        _data_size += segment_size;
        segment_writer.reset();
    }
    _segment_writers.clear();
    if (_segment_writer_finalize_token != nullptr) {
        _segment_writer_finalize_token.reset();
    }
    _finished = true;
    return Status::OK();
}

void VerticalGeneralTabletWriter::close() {
    if (!_finished && !_files.empty()) {
        std::vector<std::string> full_paths_to_delete;
        full_paths_to_delete.reserve(_files.size());
        for (const auto& f : _files) {
            full_paths_to_delete.emplace_back(_tablet_mgr->segment_location(_tablet_id, f.path));
        }
        delete_files_async(std::move(full_paths_to_delete));
    }
    _files.clear();
}

StatusOr<std::shared_ptr<SegmentWriter>> VerticalGeneralTabletWriter::create_segment_writer(
        const std::vector<uint32_t>& column_indexes, bool is_key) {
    DCHECK(_schema != nullptr);
    auto name = gen_segment_filename(_txn_id);
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(_tablet_mgr->segment_location(_tablet_id, name)));
    SegmentWriterOptions opts;
    auto w = std::make_shared<SegmentWriter>(std::move(of), _seg_id++, _schema, opts);
    RETURN_IF_ERROR(w->init(column_indexes, is_key));
    return w;
}

Status VerticalGeneralTabletWriter::flush_columns(const std::shared_ptr<SegmentWriter>& segment_writer) {
    if (_segment_writer_finalize_token != nullptr) {
        auto status = check_futures();
        if (!status.ok()) {
            return status;
        }
        auto mem_tracker = tls_thread_status.mem_tracker();
        auto task = std::make_shared<std::packaged_task<Status()>>([segment_writer, mem_tracker]() {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker);
            uint64_t index_size = 0;
            return segment_writer->finalize_columns(&index_size);
        });
        auto packaged_func = [task]() { (*task)(); };
        auto timeout_deadline =
                std::chrono::system_clock::now() + std::chrono::milliseconds(kDefaultTimeoutForAsyncWriteSegment);
        auto st = _segment_writer_finalize_token->submit_func(std::move(packaged_func), timeout_deadline);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to submit segment writer finalizing task to thread pool, " << st;
            return st;
        }
        _futures.push_back(task->get_future());
    } else {
        uint64_t index_size = 0;
        RETURN_IF_ERROR(segment_writer->finalize_columns(&index_size));
    }
    return Status::OK();
}

template <typename R>
bool is_ready(std::future<R> const& f) {
    return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

Status VerticalGeneralTabletWriter::check_futures() {
    for (auto it = _futures.begin(); it != _futures.end();) {
        if (is_ready(*it)) {
            auto st = it->get();
            if (!st.ok()) {
                LOG(WARNING) << "Segment flushing task resulted in error: " << st;
                return st;
            }
            it = _futures.erase(it);
        } else {
            ++it;
        }
    }
    return Status::OK();
}

Status VerticalGeneralTabletWriter::wait_futures_finish() {
    Status ret = Status::OK();
    for (auto& future : _futures) {
        if (auto st = future.get(); !st.ok()) {
            VLOG(3) << "Fail to finalize segment, " << st;
            ret.update(st);
        }
    }
    _futures.clear();
    return ret;
}

} // namespace starrocks::lake
