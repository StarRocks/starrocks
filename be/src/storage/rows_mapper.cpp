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

#include "storage/rows_mapper.h"

#include <fmt/format.h>

#include "base/coding.h"
#include "base/container/raw_container.h"
#include "base/debug/trace.h"
#include "base/hash/crc32c.h"
#include "common/config_primary_key_fwd.h"
#include "common/thread/threadpool.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "lake/filenames.h"
#include "runtime/env/global_env.h"
#include "storage/data_dir.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_engine.h"

namespace starrocks {

Status RowsMapperBuilder::_init() {
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(_filename));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_wfile, fs->new_writable_file(wblock_opts, _filename));
    return Status::OK();
}

Status RowsMapperBuilder::append(const std::vector<uint64_t>& rssid_rowids) {
    if (rssid_rowids.empty()) {
        // skip create rows mapper file when output rowset is empty.
        return Status::OK();
    }
    if (_wfile == nullptr) {
        RETURN_IF_ERROR(_init());
    }
    RETURN_IF_ERROR(_wfile->append(Slice((const char*)rssid_rowids.data(), rssid_rowids.size() * 8)));
    _checksum = crc32c::Extend(_checksum, (const char*)rssid_rowids.data(), rssid_rowids.size() * 8);
    _row_count += rssid_rowids.size();
    return Status::OK();
}

Status RowsMapperBuilder::finalize() {
    if (_wfile == nullptr) {
        // Empty rows, skip finalize
        return Status::OK();
    }
    std::string row_count_str;
    // row count
    put_fixed64_le(&row_count_str, _row_count);
    _checksum = crc32c::Extend(_checksum, row_count_str.data(), row_count_str.size());
    // checksum
    std::string checksum_str;
    put_fixed32_le(&checksum_str, _checksum);
    RETURN_IF_ERROR(_wfile->append(row_count_str));
    RETURN_IF_ERROR(_wfile->append(checksum_str));
    return _wfile->close();
}

FileInfo RowsMapperBuilder::file_info() const {
    FileInfo info;
    // WHY: Include file size to optimize remote storage access. For S3/HDFS, avoiding
    // a separate get_size() call saves ~10-50ms per file, significant during parallel pk execution
    // where hundreds of mapper files may be accessed concurrently.
    if (_wfile) {
        info.path = file_name(_filename);
        info.size = _wfile->size();
    } else {
        info.path = "";
    }
    return info;
}

RowsMapperIterator::~RowsMapperIterator() {
    // Background per-segment reads hold raw pointers into chunk-owned vectors
    // and per-chunk RAFs; wait them out before tearing down state.
    _drain_in_flight();
    if (_rfile != nullptr) {
        const std::string filename = _rfile->filename();
        _rfile.reset(nullptr);
        // IMPORTANT: Different cleanup strategies for local vs remote mapper files
        if (!lake::is_lcrm(file_name(filename))) {
            // WHY: Local .crm files are temporary and stored on local disk, so we delete them
            // immediately after use to free disk space. This is safe since they're only used
            // during a single compaction operation.
            auto st = fs::delete_file(filename);
            if (!st.ok()) {
                LOG(ERROR) << "delete rows mapper file fail, st: " << st;
            }
        }
        // NOTE: .lcrm files are stored on remote storage (S3/HDFS) and managed by tablet metadata.
        // They must NOT be deleted here because:
        // 1. Multiple nodes may be reading them concurrently during parallel pk execution
        // 2. They're referenced in metadata and will be cleaned up via GC process
        // 3. Premature deletion would cause data inconsistency for in-flight transactions
    }
}

// Open file
Status RowsMapperIterator::open(const FileInfo& filename) {
    _path = filename.path;
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(filename.path));
    // The .lcrm rows-mapper file is read exactly once during compaction publish and
    // then never accessed again — caching it locally is pure cache pollution that
    // evicts hotter data (PK index pages, segment blocks). Disable fill-cache so the
    // bytes are streamed through but not retained.
    RandomAccessFileOptions opts;
    opts.skip_fill_local_cache = true;
    ASSIGN_OR_RETURN(_rfile, fs->new_random_access_file(opts, filename.path));
    int64_t file_size = 0;
    // PERFORMANCE OPTIMIZATION: Reuse file size if already known
    if (filename.size.has_value()) {
        // WHY: For remote storage (S3/HDFS), get_size() requires a HEAD request which adds
        // 10-50ms latency. When processing hundreds of files during parallel pk execution,
        // this optimization saves seconds of total latency by reusing size from metadata.
        file_size = filename.size.value();
    } else {
        // Fallback: Query file size if not provided (local fs case, very fast)
        ASSIGN_OR_RETURN(file_size, _rfile->get_size());
    }
    // Surface the .lcrm body size in the COMPACTION publish trace so we can
    // correlate slow compact_mapper_read_us tails with file size.
    TRACE_COUNTER_INCREMENT("compact_mapper_filesize_bytes", file_size);
    // 1. read checksum
    std::string checksum_str;
    raw::stl_string_resize_uninitialized(&checksum_str, 4);
    RETURN_IF_ERROR(_rfile->read_at_fully(file_size - 4, checksum_str.data(), checksum_str.size()));
    _expected_checksum = decode_fixed32_le((const uint8_t*)checksum_str.data());
    // 2. read row count
    std::string row_count_str;
    raw::stl_string_resize_uninitialized(&row_count_str, 8);
    RETURN_IF_ERROR(_rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
    _row_count = decode_fixed64_le((const uint8_t*)row_count_str.data());
    // 3. check file size (should be 4 bytes(checksum) + 8 bytes(row count) + id list)
    if (file_size != 12 + _row_count * EACH_ROW_SIZE) {
        return Status::Corruption(
                fmt::format("RowsMapper file corruption. file size: {}, row count: {}", file_size, _row_count));
    }
    return Status::OK();
}

Status RowsMapperIterator::prepare_segments(const std::vector<size_t>& segment_row_counts) {
    // One-shot configuration: must be called before any `next_values`.
    if (_pos != 0 || _pipelined) {
        return Status::InternalError("RowsMapperIterator::prepare_segments must be called once, before next_values");
    }
    size_t sum = 0;
    for (auto n : segment_row_counts) sum += n;
    if (sum != _row_count) {
        return Status::Corruption(fmt::format(
                "RowsMapperIterator: declared segment-row-count sum {} != file row count {}", sum, _row_count));
    }
    _segment_sizes = segment_row_counts;
    _segment_offsets.resize(segment_row_counts.size());
    int64_t offset = 0;
    for (size_t i = 0; i < segment_row_counts.size(); ++i) {
        _segment_offsets[i] = offset;
        offset += static_cast<int64_t>(segment_row_counts[i]) * EACH_ROW_SIZE;
    }
    _pipelined = true;
    _next_to_submit = 0;
    _next_to_serve = 0;

    // Submit the first K chunks immediately so the caller can pipeline the
    // per-segment processing of segment_0 against the still-outstanding reads
    // for segments 1..K-1.
    const int32_t K = std::max(1, config::lake_rows_mapper_read_parallelism);
    for (int32_t i = 0; i < K; ++i) {
        if (_next_to_submit >= _segment_sizes.size()) break;
        RETURN_IF_ERROR(_maybe_submit_next());
    }
    return Status::OK();
}

Status RowsMapperIterator::_maybe_submit_next() {
    if (_next_to_submit >= _segment_sizes.size()) return Status::OK();
    const size_t seg = _next_to_submit++;
    const size_t row_count = _segment_sizes[seg];

    InFlightChunk chunk;
    chunk.segment_idx = seg;
    chunk.row_count = row_count;
    chunk.file_offset = _segment_offsets[seg];

    if (row_count == 0) {
        // Empty segment — no read needed. Build a ready future returning OK so the
        // consumer can still pop it uniformly.
        std::promise<Status> p;
        p.set_value(Status::OK());
        chunk.future = p.get_future().share();
        _in_flight.push_back(std::move(chunk));
        return Status::OK();
    }

    // Each in-flight chunk owns its own RandomAccessFile so concurrent reads never
    // touch shared file-class state (the underlying starlet wrapper has been
    // observed to crash under concurrent access on the same handle).
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_path));
    RandomAccessFileOptions opts;
    opts.skip_fill_local_cache = true;
    ASSIGN_OR_RETURN(chunk.rf, fs->new_random_access_file(opts, _path));

    chunk.data.resize(row_count);

    auto promise = std::make_shared<std::promise<Status>>();
    chunk.future = promise->get_future().share();

    auto* rf_raw = chunk.rf.get();
    uint64_t* data_raw = chunk.data.data();
    const size_t bytes = row_count * EACH_ROW_SIZE;
    const int64_t off = chunk.file_offset;

    _in_flight.push_back(std::move(chunk));

    auto* pool = GlobalEnv::GetInstance()->pk_index_execution_thread_pool();
    if (pool == nullptr) {
        // Pool unavailable — execute inline so the promise is always satisfied.
        promise->set_value(rf_raw->read_at_fully(off, data_raw, bytes));
        return Status::OK();
    }
    auto submit_st = pool->submit_func([promise, rf_raw, data_raw, off, bytes]() {
        promise->set_value(rf_raw->read_at_fully(off, data_raw, bytes));
    });
    if (!submit_st.ok()) {
        // submit_func failed — execute inline so the promise still resolves
        // (otherwise `next_values` would block forever waiting on it).
        promise->set_value(rf_raw->read_at_fully(off, data_raw, bytes));
    }
    return Status::OK();
}

void RowsMapperIterator::_drain_in_flight() {
    // Wait on every still-pending chunk. Tasks hold raw pointers into chunk-owned
    // vectors / RAFs that are about to be destroyed.
    while (!_in_flight.empty()) {
        auto& front = _in_flight.front();
        (void)front.future.get();
        _in_flight.pop_front();
    }
}

Status RowsMapperIterator::next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids) {
    if (fetch_cnt == 0) {
        // No need to fetch
        return Status::OK();
    }
    if (_pos + fetch_cnt > _row_count) {
        return Status::EndOfFile(fmt::format("RowsMapperIterator end of file. position/fetch cnt/row count: {}/{}/{}",
                                             _pos, fetch_cnt, _row_count));
    }

    if (_pipelined) {
        // Pipelined mode: caller must consume segments in the order declared by
        // prepare_segments, with `fetch_cnt` matching `_segment_sizes[_next_to_serve]`.
        if (_next_to_serve >= _segment_sizes.size() || fetch_cnt != _segment_sizes[_next_to_serve]) {
            return Status::InternalError(fmt::format(
                    "RowsMapperIterator pipelined consume mismatch: next_to_serve={} fetch_cnt={} expected={}",
                    _next_to_serve, fetch_cnt,
                    _next_to_serve < _segment_sizes.size() ? _segment_sizes[_next_to_serve] : 0));
        }
        // Block on the front chunk only; later chunks keep downloading.
        auto& front = _in_flight.front();
        RETURN_IF_ERROR(front.future.get());
        // O(1) swap — no memcpy. Caller gets ownership of the read data; the
        // chunk's now-empty vector goes away with `pop_front` below.
        rssid_rowids->swap(front.data);
        _in_flight.pop_front();
        ++_next_to_serve;
        // Refill the pipeline: one chunk consumed, submit the next-after-K-th
        // segment so K reads stay in flight (subject to remaining segments).
        RETURN_IF_ERROR(_maybe_submit_next());
    } else {
        // Sequential fallback: single direct read into the caller's buffer.
        rssid_rowids->resize(fetch_cnt);
        RETURN_IF_ERROR(_rfile->read_at_fully(static_cast<int64_t>(_pos) * EACH_ROW_SIZE, rssid_rowids->data(),
                                              fetch_cnt * EACH_ROW_SIZE));
    }

    _current_checksum = crc32c::Extend(_current_checksum, (const char*)rssid_rowids->data(), rssid_rowids->size() * 8);
    _pos += fetch_cnt;
    return Status::OK();
}

Status RowsMapperIterator::status() {
    if (_pos != _row_count) {
        return Status::Corruption(fmt::format("Chunk vs rows mapper's row count mismatch. {} vs {} filename: {}", _pos,
                                              _row_count, _rfile->filename()));
    }
    std::string row_count_str;
    // row count
    put_fixed64_le(&row_count_str, _row_count);
    _current_checksum = crc32c::Extend(_current_checksum, row_count_str.data(), row_count_str.size());
    if (_expected_checksum != _current_checksum) {
        return Status::Corruption(fmt::format("checksum mismatch. cur: {} expected: {} filename: {}", _current_checksum,
                                              _expected_checksum, _rfile->filename()));
    }
    return Status::OK();
}

StatusOr<std::string> new_lake_rows_mapper_filename(lake::TabletManager* mgr, int64_t tablet_id, int64_t txn_id) {
    // DESIGN DECISION: Storage location depends on execution mode
    if (config::enable_pk_index_parallel_execution) {
        // WHY: Remote storage (.lcrm) for parallel execution mode
        // TRADEOFF: Slower I/O (~50-200ms) vs multi-node accessibility
        // During parallel pk index execution, multiple compute nodes may need to read
        // the same mapper file simultaneously. Storing on S3/HDFS allows all nodes to
        // access it without file replication, enabling true distributed processing.
        // Performance impact is acceptable since parallel execution gains outweigh I/O overhead.
        return mgr->lcrm_location(tablet_id, lake::gen_lcrm_filename(txn_id));
    }
    // WHY: Local disk (.crm) for single-node execution mode
    // TRADEOFF: Fast I/O (~1-5ms) vs single-node limitation
    // When parallel execution is disabled, using local disk provides 10-100x faster I/O.
    // This is the optimal choice for single-node compaction where distributed access isn't needed.
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    if (data_dir == nullptr) {
        return Status::NotFound(fmt::format("Not local disk found. tablet id: {}", tablet_id));
    }
    return data_dir->get_tmp_path() + "/" + fmt::format("{:016X}_{:016X}.crm", tablet_id, txn_id);
}

StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id) {
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    if (data_dir == nullptr) {
        return Status::NotFound(fmt::format("Not local disk found. tablet id: {}", tablet_id));
    }
    return data_dir->get_tmp_path() + "/" + fmt::format("{:016X}_{:016X}.crm", tablet_id, txn_id);
}

StatusOr<std::string> lake_rows_mapper_filename(lake::TabletManager* mgr, int64_t tablet_id,
                                                const std::string& lcrm_file) {
    return mgr->lcrm_location(tablet_id, lcrm_file);
}

std::string local_rows_mapper_filename(Tablet* tablet, const std::string& rowset_id) {
    return tablet->data_dir()->get_tmp_path() + "/" + fmt::format("{:016X}_{}.crm", tablet->tablet_id(), rowset_id);
}

} // namespace starrocks