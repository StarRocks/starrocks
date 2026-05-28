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
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "lake/filenames.h"
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

Status RowsMapperIterator::next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids) {
    if (fetch_cnt == 0) {
        // No need to fetch
        return Status::OK();
    }
    if (_pos + fetch_cnt > _row_count) {
        return Status::EndOfFile(fmt::format("RowsMapperIterator end of file. position/fetch cnt/row count: {}/{}/{}",
                                             _pos, fetch_cnt, _row_count));
    }
    rssid_rowids->resize(fetch_cnt);

    // Read-ahead path: serve from `_buf` whenever the requested slice fits inside
    // the currently-buffered window [_buf_pos, _buf_pos + _buf.size()/EACH_ROW_SIZE).
    // For huge single fetches that exceed the buffer's capacity, fall through to
    // the original direct read_at_fully so behaviour stays correct under any tuning.
    const size_t need_bytes = fetch_cnt * EACH_ROW_SIZE;
    if (need_bytes > _buf.size() && need_bytes > static_cast<size_t>(config::lake_rows_mapper_read_buf_bytes)) {
        // Caller requested more than one buffer's worth in a single call — bypass
        // the buffer to avoid serving a partial slice. Rare in practice (per-segment
        // fetches are bounded by segment row count).
        RETURN_IF_ERROR(_rfile->read_at_fully(_pos * EACH_ROW_SIZE, rssid_rowids->data(), need_bytes));
    } else {
        // Check whether the requested range lies fully inside the current buffer;
        // refill if not (also covers the first call where _buf is empty).
        const uint64_t buf_end_row = _buf_pos + _buf.size() / EACH_ROW_SIZE;
        if (_buf.empty() || _pos < _buf_pos || _pos + fetch_cnt > buf_end_row) {
            RETURN_IF_ERROR(_refill_buf());
        }
        const size_t off_in_buf = (_pos - _buf_pos) * EACH_ROW_SIZE;
        memcpy(rssid_rowids->data(), _buf.data() + off_in_buf, need_bytes);
    }

    _current_checksum = crc32c::Extend(_current_checksum, (const char*)rssid_rowids->data(), rssid_rowids->size() * 8);
    _pos += fetch_cnt;
    return Status::OK();
}

Status RowsMapperIterator::_refill_buf() {
    // Refill aligned at `_pos`. Reading from `_pos` rather than the previous buffer's
    // end is harmless given the sequential access pattern (_pos only ever advances)
    // and is robust if the caller ever skips a range.
    const uint64_t remaining_rows = _row_count - _pos;
    const uint64_t buf_rows =
            std::min(remaining_rows, static_cast<uint64_t>(config::lake_rows_mapper_read_buf_bytes) / EACH_ROW_SIZE);
    if (buf_rows == 0) {
        _buf.clear();
        _buf_pos = _pos;
        return Status::OK();
    }
    const size_t read_bytes = buf_rows * EACH_ROW_SIZE;
    _buf.resize(read_bytes);
    RETURN_IF_ERROR(_rfile->read_at_fully(_pos * EACH_ROW_SIZE, _buf.data(), read_bytes));
    _buf_pos = _pos;
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