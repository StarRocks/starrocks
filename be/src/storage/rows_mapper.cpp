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

#include "fs/fs.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/raw_container.h"

namespace starrocks {

Status RowsMapperBuilder::_init() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_filename));
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

RowsMapperIterator::~RowsMapperIterator() {
    if (_rfile != nullptr) {
        const std::string filename = _rfile->filename();
        _rfile.reset(nullptr);
        auto st = fs::delete_file(filename);
        if (!st.ok()) {
            LOG(ERROR) << "delete rows mapper file fail, st: " << st;
        }
    }
}

// Open file
Status RowsMapperIterator::open(const std::string& filename) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
    ASSIGN_OR_RETURN(_rfile, fs->new_random_access_file(filename));
    ASSIGN_OR_RETURN(int64_t file_size, _rfile->get_size());
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
    RETURN_IF_ERROR(_rfile->read_at_fully(_pos * EACH_ROW_SIZE, rssid_rowids->data(), fetch_cnt * EACH_ROW_SIZE));
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

StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id) {
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    if (data_dir == nullptr) {
        return Status::NotFound(fmt::format("Not local disk found. tablet id: {}", tablet_id));
    }
    return data_dir->get_tmp_path() + "/" + fmt::format("{:016X}_{:016X}.crm", tablet_id, txn_id);
}

StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id, int32_t subtask_id) {
    auto data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
    if (data_dir == nullptr) {
        return Status::NotFound(fmt::format("Not local disk found. tablet id: {}", tablet_id));
    }
    return data_dir->get_tmp_path() + "/" + fmt::format("{:016X}_{:016X}_{}.crm", tablet_id, txn_id, subtask_id);
}

StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id) {
    ASSIGN_OR_RETURN(auto filename, lake_rows_mapper_filename(tablet_id, txn_id));
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
    ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file(filename));
    ASSIGN_OR_RETURN(int64_t file_size, rfile->get_size());
    if (file_size < 12) {
        return Status::Corruption(fmt::format("RowsMapper file too small. file size: {}", file_size));
    }
    // Read row count from file (8 bytes before the last 4 bytes checksum)
    std::string row_count_str;
    raw::stl_string_resize_uninitialized(&row_count_str, 8);
    RETURN_IF_ERROR(rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
    return decode_fixed64_le((const uint8_t*)row_count_str.data());
}

StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id, int32_t subtask_count) {
    if (subtask_count <= 0) {
        return Status::InvalidArgument("subtask_count must be positive");
    }

    uint64_t total_row_count = 0;
    bool any_file_exists = false;

    for (int32_t subtask_id = 0; subtask_id < subtask_count; subtask_id++) {
        ASSIGN_OR_RETURN(auto filename, lake_rows_mapper_filename(tablet_id, txn_id, subtask_id));

        if (!fs::path_exist(filename)) {
            // Subtask file doesn't exist, skip (subtask may have produced empty output)
            continue;
        }

        any_file_exists = true;

        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file(filename));
        ASSIGN_OR_RETURN(int64_t file_size, rfile->get_size());

        if (file_size < 12) {
            return Status::Corruption(
                    fmt::format("RowsMapper file too small. file: {}, size: {}", filename, file_size));
        }

        // Read row count from file (8 bytes before the last 4 bytes checksum)
        std::string row_count_str;
        raw::stl_string_resize_uninitialized(&row_count_str, 8);
        RETURN_IF_ERROR(rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
        total_row_count += decode_fixed64_le((const uint8_t*)row_count_str.data());
    }

    if (!any_file_exists) {
        return Status::NotFound(fmt::format("No rows mapper files found for tablet {} txn {} subtask_count {}",
                                            tablet_id, txn_id, subtask_count));
    }

    return total_row_count;
}

StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id,
                                              const std::vector<int32_t>& success_subtask_ids) {
    if (success_subtask_ids.empty()) {
        return Status::InvalidArgument("success_subtask_ids must not be empty");
    }

    uint64_t total_row_count = 0;
    bool any_file_exists = false;

    for (int32_t subtask_id : success_subtask_ids) {
        ASSIGN_OR_RETURN(auto filename, lake_rows_mapper_filename(tablet_id, txn_id, subtask_id));

        if (!fs::path_exist(filename)) {
            // Subtask file doesn't exist, skip (subtask may have produced empty output)
            continue;
        }

        any_file_exists = true;

        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        ASSIGN_OR_RETURN(auto rfile, fs->new_random_access_file(filename));
        ASSIGN_OR_RETURN(int64_t file_size, rfile->get_size());

        if (file_size < 12) {
            return Status::Corruption(
                    fmt::format("RowsMapper file too small. file: {}, size: {}", filename, file_size));
        }

        // Read row count from file (8 bytes before the last 4 bytes checksum)
        std::string row_count_str;
        raw::stl_string_resize_uninitialized(&row_count_str, 8);
        RETURN_IF_ERROR(rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
        total_row_count += decode_fixed64_le((const uint8_t*)row_count_str.data());
    }

    if (!any_file_exists) {
        return Status::NotFound(
                fmt::format("No rows mapper files found for tablet {} txn {} success_subtask_ids", tablet_id, txn_id));
    }

    return total_row_count;
}

MultiRowsMapperIterator::~MultiRowsMapperIterator() {
    // Unlike RowsMapperIterator, we do NOT delete files in destructor.
    // For parallel compaction, multiple subtask files are managed separately:
    // - Files are created by each parallel compaction subtask
    // - Files are read by light_publish via this iterator
    // - Files should be cleaned up explicitly after light_publish completes successfully
    //   or when the tablet parallel compaction state is cleaned up
    // This allows for retry scenarios where files need to persist across attempts.
    if (_delete_files_on_close) {
        for (auto& file : _files) {
            if (file.rfile != nullptr) {
                const std::string filename = file.rfile->filename();
                file.rfile.reset(nullptr);
                auto st = fs::delete_file(filename);
                if (!st.ok()) {
                    LOG(ERROR) << "delete rows mapper file fail, st: " << st;
                }
            }
        }
    }
}

Status MultiRowsMapperIterator::open(int64_t tablet_id, int64_t txn_id, int32_t subtask_count) {
    if (subtask_count <= 0) {
        return Status::InvalidArgument("subtask_count must be positive");
    }

    _files.clear();
    _total_row_count = 0;
    _current_file_idx = 0;

    // Open files in subtask_id order (0, 1, 2, ...) to match segment order
    for (int32_t subtask_id = 0; subtask_id < subtask_count; subtask_id++) {
        ASSIGN_OR_RETURN(auto filename, lake_rows_mapper_filename(tablet_id, txn_id, subtask_id));

        if (!fs::path_exist(filename)) {
            // Subtask file doesn't exist, skip (subtask may have produced empty output)
            continue;
        }

        FileInfo info;
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        ASSIGN_OR_RETURN(info.rfile, fs->new_random_access_file(filename));
        ASSIGN_OR_RETURN(int64_t file_size, info.rfile->get_size());

        if (file_size < 12) {
            return Status::Corruption(
                    fmt::format("RowsMapper file too small. file: {}, size: {}", filename, file_size));
        }

        // Read checksum
        std::string checksum_str;
        raw::stl_string_resize_uninitialized(&checksum_str, 4);
        RETURN_IF_ERROR(info.rfile->read_at_fully(file_size - 4, checksum_str.data(), checksum_str.size()));
        info.expected_checksum = decode_fixed32_le((const uint8_t*)checksum_str.data());

        // Read row count
        std::string row_count_str;
        raw::stl_string_resize_uninitialized(&row_count_str, 8);
        RETURN_IF_ERROR(info.rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
        info.row_count = decode_fixed64_le((const uint8_t*)row_count_str.data());

        // Verify file size
        if (file_size != 12 + info.row_count * EACH_ROW_SIZE) {
            return Status::Corruption(fmt::format("RowsMapper file corruption. file: {}, size: {}, row_count: {}",
                                                  filename, file_size, info.row_count));
        }

        _total_row_count += info.row_count;
        _files.push_back(std::move(info));
    }

    if (_files.empty()) {
        return Status::NotFound(fmt::format("No rows mapper files found for tablet {} txn {} subtask_count {}",
                                            tablet_id, txn_id, subtask_count));
    }

    return Status::OK();
}

Status MultiRowsMapperIterator::open(int64_t tablet_id, int64_t txn_id,
                                     const std::vector<int32_t>& success_subtask_ids) {
    if (success_subtask_ids.empty()) {
        return Status::InvalidArgument("success_subtask_ids must not be empty");
    }

    _files.clear();
    _total_row_count = 0;
    _current_file_idx = 0;

    // Open files in the order specified by success_subtask_ids
    for (int32_t subtask_id : success_subtask_ids) {
        ASSIGN_OR_RETURN(auto filename, lake_rows_mapper_filename(tablet_id, txn_id, subtask_id));

        if (!fs::path_exist(filename)) {
            // Subtask file doesn't exist, skip (subtask may have produced empty output)
            continue;
        }

        FileInfo info;
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(filename));
        ASSIGN_OR_RETURN(info.rfile, fs->new_random_access_file(filename));
        ASSIGN_OR_RETURN(int64_t file_size, info.rfile->get_size());

        if (file_size < 12) {
            return Status::Corruption(
                    fmt::format("RowsMapper file too small. file: {}, size: {}", filename, file_size));
        }

        // Read checksum
        std::string checksum_str;
        raw::stl_string_resize_uninitialized(&checksum_str, 4);
        RETURN_IF_ERROR(info.rfile->read_at_fully(file_size - 4, checksum_str.data(), checksum_str.size()));
        info.expected_checksum = decode_fixed32_le((const uint8_t*)checksum_str.data());

        // Read row count
        std::string row_count_str;
        raw::stl_string_resize_uninitialized(&row_count_str, 8);
        RETURN_IF_ERROR(info.rfile->read_at_fully(file_size - 12, row_count_str.data(), row_count_str.size()));
        info.row_count = decode_fixed64_le((const uint8_t*)row_count_str.data());

        // Verify file size
        if (file_size != 12 + info.row_count * EACH_ROW_SIZE) {
            return Status::Corruption(fmt::format("RowsMapper file corruption. file: {}, size: {}, row_count: {}",
                                                  filename, file_size, info.row_count));
        }

        _total_row_count += info.row_count;
        _files.push_back(std::move(info));
    }

    if (_files.empty()) {
        return Status::NotFound(
                fmt::format("No rows mapper files found for tablet {} txn {} success_subtask_ids", tablet_id, txn_id));
    }

    return Status::OK();
}

Status MultiRowsMapperIterator::next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids) {
    if (fetch_cnt == 0) {
        return Status::OK();
    }

    rssid_rowids->clear();
    rssid_rowids->reserve(fetch_cnt);

    size_t remaining = fetch_cnt;
    while (remaining > 0 && _current_file_idx < _files.size()) {
        auto& file = _files[_current_file_idx];
        size_t available = file.row_count - file.pos;
        size_t to_read = std::min(remaining, available);

        if (to_read > 0) {
            std::vector<uint64_t> batch(to_read);
            RETURN_IF_ERROR(file.rfile->read_at_fully(file.pos * EACH_ROW_SIZE, batch.data(), to_read * EACH_ROW_SIZE));
            file.current_checksum =
                    crc32c::Extend(file.current_checksum, (const char*)batch.data(), to_read * EACH_ROW_SIZE);
            file.pos += to_read;

            rssid_rowids->insert(rssid_rowids->end(), batch.begin(), batch.end());
            remaining -= to_read;
        }

        // Move to next file when current file is exhausted
        if (file.pos >= file.row_count) {
            _current_file_idx++;
        }
    }

    if (remaining > 0) {
        return Status::EndOfFile(fmt::format("MultiRowsMapperIterator: not enough rows, requested {}, got {}",
                                             fetch_cnt, fetch_cnt - remaining));
    }

    return Status::OK();
}

Status MultiRowsMapperIterator::status() {
    // Verify all files have been fully read and checksums match
    for (size_t i = 0; i < _files.size(); i++) {
        auto& file = _files[i];
        if (file.pos != file.row_count) {
            return Status::Corruption(
                    fmt::format("MultiRowsMapperIterator: file {} not fully read. pos: {}, row_count: {}",
                                file.rfile->filename(), file.pos, file.row_count));
        }

        // Compute checksum for row count
        std::string row_count_str;
        put_fixed64_le(&row_count_str, file.row_count);
        file.current_checksum = crc32c::Extend(file.current_checksum, row_count_str.data(), row_count_str.size());

        if (file.expected_checksum != file.current_checksum) {
            return Status::Corruption(
                    fmt::format("MultiRowsMapperIterator: checksum mismatch for file {}. expected: {}, actual: {}",
                                file.rfile->filename(), file.expected_checksum, file.current_checksum));
        }
    }

    return Status::OK();
}

void delete_lake_rows_mapper_files(int64_t tablet_id, int64_t txn_id, int32_t subtask_count) {
    if (subtask_count <= 0) {
        return;
    }

    for (int32_t subtask_id = 0; subtask_id < subtask_count; subtask_id++) {
        auto filename_st = lake_rows_mapper_filename(tablet_id, txn_id, subtask_id);
        if (!filename_st.ok()) {
            continue;
        }
        const std::string& filename = filename_st.value();
        if (fs::path_exist(filename)) {
            auto st = fs::delete_file(filename);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to delete rows mapper file: " << filename << ", error: " << st;
            }
        }
    }
}

std::string local_rows_mapper_filename(Tablet* tablet, const std::string& rowset_id) {
    return tablet->data_dir()->get_tmp_path() + "/" + fmt::format("{:016X}_{}.crm", tablet->tablet_id(), rowset_id);
}

} // namespace starrocks