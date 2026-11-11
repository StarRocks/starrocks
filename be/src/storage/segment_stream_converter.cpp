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

#include "storage/segment_stream_converter.h"

#include <algorithm>
#include <atomic>
#include <unordered_set>

#include "fs/fs_memory.h"
#include "replication_utils.h"
#include "storage/rowset/segment.h"

namespace starrocks {

#ifndef BE_TEST
static
#endif
        std::atomic<size_t>
                s_segment_footer_buffer_size = 4 * 1024 * 1024; // 4MB

SegmentStreamConverter::SegmentStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                                               std::unique_ptr<WritableFile> output_file,
                                               std::unordered_map<uint32_t, uint32_t>* column_unique_id_map,
                                               bool strict_column_uid_map,
                                               const std::unordered_set<uint32_t>* dropped_column_uids)
        : FileStreamConverter(input_file_name, input_file_size, std::move(output_file)),
          _column_unique_id_map(column_unique_id_map),
          _strict_column_uid_map(strict_column_uid_map),
          _dropped_column_uids(dropped_column_uids) {
    if (_column_unique_id_map != nullptr && !_column_unique_id_map->empty()) {
        _segment_footer_buffer.reserve(s_segment_footer_buffer_size.load(std::memory_order::acquire));
    }
}

Status SegmentStreamConverter::append(const void* data, size_t size) {
    if (_strict_column_uid_map && (_column_unique_id_map == nullptr || _column_unique_id_map->empty())) {
        return Status::Corruption("Missing strict lake replication column UID map");
    }
    if (_column_unique_id_map == nullptr || _column_unique_id_map->empty()) {
        return FileStreamConverter::append(data, size);
    }

    uint64_t remaining_file_size = _input_file_size - _output_file->size();
    if (size > remaining_file_size) {
        LOG(WARNING) << "Append size is larger than remaining file size, append size: " << size
                     << ", input_file_size: " << _input_file_size << ", output_file_size: " << _output_file->size()
                     << ", segment_footer_buffer_size: " << _segment_footer_buffer.size()
                     << ", segment_footer_buffer_capacity: " << _segment_footer_buffer.capacity();
        return Status::Corruption("Append size is larger than remaining file size");
    }

    // remaining file size can put in buffer
    if (remaining_file_size <= _segment_footer_buffer.capacity()) {
        if (size > _segment_footer_buffer.capacity() - _segment_footer_buffer.size()) {
            LOG(WARNING) << "Append size is large than free buffer size, append size: " << size
                         << ", input_file_size: " << _input_file_size << ", output_file_size: " << _output_file->size()
                         << ", segment_footer_buffer_size: " << _segment_footer_buffer.size()
                         << ", segment_footer_buffer_capacity: " << _segment_footer_buffer.capacity();
            return Status::Corruption("Append size is large than free buffer size");
        }
        _segment_footer_buffer.append((const char*)data, size);
        return Status::OK();
    }

    uint64_t remaining_stream_size = remaining_file_size - size;
    if (remaining_stream_size >= _segment_footer_buffer.capacity()) {
        return _output_file->append(Slice((const char*)data, size));
    }

    uint64_t to_append_output_file_size = remaining_file_size - _segment_footer_buffer.capacity();
    RETURN_IF_ERROR(_output_file->append(Slice((const char*)data, to_append_output_file_size)));

    _segment_footer_buffer.append((const char*)data + to_append_output_file_size, size - to_append_output_file_size);

    return Status::OK();
}

Status SegmentStreamConverter::close() {
    if (_strict_column_uid_map && (_column_unique_id_map == nullptr || _column_unique_id_map->empty())) {
        return Status::Corruption("Missing strict lake replication column UID map");
    }
    if (_column_unique_id_map == nullptr || _column_unique_id_map->empty()) {
        return FileStreamConverter::close();
    }

    if (_output_file->size() + _segment_footer_buffer.size() != _input_file_size) {
        LOG(WARNING) << "File size not matched, input_file_size: " << _input_file_size
                     << ", output_file_size: " << _output_file->size()
                     << ", segment_footer_buffer_size: " << _segment_footer_buffer.size()
                     << ", segment_footer_buffer_capacity: " << _segment_footer_buffer.capacity();
        return Status::Corruption("File size not matched");
    }

    auto memory_file = new_random_access_file_from_memory(_input_file_name, _segment_footer_buffer);
    SegmentFooterPB segment_footer_pb;
    size_t footer_length_hint = _segment_footer_buffer.size();
    auto status_or = Segment::parse_segment_footer(memory_file.get(), &segment_footer_pb, &footer_length_hint, nullptr);
    if (!status_or.ok()) {
        if (footer_length_hint > _segment_footer_buffer.size()) {
            s_segment_footer_buffer_size.store(footer_length_hint, std::memory_order::release);
        }
        return status_or.status();
    }

    auto segment_footer_size = status_or.value();
    if (segment_footer_size < _segment_footer_buffer.size()) {
        RETURN_IF_ERROR(_output_file->append(
                Slice(_segment_footer_buffer.data(), _segment_footer_buffer.size() - segment_footer_size)));
    }

    if (_strict_column_uid_map) {
        std::unordered_set<uint32_t> seen;
        for (const auto& column : segment_footer_pb.columns()) {
            if (!column.has_unique_id() || !seen.insert(column.unique_id()).second) {
                return Status::Corruption("Missing/duplicate strict lake replication segment column UID");
            }
            if (!_column_unique_id_map->contains(column.unique_id()) &&
                (_dropped_column_uids == nullptr || !_dropped_column_uids->contains(column.unique_id()))) {
                return Status::Corruption("LAKE_REPLICATION_UNKNOWN_SEGMENT_COLUMN_UID: " +
                                          std::to_string(column.unique_id()));
            }
        }
        if (_dropped_column_uids != nullptr) {
            // Validate the whole footer before removing anything. Data and index pages
            // keep their absolute offsets; only the validated dropped columns become
            // unreachable. Preserve the remaining column order and column_id fields.
            auto* columns = segment_footer_pb.mutable_columns();
            columns->erase(std::remove_if(columns->begin(), columns->end(),
                                          [this](const auto& column) {
                                              return _dropped_column_uids->contains(column.unique_id());
                                          }),
                           columns->end());
        }
    }
    ReplicationUtils::convert_column_unique_ids(segment_footer_pb.mutable_columns(), _column_unique_id_map);

    RETURN_IF_ERROR(Segment::write_segment_footer(_output_file.get(), segment_footer_pb));

    // Record the final output file size (may differ from input size due to footer changes)
    _final_output_file_size = _output_file->size();

    LOG(INFO) << "Segment conversion completed, input_file: " << _input_file_name
              << ", input_size: " << _input_file_size << ", output_size: " << _final_output_file_size;

    return _output_file->close();
}

} // namespace starrocks
