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

#include <atomic>

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
                                               const std::unordered_map<uint32_t, uint32_t>* column_unique_id_map)
        : FileStreamConverter(input_file_name, input_file_size, std::move(output_file)),
          _column_unique_id_map(column_unique_id_map) {
    if (_column_unique_id_map != nullptr && !_column_unique_id_map->empty()) {
        _segment_footer_buffer.reserve(s_segment_footer_buffer_size.load(std::memory_order::acquire));
    }
}

Status SegmentStreamConverter::append(const void* data, size_t size) {
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

    ReplicationUtils::convert_column_unique_ids(segment_footer_pb.mutable_columns(), *_column_unique_id_map);

    RETURN_IF_ERROR(Segment::write_segment_footer(_output_file.get(), segment_footer_pb));

    return _output_file->close();
}

} // namespace starrocks
