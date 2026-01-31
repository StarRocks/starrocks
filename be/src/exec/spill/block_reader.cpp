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

#include "base/string/slice.h"
#include "common/statusor.h"
#include "exec/spill/block_manager.h"
#include "fmt/format.h"
#include "io/input_stream.h"

namespace starrocks::spill {

// try to read `expected_length` bytes from file, return the actual length read or an error.
// if at_least_length is set and the actual length read is less than it, an error will be returned.
// if at_least_length is not set and the actual length read is not equal to expected_length, an error will be returned.
StatusOr<int64_t> try_to_read_from_file(io::InputStreamWrapper* readable, void* dst, int64_t expected_length,
                                        int64_t at_least_length = 0) {
    char* ptr = static_cast<char*>(dst);
    int64_t left = expected_length;
    int64_t total_read = 0;

    while (left > 0) {
        ASSIGN_OR_RETURN(auto read_len, readable->read(ptr, left));
        if (read_len == 0) {
            break;
        }
        ptr += read_len;
        total_read += read_len;
        left -= read_len;
    }

    RETURN_IF(total_read == 0, Status::EndOfFile("no more data to read"));
    if (at_least_length > 0) {
        RETURN_IF(total_read < at_least_length,
                  Status::InternalError(fmt::format("block's length is mismatched, actual[{}], at least[{}]",
                                                    total_read, at_least_length)));
    } else {
        RETURN_IF(total_read != expected_length,
                  Status::InternalError(fmt::format("block's length is mismatched, actual[{}], expected[{}]",
                                                    total_read, expected_length)));
    }
    return total_read;
}

Status BlockReader::read_fully(void* data, int64_t count) {
    if (_readable == nullptr) {
        ASSIGN_OR_RETURN(_readable, _block->get_readable());
        _length = _block->size();
        // init buffer
        if (_options.enable_buffer_read) {
            _options.max_buffer_bytes = std::min(_options.max_buffer_bytes, _length);
            _buffer = std::make_unique<uint8_t[]>(_options.max_buffer_bytes);
        }
    }

    if (_offset + count > _length) {
        return Status::EndOfFile("no more data in this block");
    }

    if (_options.enable_buffer_read) {
        int64_t length_in_buffer = _slice.size;
        if (length_in_buffer >= count) {
            // all data can be read from buffer
            std::memcpy(data, _slice.data, count);
            _slice.remove_prefix(count);
        } else {
            // read partial data from buffer first
            uint8_t* offset = reinterpret_cast<uint8_t*>(data);
            if (length_in_buffer > 0) {
                std::memcpy(offset, _slice.data, length_in_buffer);
                _slice.remove_prefix(length_in_buffer);
                offset += length_in_buffer;
            }
            int64_t length_need_read = count - length_in_buffer;
            if (length_need_read >= _options.max_buffer_bytes) {
                // if res length is larger than max_buffer_bytes, read from file directly
                SCOPED_TIMER(_options.read_io_timer);
                COUNTER_UPDATE(_options.read_io_count, 1);
                ASSIGN_OR_RETURN(auto read_len, try_to_read_from_file(_readable.get(), offset, length_need_read));
                _slice.clear();
                COUNTER_UPDATE(_options.read_io_bytes, read_len);
            } else {
                // refill buffer, then read res data from buffer
                SCOPED_TIMER(_options.read_io_timer);
                COUNTER_UPDATE(_options.read_io_count, 1);
                ASSIGN_OR_RETURN(auto read_len, try_to_read_from_file(_readable.get(), _buffer.get(),
                                                                      _options.max_buffer_bytes, length_need_read));
                _slice = Slice(_buffer.get(), read_len);
                std::memcpy(offset, _slice.data, length_need_read);
                _slice.remove_prefix(length_need_read);
                COUNTER_UPDATE(_options.read_io_bytes, read_len);
            }
        }
    } else {
        SCOPED_TIMER(_options.read_io_timer);
        COUNTER_UPDATE(_options.read_io_count, 1);
        ASSIGN_OR_RETURN(auto read_len, try_to_read_from_file(_readable.get(), data, count));
        COUNTER_UPDATE(_options.read_io_bytes, read_len);
    }
    _offset += count;
    return Status::OK();
}

} // namespace starrocks::spill