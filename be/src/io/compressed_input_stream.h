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

#include <utility>

#include "common/status.h"
#include "io/input_stream.h"
#include "io/seekable_input_stream.h"
#include "util/bit_util.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {
class StreamCompression;
} // namespace starrocks

namespace starrocks::io {

class CompressedInputStream final : public InputStream {
public:
    CompressedInputStream(std::shared_ptr<InputStream> source_stream, std::shared_ptr<StreamCompression> decompressor,
                          size_t compressed_data_cache_size = 8 * 1024 * 1024LU)
            : _source_stream(std::move(source_stream)),
              _decompressor(std::move(decompressor)),
              _compressed_buff(BitUtil::round_up(compressed_data_cache_size, CACHELINE_SIZE)) {}

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status skip(int64_t n) override;

    // TODO: add custom statistics
    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _source_stream->get_numeric_statistics();
    }

private:
    // Used to store the compressed data read from |_source_stream|.
    class CompressedBuffer {
    public:
        explicit CompressedBuffer(size_t buff_size) : _compressed_data(BitUtil::round_up(buff_size, CACHELINE_SIZE)) {}

        Slice read_buffer() const { return {&_compressed_data[_offset], _limit - _offset}; }

        Slice write_buffer() const { return {&_compressed_data[_limit], _compressed_data.size() - _limit}; }

        void skip(size_t n) {
            _offset += n;
            assert(_offset <= _limit);
        }

        Status read(InputStream* f) {
            if (_offset > 0) {
                // Copy the bytes between the buffer's current offset and limit to the beginning of
                // the buffer.
                memmove(&_compressed_data[0], &_compressed_data[_offset], available());
                _limit -= _offset;
                _offset = 0;
            }
            if (_limit >= _compressed_data.size()) {
                return Status::InternalError("reached the buffer limit");
            }
            Slice buff(write_buffer());
            ASSIGN_OR_RETURN(buff.size, f->read(buff.data, buff.size));
            if (buff.size == 0) return Status::EndOfFile("");
            _limit += buff.size;
            return Status::OK();
        }

        size_t available() const { return _limit - _offset; }

    private:
        raw::RawVector<uint8_t> _compressed_data;
        size_t _offset{0};
        size_t _limit{0};
    };

    std::shared_ptr<InputStream> _source_stream;
    std::shared_ptr<StreamCompression> _decompressor;
    CompressedBuffer _compressed_buff;
    bool _stream_end = false;
};

class CompressedSeekableInputStream final : public SeekableInputStream {
public:
    CompressedSeekableInputStream(std::shared_ptr<CompressedInputStream> source)
            : _source(std::move(std::move(source))) {}

    StatusOr<int64_t> read(void* data, int64_t size) override { return _source->read(data, size); }

    Status skip(int64_t n) override { return _source->skip(n); }

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _source->get_numeric_statistics();
    }

    Status seek(int64_t position) override { return Status::NotSupported(""); }
    StatusOr<int64_t> position() override { return Status::NotSupported(""); }
    StatusOr<int64_t> get_size() override { return Status::NotSupported(""); }

private:
    std::shared_ptr<CompressedInputStream> _source;
};

} // namespace starrocks::io
