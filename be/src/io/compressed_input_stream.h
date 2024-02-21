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
              _compressed_buff(compressed_data_cache_size) {}

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
        static constexpr size_t MAX_BLOCK_HEADER_SIZE = 64;

        explicit CompressedBuffer(size_t buff_size) : _compressed_data(aligned_size(buff_size)) {}

        size_t aligned_size(size_t size) const { return BitUtil::round_up(size, CACHELINE_SIZE); }

        Slice read_buffer() const { return {&_compressed_data[_offset], _limit - _offset}; }

        Slice write_buffer() const { return {&_compressed_data[_limit], _compressed_data.size() - _limit}; }

        size_t available() const { return _limit - _offset; }

        void skip(size_t n) {
            _offset += n;
            assert(_offset <= _limit);
        }

        Status read_with_hint_size(InputStream* f, size_t hint_size);

    private:
        raw::RawVector<uint8_t> _compressed_data;
        size_t _offset{0};
        size_t _limit{0};
        bool _eof = false;
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
