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

#include <limits>
#include <memory>

#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "io/async_flush_output_stream.h"
#include "io/formatted_output_stream.h"
#include "util/compression/stream_compressor.h"
#include "util/raw_container.h"

namespace starrocks::io {

class FormattedOutputStreamFile final : public FormattedOutputStream {
public:
    FormattedOutputStreamFile(std::unique_ptr<WritableFile> file, size_t buff_size)
            : FormattedOutputStream(buff_size), _file(std::move(file)) {}

    Status finalize() override {
        RETURN_IF_ERROR(FormattedOutputStream::finalize());
        return _file->close();
    }

    std::size_t size() override { return _file->size(); }

protected:
    Status _sync(const char* data, size_t size) override { return _file->append(Slice(data, size)); }

private:
    std::unique_ptr<WritableFile> _file;
};

class AsyncFormattedOutputStreamFile final : public FormattedOutputStream {
public:
    AsyncFormattedOutputStreamFile(io::AsyncFlushOutputStream* stream, size_t buff_size)
            : FormattedOutputStream(buff_size), _stream(stream) {}

    Status finalize() override {
        RETURN_IF_ERROR(FormattedOutputStream::finalize());
        return _stream->close();
    }

    std::size_t size() override { return _stream->tell(); }

protected:
    Status _sync(const char* data, size_t size) override {
        auto p = reinterpret_cast<const uint8_t*>(data);
        return _stream->write(p, size);
    }

private:
    io::AsyncFlushOutputStream* _stream;
};

// CompressedFormattedOutputStream is an abstract base class for compressed output streams.
// Use the factory method create() to get the appropriate implementation.
//
// Streaming compression support:
//   - GZIP, ZSTD, LZ4_FRAME, DEFLATE, ZLIB, BZIP2: Use streaming codecs and output
//     standard frames that common tools can decode.
//   - SNAPPY, LZ4 (raw): Use a block-stream wrapper compatible with the existing
//     StreamDecompressor decoders.
//
// Block stream format (for SNAPPY and LZ4 raw, big-endian):
//   Block 1: [uncompressed_len: 4B][compressed_len: 4B][compressed_data]
//   Block 2: [uncompressed_len: 4B][compressed_len: 4B][compressed_data]
//   ...
//   Terminator: [0x00000000: 4B]
class CompressedFormattedOutputStream : public FormattedOutputStream {
public:
    // Factory method to create CompressedFormattedOutputStream with proper error handling.
    // Returns StreamingCompressedFormattedOutputStream for streaming codecs,
    // or BlockCompressedFormattedOutputStream for block codecs.
    // Returns error status if compression codec initialization fails.
    // @param underlying_stream: The stream to write compressed data to (shared ownership via shared_ptr)
    // @param compression_type: The compression algorithm to use
    // @param buff_size: Buffer size for the base FormattedOutputStream
    static StatusOr<std::shared_ptr<CompressedFormattedOutputStream>> create(
            std::shared_ptr<FormattedOutputStream> underlying_stream, CompressionTypePB compression_type,
            size_t buff_size);

    ~CompressedFormattedOutputStream() override = default;

protected:
    CompressedFormattedOutputStream(std::shared_ptr<FormattedOutputStream> underlying_stream, size_t buff_size)
            : FormattedOutputStream(buff_size), _underlying_stream(std::move(underlying_stream)) {}

    std::shared_ptr<FormattedOutputStream> _underlying_stream;
    size_t _compressed_bytes_written = 0;
};

// StreamingCompressedFormattedOutputStream uses streaming compressors (GZIP, ZSTD, LZ4_FRAME, DEFLATE, ZLIB, BZIP2).
// Data is compressed incrementally and written to the underlying stream.
class StreamingCompressedFormattedOutputStream final : public CompressedFormattedOutputStream {
public:
    // NOTE: Do not use this constructor directly. Use CompressedFormattedOutputStream::create() instead.
    StreamingCompressedFormattedOutputStream(std::shared_ptr<FormattedOutputStream> underlying_stream,
                                             std::unique_ptr<StreamCompressor> compressor, size_t buff_size);
    ~StreamingCompressedFormattedOutputStream() override = default;

    Status finalize() override;
    std::size_t size() override;

protected:
    Status _sync(const char* data, size_t size) override;

private:
    Status _compress_and_write(const uint8_t* data, size_t size);

    std::unique_ptr<StreamCompressor> _compressor;
    raw::RawVector<uint8_t> _compress_buffer;
};

// BlockCompressedFormattedOutputStream uses block compressors (SNAPPY, LZ4).
// Data is buffered into blocks of fixed size, compressed, and written with block headers.
class BlockCompressedFormattedOutputStream final : public CompressedFormattedOutputStream {
public:
    // NOTE: Do not use this constructor directly. Use CompressedFormattedOutputStream::create() instead.
    BlockCompressedFormattedOutputStream(std::shared_ptr<FormattedOutputStream> underlying_stream,
                                         CompressionTypePB compression_type, size_t buff_size);
    ~BlockCompressedFormattedOutputStream() override = default;

    Status finalize() override;
    std::size_t size() override;

protected:
    Status _sync(const char* data, size_t size) override;

private:
    Status _flush_block();
    Status _write_block(uint32_t block_size, const uint8_t* compressed_data, uint32_t compressed_len);
    Status _write_block_end();
    static size_t _estimate_block_compressed_len(CompressionTypePB compression_type, size_t input_size);

    static constexpr size_t kBlockBufferSize = 1024 * 1024;
    static constexpr size_t kBlockHeaderSize = 8;
    static constexpr size_t kBlockEndSize = 4;
    // Ensure kBlockBufferSize fits in uint32_t for block header encoding
    static_assert(kBlockBufferSize <= std::numeric_limits<uint32_t>::max(), "kBlockBufferSize must fit in uint32_t");

    CompressionTypePB _compression_type;
    raw::RawVector<uint8_t> _compress_buffer;
    raw::RawVector<uint8_t> _block_buffer;
};

} // namespace starrocks::io
