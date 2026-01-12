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

#include <memory>

#include "formats/csv/output_stream.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "io/async_flush_output_stream.h"
#include "util/compression/block_compression.h"
#include "util/raw_container.h"

namespace starrocks::csv {

class OutputStreamFile final : public OutputStream {
public:
    OutputStreamFile(std::unique_ptr<WritableFile> file, size_t buff_size)
            : OutputStream(buff_size), _file(std::move(file)) {}

    Status finalize() override {
        RETURN_IF_ERROR(OutputStream::finalize());
        return _file->close();
    }

    std::size_t size() override { return _file->size(); }

protected:
    Status _sync(const char* data, size_t size) override { return _file->append(Slice(data, size)); }

private:
    std::unique_ptr<WritableFile> _file;
};

class AsyncOutputStreamFile final : public OutputStream {
public:
    AsyncOutputStreamFile(io::AsyncFlushOutputStream* stream, size_t buff_size)
            : OutputStream(buff_size), _stream(stream) {}

    Status finalize() override {
        RETURN_IF_ERROR(OutputStream::finalize());
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

// CompressedOutputStream wraps any OutputStream and adds compression.
// This design follows the decorator pattern, allowing flexible composition:
//   data -> AsyncOutputStreamFile -> CompressedOutputStream
// or any other OutputStream implementation.
//
// Incremental compression support:
//   - GZIP, LZ4_FRAME, ZSTD: Support incremental compression via frame concatenation.
//     Data is compressed and written in chunks (default 64MB) to limit memory usage.
//     Multiple compressed frames can be concatenated to form a valid file that
//     standard decompression tools can handle.
//   - SNAPPY, LZ4 (raw): Do NOT support incremental compression. Creating a
//     CompressedOutputStream with these types will return an error.
class CompressedOutputStream final : public OutputStream {
public:
    // Default chunk size for incremental compression (64MB).
    // When the uncompressed buffer reaches this size, it will be compressed
    // and written as a separate frame to the underlying stream.
    static constexpr size_t kDefaultChunkSize = 64 * 1024 * 1024;

    // Factory method to create CompressedOutputStream with proper error handling.
    // Returns error status if compression codec initialization fails.
    // @param underlying_stream: The stream to write compressed data to (not owned, must outlive this object)
    // @param compression_type: The compression algorithm to use
    // @param buff_size: Buffer size for the base OutputStream
    static StatusOr<std::shared_ptr<CompressedOutputStream>> create(std::shared_ptr<OutputStream> underlying_stream,
                                                                    CompressionTypePB compression_type,
                                                                    size_t buff_size);

    ~CompressedOutputStream() override = default;

    Status finalize() override;
    std::size_t size() override;

protected:
    Status _sync(const char* data, size_t size) override;

private:
    // Private constructor - use create() factory method instead
    CompressedOutputStream(std::shared_ptr<OutputStream> underlying_stream, const BlockCompressionCodec* codec,
                           CompressionTypePB compression_type, size_t buff_size);

    // Compress and write the current buffer to underlying stream
    Status _flush_compressed_chunk();

    // Check if the compression type supports incremental compression via frame concatenation.
    // GZIP, LZ4_FRAME, and ZSTD support this because multiple frames can be concatenated.
    bool _supports_incremental_compression() const;

    std::shared_ptr<OutputStream> _underlying_stream;
    const BlockCompressionCodec* _codec;
    CompressionTypePB _compression_type;
    // Buffer to accumulate uncompressed data before compression
    raw::RawVector<uint8_t> _uncompressed_buffer;
    raw::RawVector<uint8_t> _compress_buffer;
    // Track the total compressed bytes written to underlying stream
    size_t _compressed_bytes_written = 0;
};

} // namespace starrocks::csv
