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

#include "formats/csv/output_stream_file.h"

namespace starrocks::csv {

// Check if compression type supports incremental compression via frame concatenation.
// Only these formats can have multiple independent frames concatenated together.
static bool supports_incremental_compression(CompressionTypePB compression_type) {
    switch (compression_type) {
    case CompressionTypePB::GZIP:
        // Multiple GZIP streams can be concatenated; gunzip handles this correctly.
        return true;
    case CompressionTypePB::LZ4_FRAME:
        // LZ4 frame format supports concatenation of multiple frames.
        return true;
    case CompressionTypePB::ZSTD:
        // ZSTD supports concatenation of multiple frames.
        return true;
    default:
        // SNAPPY, LZ4 (raw block), and others don't support frame concatenation.
        return false;
    }
}

StatusOr<std::shared_ptr<CompressedOutputStream>> CompressedOutputStream::create(
        std::shared_ptr<OutputStream> underlying_stream, CompressionTypePB compression_type, size_t buff_size) {
    // Check if compression type supports incremental compression
    if (!supports_incremental_compression(compression_type)) {
        return Status::InvalidArgument(
                fmt::format("Compression type {} does not support incremental compression for CSV export. "
                            "Supported types: GZIP, LZ4_FRAME, ZSTD",
                            CompressionTypePB_Name(compression_type)));
    }

    const BlockCompressionCodec* codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
    if (codec == nullptr) {
        return Status::InvalidArgument(
                fmt::format("Failed to get compression codec for type: {}", CompressionTypePB_Name(compression_type)));
    }
    return std::shared_ptr<CompressedOutputStream>(
            new CompressedOutputStream(std::move(underlying_stream), codec, compression_type, buff_size));
}

CompressedOutputStream::CompressedOutputStream(std::shared_ptr<OutputStream> underlying_stream,
                                               const BlockCompressionCodec* codec, CompressionTypePB compression_type,
                                               size_t buff_size)
        : OutputStream(buff_size),
          _underlying_stream(std::move(underlying_stream)),
          _codec(codec),
          _compression_type(compression_type) {}

bool CompressedOutputStream::_supports_incremental_compression() const {
    return supports_incremental_compression(_compression_type);
}

Status CompressedOutputStream::_flush_compressed_chunk() {
    if (_uncompressed_buffer.empty()) {
        return Status::OK();
    }

    Slice input(reinterpret_cast<const char*>(_uncompressed_buffer.data()), _uncompressed_buffer.size());
    size_t max_compressed_size = _codec->max_compressed_len(_uncompressed_buffer.size());
    _compress_buffer.resize(max_compressed_size);
    Slice output(reinterpret_cast<char*>(_compress_buffer.data()), max_compressed_size);

    RETURN_IF_ERROR(_codec->compress(input, &output));

    // Write compressed data to underlying stream
    RETURN_IF_ERROR(_underlying_stream->write(Slice(output.data, output.size)));

    // Track compressed bytes written
    _compressed_bytes_written += output.size;

    // Clear the uncompressed buffer
    _uncompressed_buffer.clear();

    return Status::OK();
}

Status CompressedOutputStream::_sync(const char* data, size_t size) {
    if (size == 0) {
        return Status::OK();
    }

    // Buffer uncompressed data
    size_t old_size = _uncompressed_buffer.size();
    _uncompressed_buffer.resize(old_size + size);
    memcpy(_uncompressed_buffer.data() + old_size, data, size);

    // Flush when buffer reaches chunk size to limit memory usage.
    // Each flush creates an independent compressed frame that can be
    // concatenated with other frames.
    if (_uncompressed_buffer.size() >= kDefaultChunkSize) {
        RETURN_IF_ERROR(_flush_compressed_chunk());
    }

    return Status::OK();
}

Status CompressedOutputStream::finalize() {
    // First flush any remaining data in the base OutputStream buffer
    RETURN_IF_ERROR(OutputStream::finalize());

    // Compress and write any remaining buffered data
    RETURN_IF_ERROR(_flush_compressed_chunk());

    // Shrink buffer to free memory
    _uncompressed_buffer.shrink_to_fit();

    // Finalize the underlying stream
    return _underlying_stream->finalize();
}

std::size_t CompressedOutputStream::size() {
    // Return the total compressed bytes written so far, plus an estimate for pending
    // uncompressed data in the buffer. This provides a reasonable approximation of
    // the final file size for file rotation decisions.
    //
    // For pending data, we use the max_compressed_len as a conservative estimate.
    // This may slightly overestimate, but ensures file rotation triggers before
    // files get too large.
    size_t pending_estimate = 0;
    if (!_uncompressed_buffer.empty()) {
        pending_estimate = _codec->max_compressed_len(_uncompressed_buffer.size());
    }
    return _compressed_bytes_written + pending_estimate;
}

} // namespace starrocks::csv
