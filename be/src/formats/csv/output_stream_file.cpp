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

#include <algorithm>
#include <cstring>

#include "fmt/format.h"
#include "util/coding.h"
#include "util/compression/compression_headers.h"

namespace starrocks::csv {

static bool is_block_compression(CompressionTypePB compression_type) {
    return compression_type == CompressionTypePB::SNAPPY || compression_type == CompressionTypePB::LZ4;
}

StatusOr<std::shared_ptr<CompressedOutputStream>> CompressedOutputStream::create(
        std::shared_ptr<OutputStream> underlying_stream, CompressionTypePB compression_type, size_t buff_size) {
    if (compression_type == CompressionTypePB::NO_COMPRESSION ||
        compression_type == CompressionTypePB::UNKNOWN_COMPRESSION) {
        return Status::InvalidArgument(
                fmt::format("Invalid compression type: {}", CompressionTypePB_Name(compression_type)));
    }

    std::unique_ptr<StreamCompressor> compressor;
    if (!is_block_compression(compression_type)) {
        RETURN_IF_ERROR(StreamCompressor::create_compressor(compression_type, &compressor));
    }
    return std::shared_ptr<CompressedOutputStream>(new CompressedOutputStream(
            std::move(underlying_stream), compression_type, std::move(compressor), buff_size));
}

CompressedOutputStream::CompressedOutputStream(std::shared_ptr<OutputStream> underlying_stream,
                                               CompressionTypePB compression_type,
                                               std::unique_ptr<StreamCompressor> compressor, size_t buff_size)
        : OutputStream(buff_size),
          _underlying_stream(std::move(underlying_stream)),
          _compression_type(compression_type),
          _is_block_compression(is_block_compression(compression_type)),
          _compressor(std::move(compressor)) {
    _compress_buffer.resize(64 * 1024);
    if (_is_block_compression) {
        _block_buffer.reserve(kBlockBufferSize);
    }
}

Status CompressedOutputStream::_compress_and_write(const uint8_t* data, size_t size) {
    size_t remaining = size;
    while (remaining > 0) {
        size_t needed = _compressor->max_compressed_len(remaining);
        if (needed > _compress_buffer.size()) {
            _compress_buffer.resize(needed);
        }
        size_t input_read = 0;
        size_t output_written = 0;
        RETURN_IF_ERROR(_compressor->compress(data, remaining, &input_read, _compress_buffer.data(),
                                              _compress_buffer.size(), &output_written));
        if (output_written > 0) {
            RETURN_IF_ERROR(_underlying_stream->write(
                    Slice(reinterpret_cast<const char*>(_compress_buffer.data()), output_written)));
            _compressed_bytes_written += output_written;
        }
        if (input_read == 0 && output_written == 0) {
            return Status::InternalError("Stream compression made no progress");
        }
        data += input_read;
        remaining -= input_read;
    }
    return Status::OK();
}

Status CompressedOutputStream::_flush_block() {
    if (_block_buffer.empty()) {
        return Status::OK();
    }

    uint32_t block_size = _block_buffer.size();
    if (_compression_type == CompressionTypePB::SNAPPY) {
        size_t max_compressed = snappy::MaxCompressedLength(block_size);
        _compress_buffer.resize(max_compressed);
        size_t compressed_len = 0;
        snappy::RawCompress(reinterpret_cast<const char*>(_block_buffer.data()), block_size,
                            reinterpret_cast<char*>(_compress_buffer.data()), &compressed_len);
        RETURN_IF_ERROR(_write_block(block_size, _compress_buffer.data(), static_cast<uint32_t>(compressed_len)));
    } else if (_compression_type == CompressionTypePB::LZ4) {
        size_t max_compressed = LZ4_compressBound(block_size);
        _compress_buffer.resize(max_compressed);
        int compressed_len =
                LZ4_compress_default(reinterpret_cast<const char*>(_block_buffer.data()),
                                     reinterpret_cast<char*>(_compress_buffer.data()), block_size, max_compressed);
        if (compressed_len <= 0) {
            return Status::InternalError("LZ4 compress failed");
        }
        RETURN_IF_ERROR(_write_block(block_size, _compress_buffer.data(), static_cast<uint32_t>(compressed_len)));
    } else {
        return Status::InvalidArgument(
                fmt::format("Unsupported block compression type: {}", CompressionTypePB_Name(_compression_type)));
    }

    _block_buffer.clear();
    return Status::OK();
}

Status CompressedOutputStream::_write_block(uint32_t block_size, const uint8_t* compressed_data,
                                            uint32_t compressed_len) {
    // Block stream format (big-endian):
    //   [uncompressed_block_len][compressed_len][compressed_payload]
    uint8_t header[kBlockHeaderSize];
    encode_fixed32_be(header, block_size);
    encode_fixed32_be(header + 4, compressed_len);
    RETURN_IF_ERROR(_underlying_stream->write(Slice(reinterpret_cast<const char*>(header), sizeof(header))));
    RETURN_IF_ERROR(_underlying_stream->write(Slice(reinterpret_cast<const char*>(compressed_data), compressed_len)));
    _compressed_bytes_written += sizeof(header) + compressed_len;
    return Status::OK();
}

size_t CompressedOutputStream::_estimate_block_compressed_len(CompressionTypePB compression_type, size_t input_size) {
    if (compression_type == CompressionTypePB::SNAPPY) {
        return snappy::MaxCompressedLength(input_size);
    }
    return LZ4_compressBound(input_size);
}

Status CompressedOutputStream::_write_block_end() {
    // Block stream terminator: block_len = 0
    uint8_t header[kBlockEndSize];
    encode_fixed32_be(header, 0);
    RETURN_IF_ERROR(_underlying_stream->write(Slice(reinterpret_cast<const char*>(header), sizeof(header))));
    _compressed_bytes_written += sizeof(header);
    return Status::OK();
}

Status CompressedOutputStream::_sync(const char* data, size_t size) {
    if (size == 0) {
        return Status::OK();
    }

    if (_is_block_compression) {
        size_t remaining = size;
        const uint8_t* input = reinterpret_cast<const uint8_t*>(data);
        while (remaining > 0) {
            size_t to_copy = std::min(remaining, kBlockBufferSize - _block_buffer.size());
            size_t old_size = _block_buffer.size();
            _block_buffer.resize(old_size + to_copy);
            memcpy(_block_buffer.data() + old_size, input, to_copy);
            input += to_copy;
            remaining -= to_copy;
            if (_block_buffer.size() >= kBlockBufferSize) {
                RETURN_IF_ERROR(_flush_block());
            }
        }
        return Status::OK();
    } else {
        return _compress_and_write(reinterpret_cast<const uint8_t*>(data), size);
    }
}

Status CompressedOutputStream::finalize() {
    // First flush any remaining data in the base OutputStream buffer
    RETURN_IF_ERROR(OutputStream::finalize());

    if (_is_block_compression) {
        RETURN_IF_ERROR(_flush_block());
        RETURN_IF_ERROR(_write_block_end());
    } else {
        bool stream_end = false;
        while (!stream_end) {
            size_t needed = _compressor->max_compressed_len(0);
            if (needed > _compress_buffer.size()) {
                _compress_buffer.resize(needed);
            }
            size_t output_written = 0;
            RETURN_IF_ERROR(_compressor->finish(_compress_buffer.data(), _compress_buffer.size(), &output_written,
                                                &stream_end));
            if (output_written > 0) {
                RETURN_IF_ERROR(_underlying_stream->write(
                        Slice(reinterpret_cast<const char*>(_compress_buffer.data()), output_written)));
                _compressed_bytes_written += output_written;
            }
            if (!stream_end && output_written == 0) {
                return Status::InternalError("Stream compression finish made no progress");
            }
        }
    }

    // Clear buffers to free memory (they are no longer needed after finalization)
    _block_buffer.clear();
    _compress_buffer.clear();

    // Finalize the underlying stream
    return _underlying_stream->finalize();
}

std::size_t CompressedOutputStream::size() {
    size_t total_pending = _pending_buffer_size() + _block_buffer.size();
    size_t pending_estimate = 0;
    if (total_pending > 0) {
        if (_is_block_compression) {
            // Estimate block framing overhead per block plus a final terminator.
            size_t num_blocks = (total_pending + kBlockBufferSize - 1) / kBlockBufferSize;
            size_t per_block_overhead = kBlockHeaderSize * num_blocks + kBlockEndSize;
            pending_estimate = _estimate_block_compressed_len(_compression_type, total_pending) + per_block_overhead;
        } else {
            pending_estimate = _compressor->max_compressed_len(total_pending);
        }
    }
    return _compressed_bytes_written + pending_estimate;
}

} // namespace starrocks::csv
