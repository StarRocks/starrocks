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

StatusOr<std::shared_ptr<CompressedAsyncOutputStreamFile>> CompressedAsyncOutputStreamFile::create(
        io::AsyncFlushOutputStream* stream, CompressionTypePB compression_type, size_t buff_size) {
    const BlockCompressionCodec* codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
    return std::shared_ptr<CompressedAsyncOutputStreamFile>(
            new CompressedAsyncOutputStreamFile(stream, codec, compression_type, buff_size));
}

CompressedAsyncOutputStreamFile::CompressedAsyncOutputStreamFile(io::AsyncFlushOutputStream* stream,
                                                                 const BlockCompressionCodec* codec,
                                                                 CompressionTypePB compression_type, size_t buff_size)
        : OutputStream(buff_size), _stream(stream), _codec(codec), _compression_type(compression_type) {}

bool CompressedAsyncOutputStreamFile::_supports_incremental_compression() const {
    // GZIP supports incremental compression because multiple GZIP streams can be
    // concatenated to form a valid GZIP file. gunzip will correctly decompress
    // concatenated GZIP streams.
    // Other formats (SNAPPY, LZ4, ZSTD) do not support this, so we buffer all data
    // and compress once in finalize().
    return _compression_type == CompressionTypePB::GZIP;
}

Status CompressedAsyncOutputStreamFile::_flush_compressed_chunk() {
    if (_uncompressed_buffer.empty()) {
        return Status::OK();
    }

    Slice input(reinterpret_cast<const char*>(_uncompressed_buffer.data()), _uncompressed_buffer.size());
    size_t max_compressed_size = _codec->max_compressed_len(_uncompressed_buffer.size());
    _compress_buffer.resize(max_compressed_size);
    Slice output(reinterpret_cast<char*>(_compress_buffer.data()), max_compressed_size);

    RETURN_IF_ERROR(_codec->compress(input, &output));

    // Write compressed data to underlying stream
    auto p = reinterpret_cast<const uint8_t*>(output.data);
    RETURN_IF_ERROR(_stream->write(p, output.size));

    // Clear the uncompressed buffer
    _uncompressed_buffer.clear();

    return Status::OK();
}

Status CompressedAsyncOutputStreamFile::_sync(const char* data, size_t size) {
    if (size == 0) {
        return Status::OK();
    }

    // Buffer uncompressed data
    size_t old_size = _uncompressed_buffer.size();
    _uncompressed_buffer.resize(old_size + size);
    memcpy(_uncompressed_buffer.data() + old_size, data, size);

    // For compression types that support incremental compression (GZIP),
    // flush when buffer reaches chunk size to avoid excessive memory usage.
    if (_supports_incremental_compression() && _uncompressed_buffer.size() >= kDefaultChunkSize) {
        RETURN_IF_ERROR(_flush_compressed_chunk());
    }

    return Status::OK();
}

Status CompressedAsyncOutputStreamFile::finalize() {
    // First flush any remaining data in the base OutputStream buffer
    RETURN_IF_ERROR(OutputStream::finalize());

    // Compress and write any remaining buffered data
    RETURN_IF_ERROR(_flush_compressed_chunk());

    // Shrink buffer to free memory
    _uncompressed_buffer.shrink_to_fit();

    return _stream->close();
}

std::size_t CompressedAsyncOutputStreamFile::size() {
    // Return buffered uncompressed data size plus any data already written to stream.
    // This allows upper layers (e.g., BufferPartitionChunkWriter) to correctly check
    // file size and make decisions (e.g., trigger file rotation) before finalize().
    return _uncompressed_buffer.size() + _stream->tell();
}

} // namespace starrocks::csv
