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

CompressedAsyncOutputStreamFile::CompressedAsyncOutputStreamFile(io::AsyncFlushOutputStream* stream,
                                                                 CompressionTypePB compression_type,
                                                                 size_t buff_size)
        : OutputStream(buff_size), _stream(stream) {
    Status st = get_block_compression_codec(compression_type, &_codec);
    CHECK(st.ok()) << "Failed to get compression codec: " << st.message();
}

Status CompressedAsyncOutputStreamFile::_sync(const char* data, size_t size) {
    if (size == 0) {
        return Status::OK();
    }

    // Buffer all uncompressed data - compression happens in finalize()
    size_t old_size = _uncompressed_buffer.size();
    _uncompressed_buffer.resize(old_size + size);
    memcpy(_uncompressed_buffer.data() + old_size, data, size);
    _uncompressed_bytes += size;

    return Status::OK();
}

Status CompressedAsyncOutputStreamFile::finalize() {
    // First flush any remaining data in the base OutputStream buffer
    RETURN_IF_ERROR(OutputStream::finalize());

    // Now compress all buffered data at once
    if (_uncompressed_buffer.size() > 0) {
        Slice input(reinterpret_cast<const char*>(_uncompressed_buffer.data()), _uncompressed_buffer.size());
        size_t max_compressed_size = _codec->max_compressed_len(_uncompressed_buffer.size());
        _compress_buffer.resize(max_compressed_size);
        Slice output(reinterpret_cast<char*>(_compress_buffer.data()), max_compressed_size);

        RETURN_IF_ERROR(_codec->compress(input, &output));

        // Write compressed data to underlying stream
        auto p = reinterpret_cast<const uint8_t*>(output.data);
        RETURN_IF_ERROR(_stream->write(p, output.size));

        _compressed_bytes += output.size;

        // Clear the uncompressed buffer to free memory
        _uncompressed_buffer.clear();
        _uncompressed_buffer.shrink_to_fit();
    }

    return _stream->close();
}

std::size_t CompressedAsyncOutputStreamFile::size() {
    return _stream->tell();
}

} // namespace starrocks::csv
