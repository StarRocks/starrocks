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

    _uncompressed_bytes += size;

    // Compress the data
    Slice input(data, size);
    size_t max_compressed_size = _codec->max_compressed_len(size);
    _compress_buffer.resize(max_compressed_size);
    Slice output(reinterpret_cast<char*>(_compress_buffer.data()), max_compressed_size);

    RETURN_IF_ERROR(_codec->compress(input, &output));

    // Write compressed data to underlying stream
    auto p = reinterpret_cast<const uint8_t*>(output.data);
    RETURN_IF_ERROR(_stream->write(p, output.size));

    _compressed_bytes += output.size;
    return Status::OK();
}

Status CompressedAsyncOutputStreamFile::finalize() {
    RETURN_IF_ERROR(OutputStream::finalize());
    return _stream->close();
}

std::size_t CompressedAsyncOutputStreamFile::size() {
    return _stream->tell();
}

} // namespace starrocks::csv
