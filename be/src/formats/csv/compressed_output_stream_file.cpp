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

#include "compressed_output_stream_file.h"

#include <zlib.h>

#include "gutil/strings/substitute.h"

namespace starrocks::csv {

CompressedAsyncOutputStreamFile::CompressedAsyncOutputStreamFile(io::AsyncFlushOutputStream* stream,
                                                                 TCompressionType::type compression_type,
                                                                 size_t buff_size)
        : OutputStream(buff_size), _stream(stream), _compression_type(compression_type) {}

Status CompressedAsyncOutputStreamFile::finalize() {
    RETURN_IF_ERROR(OutputStream::finalize());
    return _stream->close();
}

Status CompressedAsyncOutputStreamFile::_sync(const char* data, size_t size) {
    if (_compression_type == TCompressionType::NO_COMPRESSION) {
        auto p = reinterpret_cast<const uint8_t*>(data);
        return _stream->write(p, size);
    }

    switch (_compression_type) {
    case TCompressionType::GZIP:
        return _compress_gzip(data, size);
    default:
        return Status::NotSupported(strings::Substitute("Compression type $0 is not supported for CSV format",
                                                        static_cast<int>(_compression_type)));
    }
}

Status CompressedAsyncOutputStreamFile::_compress_gzip(const char* data, size_t size) {
    z_stream zstrm = {};
    zstrm.zalloc = Z_NULL;
    zstrm.zfree = Z_NULL;
    zstrm.opaque = Z_NULL;

    // Use gzip format (MAX_WBITS + 16) for standard compatibility
    int ret = deflateInit2(&zstrm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
        return Status::InternalError(strings::Substitute("deflateInit2 failed: $0", zError(ret)));
    }

    // Estimate compressed size and reserve buffer
    size_t max_size = deflateBound(&zstrm, size) + 32; // Extra space for gzip header/trailer
    _compressed_buffer.resize(max_size);

    zstrm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(data));
    zstrm.avail_in = size;
    zstrm.next_out = reinterpret_cast<Bytef*>(_compressed_buffer.data());
    zstrm.avail_out = max_size;

    // Compress entire block at once
    ret = deflate(&zstrm, Z_FINISH);
    if (ret != Z_STREAM_END) {
        deflateEnd(&zstrm);
        return Status::InternalError(strings::Substitute("deflate failed: $0", zError(ret)));
    }

    size_t compressed_size = max_size - zstrm.avail_out;
    deflateEnd(&zstrm);

    // Write compressed block to stream
    return _stream->write(_compressed_buffer.data(), compressed_size);
}

} // namespace starrocks::csv
