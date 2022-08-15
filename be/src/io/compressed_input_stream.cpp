// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "io/compressed_input_stream.h"

#include "gutil/strings/substitute.h"
#include "util/compression/stream_compression.h"

namespace starrocks::io {

StatusOr<int64_t> CompressedInputStream::read(void* data, int64_t size) {
    size_t output_len = size;
    size_t output_bytes = 0;

    while (output_bytes == 0) {
        Status st = _compressed_buff.read(_source_stream.get());
        if (!st.ok() && !st.is_end_of_file()) {
            return st;
        } else if (st.is_end_of_file() && _stream_end) {
            break;
        }

        uint8_t* output = reinterpret_cast<uint8_t*>(data);
        Slice compressed_data = _compressed_buff.read_buffer();
        size_t input_bytes_read = 0;
        size_t output_bytes_written = 0;

        DCHECK_GT(compressed_data.size, 0);

        RETURN_IF_ERROR(_decompressor->decompress((uint8_t*)compressed_data.data, compressed_data.size,
                                                  &input_bytes_read, output, output_len, &output_bytes_written,
                                                  &_stream_end));
        if (UNLIKELY(output_bytes_written == 0 && input_bytes_read == 0 && st.is_end_of_file())) {
            return Status::InternalError(strings::Substitute("Failed to decompress. input_len:$0, output_len:$0",
                                                             compressed_data.size, output_len));
        }
        _compressed_buff.skip(input_bytes_read);
        output_bytes += output_bytes_written;
    }
    return output_bytes;
}

Status CompressedInputStream::skip(int64_t n) {
    raw::RawVector<uint8_t> buff;
    buff.resize(n);
    while (n > 0) {
        Slice s(buff.data(), n);
        auto res = read(buff.data(), n);
        if (res.ok()) {
            n -= *res;
        } else if (res.status().is_end_of_file()) {
            return Status::OK();
        } else {
            return res.status();
        }
    }
    return Status::OK();
}

} // namespace starrocks::io
