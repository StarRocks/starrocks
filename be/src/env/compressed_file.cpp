// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/compressed_file.h"

#include "exec/decompressor.h"

namespace starrocks {

Status CompressedSequentialFile::read(Slice* result) {
    size_t output_len = result->size;
    size_t output_bytes = 0;
    result->size = 0;

    while (output_bytes == 0) {
        Status st = _compressed_buff.read(_input_file.get());
        if (!st.ok() && !st.is_end_of_file()) {
            return st;
        } else if (st.is_end_of_file() && _stream_end) {
            return Status::OK();
        }

        uint8_t* output = reinterpret_cast<uint8_t*>(result->data);
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
    result->size = output_bytes;

    return Status::OK();
}

Status CompressedSequentialFile::skip(uint64_t n) {
    raw::RawVector<uint8_t> buff;
    buff.resize(n);
    while (n > 0) {
        Slice s(buff.data(), n);
        Status st = read(&s);
        if (st.ok()) {
            n -= s.size;
        } else if (st.is_end_of_file()) {
            return Status::OK();
        } else {
            return st;
        }
    }
    return Status::OK();
}

} // namespace starrocks
