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

#include "io/compressed_input_stream.h"

#include "gutil/strings/substitute.h"
#include "util/compression/stream_compression.h"

namespace starrocks::io {

Status CompressedInputStream::CompressedBuffer::read_with_hint_size(InputStream* f, size_t hint_size) {
    if (_eof) return Status::EndOfFile("");
    hint_size = aligned_size(hint_size + MAX_BLOCK_HEADER_SIZE);
    size_t buffer_size = available();
    if (buffer_size >= hint_size) return Status::OK();

    if (hint_size > _compressed_data.size()) {
        // create tmp buffer, copy it from _compressed_data to tmp, and then swap
        // noted that tmp resize is uninitialized.
        raw::RawVector<uint8_t> tmp(hint_size);
        memcpy(tmp.data(), &_compressed_data[_offset], buffer_size);
        _compressed_data.swap(tmp);
    } else {
        // no need to create tmp buffer, just do memmove.
        memmove(&_compressed_data[0], &_compressed_data[_offset], buffer_size);
    }
    _offset = 0;
    _limit = buffer_size;

    while (_limit < hint_size) {
        Slice buff(write_buffer());
        ASSIGN_OR_RETURN(buff.size, f->read(buff.data, buff.size));
        if (buff.size == 0) {
            _eof = true;
            return Status::EndOfFile("");
        }
        _limit += buff.size;
    }
    return Status::OK();
}

StatusOr<int64_t> CompressedInputStream::read(void* data, int64_t size) {
    size_t output_len = size;
    size_t output_bytes = 0;

    while (output_bytes == 0) {
        InputStream* f = _source_stream.get();
        size_t hint_size = _decompressor->get_compressed_block_size();
        _decompressor->set_compressed_block_size(0);
        Status st = _compressed_buff.read_with_hint_size(f, hint_size);

        if (!st.ok() && !st.is_end_of_file()) {
            return st;
        } else if (st.is_end_of_file() && _stream_end) {
            break;
        }

        auto* output = reinterpret_cast<uint8_t*>(data);
        Slice compressed_data = _compressed_buff.read_buffer();
        size_t input_bytes_read = 0;
        size_t output_bytes_written = 0;

        // NOTE(yanz): input data size could be 0 because for some block compression algorithm.
        // codec will decompress a block into buffer, and then copy buffer into output later(if output buffer is not large enough)
        // so sometimes input data size is 0, but there is still some data in buffer.
        // DCHECK_GT(compressed_data.size, 0);

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
