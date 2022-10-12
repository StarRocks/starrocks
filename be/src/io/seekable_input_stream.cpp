// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "io/seekable_input_stream.h"

#include <fmt/format.h>

namespace starrocks::io {

StatusOr<int64_t> SeekableInputStream::read_at(int64_t offset, void* data, int64_t count) {
    RETURN_IF_ERROR(seek(offset));
    return read(data, count);
}

Status SeekableInputStream::read_at_fully(int64_t offset, void* data, int64_t count) {
    RETURN_IF_ERROR(seek(offset));
    return read_fully(data, count);
}

Status SeekableInputStream::skip(int64_t count) {
    ASSIGN_OR_RETURN(auto pos, position());
    return seek(pos + count);
}

void SeekableInputStream::set_size(int64_t count) {}

} // namespace starrocks::io
