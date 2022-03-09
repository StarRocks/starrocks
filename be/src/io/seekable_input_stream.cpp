// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "io/seekable_input_stream.h"

namespace starrocks::io {

Status SeekableInputStream::read_at_fully(int64_t offset, void* data, int64_t count) {
    int64_t nread = 0;
    while (nread < count) {
        ASSIGN_OR_RETURN(auto n, read_at(offset + nread, static_cast<uint8_t*>(data) + nread, count - nread));
        nread += n;
        if (n == 0) {
            return Status::IOError("cannot read fully");
        }
    }
    return Status::OK();
}

} // namespace starrocks::io
