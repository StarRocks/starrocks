// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <rocksdb/status.h>

namespace starrocks {

static inline Status to_status(const rocksdb::Status& from) {
    switch (from.code()) {
    case rocksdb::Status::kOk:
        return Status::OK();
    case rocksdb::Status::kNotFound:
        return Status::NotFound(from.ToString());
    case rocksdb::Status::kCorruption:
        return Status::Corruption(from.ToString());
    case rocksdb::Status::kNotSupported:
        return Status::NotSupported(from.ToString());
    case rocksdb::Status::kInvalidArgument:
        return Status::InvalidArgument(from.ToString());
    case rocksdb::Status::kIOError:
        return Status::IOError(from.ToString());
    default:
        return Status::InternalError(from.ToString());
    }
}

} // namespace starrocks
