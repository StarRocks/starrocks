// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

namespace starrocks::io {
class ZeroCopyInputStream;
class ZeroCopyOutputStream;
} // namespace starrocks::io

namespace starrocks::vectorized {

class Column;

class ColumnSerde {
public:
    ~ColumnSerde() = default;

    // Returns an estimated size of the serialized data of |column|.
    virtual int64_t estimate_serialized_size(const Column& column) const = 0;

    // Write the contents of |column| to |out|.
    // Returns true on success and false on error.
    virtual bool serialize(const Column& column, io::ZeroCopyOutputStream* out) const = 0;

    // Read data from |in|, which is expected to have been produced by
    // `serialize` with a column of the same type as |result|.
    // Returns true on success and false on error.
    virtual bool deserialize(io::ZeroCopyInputStream* in, Column* result) const;
};

} // namespace starrocks::vectorized
namespace starrocks::vectorized
