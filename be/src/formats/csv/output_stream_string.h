// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "formats/csv/output_stream.h"

namespace starrocks::vectorized::csv {

class OutputStreamString final : public OutputStream {
public:
    explicit OutputStreamString(size_t capacity = 64) : OutputStream(capacity) {}

    const std::string& as_string() const { return _str; }

protected:
    Status _sync(const char* data, size_t size) override {
        _str.append(data, size);
        return Status::OK();
    }

private:
    std::string _str;
};

} // namespace starrocks::vectorized::csv
