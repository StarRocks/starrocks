// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "io/array_input_stream.h"

namespace starrocks::io {

class StringInputStream : public io::SeekableInputStreamWrapper {
public:
    StringInputStream(std::string contents)
            : io::SeekableInputStreamWrapper(&_stream, kDontTakeOwnership),
              _contents(std::move(contents)),
              _stream(_contents.data(), _contents.size()) {}

private:
    std::string _contents;
    ArrayInputStream _stream;
};

} // namespace starrocks::io
