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
