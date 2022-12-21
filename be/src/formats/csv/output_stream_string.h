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

#include "formats/csv/output_stream.h"

namespace starrocks::csv {

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

} // namespace starrocks::csv
