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

#include <memory>

#include "formats/csv/output_stream.h"
#include "fs/fs.h"

namespace starrocks::csv {

class OutputStreamFile final : public OutputStream {
public:
    OutputStreamFile(std::unique_ptr<WritableFile> file, size_t buff_size)
            : OutputStream(buff_size), _file(std::move(file)) {}

    Status finalize() override {
        RETURN_IF_ERROR(OutputStream::finalize());
        return _file->close();
    }

    std::size_t size() override { return _file->size(); }

protected:
    Status _sync(const char* data, size_t size) override { return _file->append(Slice(data, size)); }

private:
    std::unique_ptr<WritableFile> _file;
};

} // namespace starrocks::csv
