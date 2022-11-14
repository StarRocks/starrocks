// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "formats/csv/output_stream.h"
#include "fs/fs.h"

namespace starrocks::vectorized::csv {

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

} // namespace starrocks::vectorized::csv
