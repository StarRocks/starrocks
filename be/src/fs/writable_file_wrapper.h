// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/ownership.h"
#include "fs/writable_file_wrapper.h"

namespace starrocks {

class WritableFileWrapper : public WritableFile {
public:
    // |file| must outlive WritableFileWrapper.
    explicit WritableFileWrapper(WritableFile* file, Ownership ownership) : _file(file), _ownership(ownership) {}

    ~WritableFileWrapper() override {
        if (_ownership == kTakesOwnership) {
            delete _file;
        }
    }

    Status append(const Slice& data) override { return _file->append(data); }

    Status appendv(const Slice* data, size_t cnt) override { return _file->appendv(data, cnt); }

    Status pre_allocate(uint64_t size) override { return _file->pre_allocate(size); }

    Status close() override { return _file->close(); }

    Status flush(FlushMode mode) override { return _file->flush(mode); }

    Status sync() override { return _file->sync(); }

    uint64_t size() const override { return _file->size(); }

    const std::string& filename() const override { return _file->filename(); }

protected:
    WritableFile* _file;
    Ownership _ownership;
};

} // namespace starrocks
