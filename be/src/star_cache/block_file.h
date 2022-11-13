// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <fcntl.h>
#include <sys/uio.h>
#include "common/config.h"
#include "star_cache/types.h"
#include "common/status.h"

namespace starrocks {

class BlockFile {
public:
    BlockFile(const std::string& path, size_t quota_bytes)
        : _file_path(path)
        , _quota_bytes(quota_bytes)
        , _fd(0)
    {}
    ~BlockFile() {
        if (_fd > 0) {
            close();
        }
    }

    Status open(bool pre_allocate);
    Status close();

    Status write(off_t offset, const IOBuf& buf);
    Status read(off_t offset, size_t size, IOBuf* buf);
    Status writev(off_t offset, const std::vector<IOBuf*>& bufv);
    Status readv(off_t offset, const std::vector<size_t>& sizev, std::vector<IOBuf*>* bufv);

private:
    std::string _file_path;
    size_t _quota_bytes;
    int _fd;
};


} // namespace starrocks
