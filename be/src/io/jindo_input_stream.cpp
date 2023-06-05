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

#include "io/jindo_input_stream.h"

#include <fmt/format.h>

#include "jindosdk/jdo_file_status.h"

namespace starrocks::io {

StatusOr<int64_t> JindoInputStream::read(void* out, int64_t count) {
    if (UNLIKELY(_size == -1)) {
        ASSIGN_OR_RETURN(_size, JindoInputStream::get_size());
    }
    if (_offset >= _size) {
        return 0;
    }

    int64_t bytes_to_read = count;
    int64_t bytes_read = 0;
    int64_t bytes = 0;
    while (bytes_to_read > 0) {
        JdoContext_t jdo_read_ctx = jdo_createContext2(_jindo_client, _open_handle);
        bytes = jdo_pread(jdo_read_ctx, static_cast<char*>(out), bytes_to_read, _offset);
        Status read_status = check_jindo_status(jdo_read_ctx);
        if (UNLIKELY(!read_status.ok())) {
            LOG(ERROR) << fmt::format("Failed to read the file {}, offset = {}, length = {}", _file_path, _offset,
                                      bytes_to_read);
            return read_status;
        }
        jdo_freeContext(jdo_read_ctx);
        if (bytes < 0) {
            break;
        }
        _offset += bytes;
        bytes_read += bytes;
        bytes_to_read -= bytes;
    }
    // relax check exception due to the wrong params from CSV scanner
    if (bytes_read < count) {
        std::string msg = fmt::format(
                "No enough data to read from the file {}, current offset = {}, bytes_read = {},"
                "bytes_to_read = {}, bytes last read = {}",
                _file_path, _offset + bytes_read, bytes_read, bytes_to_read, bytes);
        LOG(WARNING) << msg;
    }
    return bytes_read;
}

Status JindoInputStream::seek(int64_t offset) {
    if (offset < 0) return Status::InvalidArgument(fmt::format("Invalid offset {}", offset));
    _offset = offset;
    return Status::OK();
}

StatusOr<int64_t> JindoInputStream::position() {
    return _offset;
}

StatusOr<int64_t> JindoInputStream::get_size() {
    if (_size == -1) {
        JdoContext_t jdo_ctx = jdo_createContext1(_jindo_client);
        bool file_exist = jdo_exists(jdo_ctx, _file_path.c_str());
        if (UNLIKELY(!file_exist)) {
            std::string msg = fmt::format("File {} does not exist, request offset = {}", _file_path, _offset);
            LOG(ERROR) << msg;
            return Status::IOError(msg);
        }

        JdoFileStatus_t info;
        jdo_getFileStatus(jdo_ctx, _file_path.c_str(), &info);
        Status get_status = check_jindo_status(jdo_ctx);
        if (UNLIKELY(!get_status.ok())) {
            LOG(ERROR) << fmt::format("Failed to get the size of file {}", _file_path);
            return get_status;
        }
        jdo_freeContext(jdo_ctx);
        _size = jdo_getFileStatusFileSize(info);
    }
    return _size;
}

void JindoInputStream::set_size(int64_t value) {
    _size = value;
}

} // namespace starrocks::io
