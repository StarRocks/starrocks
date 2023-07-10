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

#include <fmt/format.h>

#include <memory>
#include <string>

#include "io/seekable_input_stream.h"
#include "jindo_utils.h"
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_defines.h"

namespace starrocks::io {

class JindoInputStream final : public SeekableInputStream {
public:
    explicit JindoInputStream(JdoSystem_t client, std::string file_path)
            : _jindo_client(std::move(client)), _file_path(std::move(file_path)) {
        JdoContext_t jdo_ctx = jdo_createContext1(_jindo_client);
        _open_handle = jdo_open(jdo_ctx, _file_path.c_str(), JDO_OPEN_FLAG_READ_ONLY, 0777);
        io::check_jindo_status(jdo_ctx);
        jdo_freeContext(jdo_ctx);
    }

    ~JindoInputStream() override {
        if (_open_handle) {
            auto jdo_ctx = jdo_createContext2(_jindo_client, _open_handle);
            jdo_close(jdo_ctx);
            jdo_freeContext(jdo_ctx);
            jdo_freeHandle(_open_handle);
            _open_handle = nullptr;
        }
    };

    // Disallow copy and assignment
    JindoInputStream(const JindoInputStream&) = delete;
    void operator=(const JindoInputStream&) = delete;

    // Disallow move ctor and move assignment, because no usage now
    JindoInputStream(JindoInputStream&&) = delete;
    void operator=(JindoInputStream&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    void set_size(int64_t size) override;

private:
    JdoSystem_t _jindo_client;
    JdoHandle_t _open_handle;
    std::string _file_path;
    int64_t _offset{0};
    int64_t _size{-1};
};

} // namespace starrocks::io
