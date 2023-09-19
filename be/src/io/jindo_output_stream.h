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

#include "io/output_stream.h"
#include "jindo_utils.h"
#include "jindosdk/jdo_api.h"

namespace starrocks::io {

class JindoOutputStream : public OutputStream {
public:
    explicit JindoOutputStream(JdoSystem_t client, std::string file_path)
            : _jindo_client(std::move(client)), _file_path(std::move(file_path)) {
        JdoContext_t jdo_ctx = jdo_createContext1(_jindo_client);
        _write_handle = jdo_open(jdo_ctx, _file_path.c_str(), JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_APPEND, 0777);
        Status init_status = io::check_jindo_status(jdo_ctx);
        jdo_freeContext(jdo_ctx);
    }

    ~JindoOutputStream() override {
        if (_write_handle) {
            jdo_freeHandle(_write_handle);
            _write_handle = nullptr;
        }
    };

    // Disallow copy and assignment
    JindoOutputStream(const JindoOutputStream&) = delete;
    void operator=(const JindoOutputStream&) = delete;

    // Disallow move, because no usage now
    JindoOutputStream(JindoOutputStream&&) = delete;
    void operator=(JindoOutputStream&&) = delete;

    Status write(const void* data, int64_t size) override;

    [[nodiscard]] bool allows_aliasing() const override { return false; }

    Status write_aliased(const void* data, int64_t size) override {
        return Status::NotSupported("JindoOutputStream::write_aliased");
    }

    Status skip(int64_t count) override { return Status::NotSupported("JindoOutputStream::skip"); }

    StatusOr<Buffer> get_direct_buffer() override { return Buffer(); }

    StatusOr<Position> get_direct_buffer_and_advance(int64_t size) override { return nullptr; }

    Status close() override;

private:
    JdoSystem_t _jindo_client;
    JdoHandle_t _write_handle;
    std::string _file_path;
    std::string _buffer;

    Status flush();
};

} // namespace starrocks::io
