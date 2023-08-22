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

#include "io/jindo_output_stream.h"

#include "common/logging.h"

namespace starrocks::io {

Status JindoOutputStream::write(const void* data, int64_t size) {
    _buffer.append(static_cast<const char*>(data), size);

    JdoContext_t jdo_write_ctx = jdo_createContext2(_jindo_client, _write_handle);
    jdo_write(jdo_write_ctx, static_cast<const char*>(data), size);
    Status status = io::check_jindo_status(jdo_write_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_write";
        jdo_freeContext(jdo_write_ctx);
        return Status::IOError("");
    }
    // 128MB
    if (size > 134217728) {
        RETURN_IF_ERROR(flush());
    }
    jdo_freeContext(jdo_write_ctx);
    return Status::OK();
}

Status JindoOutputStream::flush() {
    JdoContext_t jdo_write_ctx = jdo_createContext2(_jindo_client, _write_handle);
    jdo_flush(jdo_write_ctx);
    Status status = io::check_jindo_status(jdo_write_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_flush";
        jdo_freeContext(jdo_write_ctx);
        return Status::IOError("");
    }
    jdo_freeContext(jdo_write_ctx);
    return Status::OK();
}

Status JindoOutputStream::close() {
    auto jdo_ctx = jdo_createContext2(_jindo_client, _write_handle);
    jdo_close(jdo_ctx);
    jdo_freeContext(jdo_ctx);
    jdo_freeHandle(_write_handle);
    _write_handle = nullptr;
    return Status::OK();
}

} // namespace starrocks::io
