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
#include "jindosdk/jdo_option_keys.h"
#include "jindosdk/jdo_options.h"

namespace starrocks::io {

Status JindoOutputStream::write(const void* data, int64_t size) {
    if (_write_handle == nullptr) {
        JdoHandleCtx_t jdo_ctx = jdo_createHandleCtx1(*(_jindo_client->jdo_store));
        // allow open file whose parent dir not exists by set JDO_CREATE_OPTS_IS_CREATE_PARENT true
        jdo_setOption(_jindo_client->option, JDO_CREATE_OPTS_IS_CREATE_PARENT, "1");
        _write_handle = jdo_open(jdo_ctx, _file_path.c_str(), JDO_OPEN_FLAG_CREATE | JDO_OPEN_FLAG_OVERWRITE, 0777,
                                 _jindo_client->option);
        Status init_status = io::check_jindo_status(jdo_ctx);
        jdo_freeHandleCtx(jdo_ctx);
        if (!init_status.ok()) {
            return init_status;
        }
    }

    JdoHandleCtx_t jdo_write_ctx = jdo_createHandleCtx2(*(_jindo_client->jdo_store), _write_handle);
    jdo_write(jdo_write_ctx, static_cast<const char*>(data), size, nullptr);
    Status status = io::check_jindo_status(jdo_write_ctx);
    jdo_freeHandleCtx(jdo_write_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_write";
    }

    return status;
}

Status JindoOutputStream::flush() {
    JdoHandleCtx_t jdo_write_ctx = jdo_createHandleCtx2(*(_jindo_client->jdo_store), _write_handle);
    jdo_flush(jdo_write_ctx, nullptr);
    Status status = io::check_jindo_status(jdo_write_ctx);
    jdo_freeHandleCtx(jdo_write_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_flush";
    }
    return status;
}

Status JindoOutputStream::close() {
    auto jdo_ctx = jdo_createHandleCtx2(*(_jindo_client->jdo_store), _write_handle);
    jdo_close(jdo_ctx, nullptr);
    Status status = io::check_jindo_status(jdo_ctx);
    jdo_freeHandleCtx(jdo_ctx);
    if (UNLIKELY(!status.ok())) {
        LOG(ERROR) << "Failed to execute jdo_close";
    }
    return status;
}

} // namespace starrocks::io
