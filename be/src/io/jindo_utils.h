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
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_defines.h"

namespace starrocks::io {
inline Status check_jindo_status(JdoContext_t jdo_ctx) {
    int32_t code = jdo_getCtxErrorCode(jdo_ctx);
    if (UNLIKELY(code != 0)) {
        std::string error_msg;
        const char* msg = jdo_getCtxErrorMsg(jdo_ctx);
        if (msg != nullptr) {
            error_msg.assign(msg);
        }
        jdo_freeContext(jdo_ctx);
        std::string message = fmt::format("jindo error, code = {}, message = {}", code, error_msg);
        LOG(WARNING) << message;
        return Status::IOError(message);
    }
    return Status::OK();
}

} // namespace starrocks::io
