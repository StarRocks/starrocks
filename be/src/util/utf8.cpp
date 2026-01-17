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

#include "util/utf8.h"

#include <fmt/format.h>
#include <unicode/ucasemap.h>

#include <memory>
#include <stdexcept>

namespace starrocks {

struct UCaseMapDeleter {
    void operator()(UCaseMap* p) const noexcept {
        if (p != nullptr) {
            ucasemap_close(p);
        }
    }
};

static UCaseMap* get_case_map() {
    static std::unique_ptr<UCaseMap, UCaseMapDeleter> case_map = []() {
        UErrorCode err_code = U_ZERO_ERROR;
        UCaseMap* map = ucasemap_open("", U_FOLD_CASE_DEFAULT, &err_code);
        if (U_FAILURE(err_code)) {
            throw std::runtime_error(fmt::format("Failed to open UCaseMap: {}", u_errorName(err_code)));
        }
        return std::unique_ptr<UCaseMap, UCaseMapDeleter>(map);
    }();
    return case_map.get();
}

void utf8_tolower(const char* src, size_t src_len, std::string& dst) {
    UCaseMap* case_map = get_case_map();

    // Initial buffer size - UTF-8 lowercase can be larger than original (e.g., German ß -> ss)
    dst.resize(src_len * 2);

    UErrorCode err_code = U_ZERO_ERROR;
    int32_t result_len = ucasemap_utf8ToLower(case_map, dst.data(), dst.size(), src, src_len, &err_code);

    if (err_code == U_BUFFER_OVERFLOW_ERROR) {
        err_code = U_ZERO_ERROR;
        dst.resize(result_len + 1);
        result_len = ucasemap_utf8ToLower(case_map, dst.data(), dst.size(), src, src_len, &err_code);
    }

    if (U_FAILURE(err_code)) {
        throw std::runtime_error(fmt::format("Failed to convert to lowercase: {}", u_errorName(err_code)));
    }
    dst.resize(result_len);
}

} // namespace starrocks
