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
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "util/random.h"

#include <limits.h>
#include <stdint.h>
#include <string.h>

#include <thread>
#include <utility>

#include "common/compiler_util.h"

#define STORAGE_DECL static thread_local

namespace starrocks {

Random* Random::GetTLSInstance() {
    STORAGE_DECL Random* tls_instance;
    STORAGE_DECL std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

    auto rv = tls_instance;
    if (UNLIKELY(rv == nullptr)) {
        size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
        rv = new (&tls_instance_bytes) Random((uint32_t)seed);
        tls_instance = rv;
    }
    return rv;
}

std::string Random::HumanReadableString(int len) {
    std::string ret;
    ret.resize(len);
    for (int i = 0; i < len; ++i) {
        ret[i] = static_cast<char>('a' + Uniform(26));
    }
    return ret;
}

std::string Random::RandomString(int len) {
    std::string ret;
    ret.resize(len);
    for (int i = 0; i < len; i++) {
        ret[i] = static_cast<char>(' ' + Uniform(95)); // ' ' .. '~'
    }
    return ret;
}

std::string Random::RandomBinaryString(int len) {
    std::string ret;
    ret.resize(len);
    for (int i = 0; i < len; i++) {
        ret[i] = static_cast<char>(Uniform(CHAR_MAX));
    }
    return ret;
}

} // namespace starrocks
