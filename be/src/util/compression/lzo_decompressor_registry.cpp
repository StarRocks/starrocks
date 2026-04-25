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

#include "util/compression/lzo_decompressor_registry.h"

#include <mutex>

namespace starrocks::compression {

namespace {

StatusOr<size_t> unsupported_lzo_decompress(const char*, const char*, char*, char*) {
    return Status::NotSupported("LZO decompressor is not registered");
}

std::mutex& lzo_decompressor_mutex() {
    static std::mutex mutex;
    return mutex;
}

LzoDecompressor& lzo_decompressor() {
    static LzoDecompressor decompressor = unsupported_lzo_decompress;
    return decompressor;
}

} // namespace

Status register_lzo_decompressor(LzoDecompressor decompressor) {
    if (decompressor == nullptr) {
        return Status::InvalidArgument("LZO decompressor must not be null");
    }

    std::lock_guard lock(lzo_decompressor_mutex());
    LzoDecompressor& registered = lzo_decompressor();
    if (registered != unsupported_lzo_decompress && registered != decompressor) {
        return Status::AlreadyExist("LZO decompressor already registered");
    }
    registered = decompressor;
    return Status::OK();
}

StatusOr<size_t> lzo_decompress(const char* input_address, const char* input_limit, char* output_address,
                                char* output_limit) {
    LzoDecompressor decompressor;
    {
        std::lock_guard lock(lzo_decompressor_mutex());
        decompressor = lzo_decompressor();
    }
    return decompressor(input_address, input_limit, output_address, output_limit);
}

} // namespace starrocks::compression
