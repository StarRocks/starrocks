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

#include "base/compression/lzo_decompressor_registry.h"

#include <atomic>

namespace starrocks::compression {

namespace {

StatusOr<size_t> unsupported_lzo_decompress(const char*, const char*, char*, char*) {
    return Status::NotSupported("LZO decompressor is not registered");
}

std::atomic<LzoDecompressor>& lzo_decompressor() {
    static std::atomic<LzoDecompressor> decompressor = unsupported_lzo_decompress;
    return decompressor;
}

} // namespace

Status register_lzo_decompressor(LzoDecompressor decompressor) {
    if (decompressor == nullptr) {
        return Status::InvalidArgument("LZO decompressor must not be null");
    }

    LzoDecompressor expected = unsupported_lzo_decompress;
    if (lzo_decompressor().compare_exchange_strong(expected, decompressor, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
        return Status::OK();
    }
    if (expected != decompressor) {
        return Status::AlreadyExist("LZO decompressor already registered");
    }
    return Status::OK();
}

StatusOr<size_t> lzo_decompress(const char* input_address, const char* input_limit, char* output_address,
                                char* output_limit) {
    LzoDecompressor decompressor = lzo_decompressor().load(std::memory_order_relaxed);
    return decompressor(input_address, input_limit, output_address, output_limit);
}

} // namespace starrocks::compression
