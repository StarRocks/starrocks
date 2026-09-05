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

#include "formats/orc/lzo_decompressor_registration.h"

#include <cstdint>
#include <exception>

#include "base/compression/lzo_decompressor_registry.h"
#include "common/statusor.h"

namespace orc {
uint64_t lzoDecompress(const char* inputAddress, const char* inputLimit, char* outputAddress, char* outputLimit);
} // namespace orc

namespace starrocks {

namespace {

StatusOr<size_t> orc_lzo_decompress(const char* input_address, const char* input_limit, char* output_address,
                                    char* output_limit) {
    try {
        return static_cast<size_t>(orc::lzoDecompress(input_address, input_limit, output_address, output_limit));
    } catch (const std::exception& e) {
        return Status::InternalError(e.what());
    }
}

} // namespace

Status register_orc_lzo_decompressor() {
    return compression::register_lzo_decompressor(orc_lzo_decompress);
}

} // namespace starrocks
