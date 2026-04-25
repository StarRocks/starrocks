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

#include <cstddef>

#include "base/status.h"
#include "base/statusor.h"

namespace starrocks::compression {

using LzoDecompressor = StatusOr<size_t> (*)(const char* input_address, const char* input_limit, char* output_address,
                                             char* output_limit);

Status register_lzo_decompressor(LzoDecompressor decompressor);

StatusOr<size_t> lzo_decompress(const char* input_address, const char* input_limit, char* output_address,
                                char* output_limit);

} // namespace starrocks::compression
