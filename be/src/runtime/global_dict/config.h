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

#include "types/logical_type.h"

namespace starrocks {

// TODO: we need to change the dict type to int16 later
using DictId = int32_t;

constexpr auto LowCardDictType = TYPE_INT;
constexpr int DICT_DECODE_MAX_SIZE = 256;

} // namespace starrocks
