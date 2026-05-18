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

#include "common/status.h"

namespace arrow {
class Array;
} // namespace arrow

namespace starrocks {

class JsonColumn;

// Convert nested Arrow types (Map/List/Struct) and supported primitive Arrow
// types into StarRocks JSON values.
Status convert_arrow_to_json(const arrow::Array* array, JsonColumn* output, size_t array_start_idx,
                             size_t num_elements);

} // namespace starrocks
