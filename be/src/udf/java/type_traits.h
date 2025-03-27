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

#include <cstdint>

#include "util/slice.h"

namespace starrocks {
template <class Type>
struct JNIPrimTypeId {
    static constexpr bool supported = false;
};

#define DEFINE_TYPE(type, id_)                  \
    template <>                                 \
    struct JNIPrimTypeId<type> {                \
        static constexpr int id = id_;          \
        static constexpr bool supported = true; \
    }

DEFINE_TYPE(int8_t, 1);
DEFINE_TYPE(uint8_t, 2);
DEFINE_TYPE(int16_t, 3);
DEFINE_TYPE(int32_t, 4);
DEFINE_TYPE(uint32_t, 5);
DEFINE_TYPE(int64_t, 6);
DEFINE_TYPE(float, 7);
DEFINE_TYPE(double, 8);
DEFINE_TYPE(Slice, 9);
static constexpr int TYPE_ARRAY_METHOD_ID = 10;
static constexpr int TYPE_MAP_METHOD_ID = 11;

} // namespace starrocks