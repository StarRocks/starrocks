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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/include/orc/orc-config.hh

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#define ORC_VERSION "1.7.0-SNAPSHOT"

#define ORC_CXX_HAS_CSTDINT
#define ORC_CXX_HAS_INITIALIZER_LIST
#define ORC_CXX_HAS_NOEXCEPT
#define ORC_CXX_HAS_NULLPTR
#define ORC_CXX_HAS_OVERRIDE
#define ORC_CXX_HAS_UNIQUE_PTR

#ifdef ORC_CXX_HAS_CSTDINT
#include <cstdint>
#else
#include <stdint.h>
#endif

#ifdef ORC_CXX_HAS_NOEXCEPT
#define ORC_NOEXCEPT noexcept
#else
#define ORC_NOEXCEPT throw()
#endif

#ifdef ORC_CXX_HAS_NULLPTR
#define ORC_NULLPTR nullptr
#else
namespace orc {
class nullptr_t {
public:
    template <class T>
    operator T*() const {
        return 0;
    }

    template <class C, class T>
    operator T C::*() const {
        return 0;
    }

private:
    void operator&() const; // whose address can't be taken
};
const nullptr_t nullptr = {};
} // namespace orc
#define ORC_NULLPTR orc::nullptr
#endif

#ifdef ORC_CXX_HAS_OVERRIDE
#define ORC_OVERRIDE override
#else
#define ORC_OVERRIDE
#endif

#ifdef ORC_CXX_HAS_UNIQUE_PTR
#define ORC_UNIQUE_PTR std::unique_ptr
#else
#define ORC_UNIQUE_PTR std::auto_ptr
namespace std {
template <typename T>
inline T move(T& x) {
    return x;
}
} // namespace std
#endif
