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

// Thrift UUID stub for macOS
// This file provides stub implementations for TJSONProtocol UUID methods
// which are missing in the thrift-0.20.0 library on macOS.
//
// IMPORTANT: Since TJSONProtocol class doesn't declare these methods in the header,
// we cannot provide them as out-of-line member function definitions. Instead, we
// provide them as weak symbols with the correct mangled names that the linker expects.

#ifdef __APPLE__

#include <cstdint>
#include <stdexcept>
#include <string>

// Forward declarations to avoid including full thrift headers
namespace apache {
namespace thrift {
struct TUuid {
    std::string uuid;
};
namespace protocol {
class TJSONProtocol;
}
} // namespace thrift
} // namespace apache

// Provide the UUID methods as weak symbols with correct C++ name mangling
// These match the symbols that TVirtualProtocol<TJSONProtocol> is looking for

extern "C" {
// Weak symbols - will be overridden if real implementations exist
__attribute__((weak)) uint32_t _ZN6apache6thrift8protocol13TJSONProtocol8readUUIDERNS0_5TUuidE(
        void* this_ptr, apache::thrift::TUuid& uuid) {
    throw std::runtime_error("Thrift UUID not supported on macOS");
    return 0;
}

__attribute__((weak)) uint32_t _ZN6apache6thrift8protocol13TJSONProtocol9writeUUIDERKNS0_5TUuidE(
        void* this_ptr, const apache::thrift::TUuid& uuid) {
    throw std::runtime_error("Thrift UUID not supported on macOS");
    return 0;
}
}

#endif // __APPLE__
