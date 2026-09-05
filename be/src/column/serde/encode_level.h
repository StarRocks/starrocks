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

namespace starrocks::serde {

constexpr int ENCODE_INTEGER = 2;
constexpr int ENCODE_STRING = 4;

inline bool is_integer_encoding_enabled(const int encode_level) {
    return encode_level & ENCODE_INTEGER;
}

inline bool is_string_encoding_enabled(const int encode_level) {
    return encode_level & ENCODE_STRING;
}

} // namespace starrocks::serde
