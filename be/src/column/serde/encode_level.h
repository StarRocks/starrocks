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
// Stores a NullableColumn whose rows are all NULL as just a row count, dropping both sub-column
// payloads. Values behind a NULL are undefined, so nothing observable is lost, but the layout
// differs from the default one, and a reader that does not know the bit would read the tag byte as
// column data. It is therefore opt-in, and a sender may only set it once it knows the reader
// understands it:
//   - load spill: writer and reader are the same process, so it is always safe.
//   - tablet sink RPC: the two ends are DIFFERENT BEs and may differ in version during a rolling
//     upgrade. The sender may set it only for the bits the receiver advertised in
//     PTabletWriterOpenResult::supported_chunk_encode_level. A BE predating that field leaves it
//     unset, which reads as 0 and keeps the sender on the dense layout.
//   - exchange: has no such negotiation, so it must never set this bit.
constexpr int ENCODE_ALL_NULL = 8;

inline bool is_integer_encoding_enabled(const int encode_level) {
    return encode_level & ENCODE_INTEGER;
}

inline bool is_string_encoding_enabled(const int encode_level) {
    return encode_level & ENCODE_STRING;
}

inline bool is_all_null_encoding_enabled(const int encode_level) {
    return encode_level & ENCODE_ALL_NULL;
}

} // namespace starrocks::serde
