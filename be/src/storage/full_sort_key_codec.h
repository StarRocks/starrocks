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

#include <vector>

#include "column/schema.h"
#include "common/status.h"
#include "storage/variant_tuple.h"

namespace starrocks {

// Decode a full, untruncated, order-preserving sort key produced by SeekTuple::full_sort_key_encode
// (see be/src/storage/seek_tuple.h) back into a VariantTuple of typed logical values, one entry per
// requested sort key column.
//
// |sort_key_idxes| must be the same column ids (in the same order) used to produce |encoded| via the
// physical full_sort_key_encode(sort_key_idxes, padding) overload. |encoded| must be a full (i.e.
// non-prefix) key: this function does not handle the KEY_MINIMAL_MARKER/KEY_MAXIMAL_MARKER padding
// sentinel appended for prefix scan keys.
//
// Per requested column, the encoded marker byte is read first: KEY_NULL_FIRST_MARKER decodes to a NULL
// datum; KEY_NORMAL_MARKER is followed by the column's encoded value, decoded according to its type:
//   - VARCHAR/VARBINARY/CHAR: un-escape the variable-length encoding (0x00 0x01 -> 0x00, terminated by
//     0x00 0x00, unless this is the last requested column, in which case the remainder of |encoded| is
//     the raw value). For CHAR this yields the visible (NUL-truncated) prefix that the physical encoder
//     canonicalizes to.
//   - Everything else: KeyCoder::decode_ascending, using the field's on-disk fixed size.
Status decode_full_sort_key(const Slice& encoded, const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                            VariantTuple* out);

// Returns true iff every column in |sort_key_idxes| can be encoded by
// SeekTuple::full_sort_key_encode / decoded by decode_full_sort_key above, i.e. it is either
// VARCHAR/VARBINARY/CHAR (encoded directly via escaping) or a fixed-size type with a
// registered KeyCoder (get_key_coder(type) != nullptr). Types with no registered coder --
// e.g. FLOAT/DOUBLE/JSON/complex types -- make this return false. The full sort key index
// must only be enabled for a segment when this predicate holds for its sort key; otherwise
// the writer must fall back to the legacy short-key index.
bool is_full_sort_key_encodable(const Schema& schema, const std::vector<uint32_t>& sort_key_idxes);

} // namespace starrocks
