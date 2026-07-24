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

#include <memory>
#include <string>

#include "base/string/slice.h"
#include "gen_cpp/segment.pb.h"

namespace starrocks {

class SeekTuple;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<ShortKeyOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;
struct ShortKeyRangesOption;
using ShortKeyRangesOptionPtr = std::shared_ptr<ShortKeyRangesOption>;

// ShortKeyOption represents a sub key range endpoint splitted from a key range.
// It could be a completed tuple key, or a raw index key with its specific short_key_schema.
// The raw |short_key| slice is read verbatim off a segment's short key index, so its bytes and the
// paired |short_key_schema| follow that segment's encoding: for a legacy segment it is the truncated
// short-key prefix under the short-key schema; for a full-sort-key segment it is the full, untruncated
// sort key under the full sort-key schema. A tablet is logically split only when every scanned segment
// shares one encoding, so a single raw boundary stays byte-comparable against all of them.
struct ShortKeyOption {
public:
    ShortKeyOption() : tuple_key(nullptr), short_key_schema(nullptr), short_key(""), inclusive(false) {}

    ShortKeyOption(const SeekTuple* tuple_key, bool inclusive)
            : tuple_key(tuple_key), short_key_schema(nullptr), short_key(""), inclusive(inclusive) {}

    ShortKeyOption(SchemaPtr short_key_schema, const Slice& short_key, bool inclusive)
            : tuple_key(nullptr),
              short_key_schema(std::move(short_key_schema)),
              short_key(short_key),
              inclusive(inclusive) {}

    bool is_infinite() const { return tuple_key == nullptr && short_key.empty(); }

public:
    // Only one of tuple_key and short_key_schema has value.
    const SeekTuple* const tuple_key;

    const SchemaPtr short_key_schema;
    const Slice short_key;

    const bool inclusive;

    // Encoding pinned by the logical-split producer for the raw |short_key| bytes: FULL_SORT_KEY when
    // the bytes are a full, untruncated sort key (paired with the full sort-key schema); TRUNCATED for
    // the legacy short-key prefix. The Slice-overload consumer (segment_iterator
    // _get_row_ranges_by_short_key_ranges -> _lookup_ordinal(Slice,...)) selects the matching short key
    // decoder AND the row re-encode codec from THIS pin, never from a fresh read-config read, so a
    // mid-flight flip of the mutable read config cannot desync the producer's encoding from the
    // consumer's decode. Only meaningful for the raw-slice endpoint; tuple_key/infinite endpoints are
    // resolved through other paths and leave it at the default.
    ShortKeyEncodingPB encoding = SHORT_KEY_ENCODING_TRUNCATED;
};

// ShortKeyRangeOption represents a sub key range splitted from a key range.
struct ShortKeyRangeOption {
public:
    ShortKeyRangeOption(ShortKeyOptionPtr lower, ShortKeyOptionPtr upper)
            : lower(std::move(lower)), upper(std::move(upper)) {}

public:
    const ShortKeyOptionPtr lower;
    const ShortKeyOptionPtr upper;
};

struct ShortKeyRangesOption {
public:
    ShortKeyRangesOption(vector<ShortKeyRangeOptionPtr>&& short_key_ranges, bool is_first_split_of_tablet)
            : short_key_ranges(std::move(short_key_ranges)), is_first_split_of_tablet(is_first_split_of_tablet) {}

    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;
    const bool is_first_split_of_tablet;
};

} // namespace starrocks
