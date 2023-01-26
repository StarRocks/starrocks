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

#include "util/slice.h"

namespace starrocks {

class SeekTuple;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<ShortKeyOption>;

// ShortKeyOption represents a sub key range endpoint splitted from a key range.
// It could be a completed tuple key, or a short key with specific short_key_schema.
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

} // namespace starrocks
