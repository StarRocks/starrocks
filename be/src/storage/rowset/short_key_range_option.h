// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <string>

#include "util/slice.h"

namespace starrocks {
namespace vectorized {

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

} // namespace vectorized
} // namespace starrocks
