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
#include <optional>
#include <string>
#include <vector>

#include "base/statusor.h"
#include "column/variant_path_parser.h"
#include "types/variant_value.h"

namespace starrocks {

// Mutable builder for row-level VARIANT binary.
//
// Why this exists:
// - Shredded read paths need to rebuild one VariantRowValue from:
//   1) base remain payload (optional),
//   2) typed overlays (path -> value),
//   3) metadata dictionary semantics.
// - Variant binary is compacted blob format, unlike JSON in-memory trees.
// - This builder is the future convergence point for metadata-aware overlay merge.
//   Current implementation uses binary decode -> overlay -> binary encode.
class VariantBuilder {
public:
    struct Overlay {
        // Path identifying where the value is applied in the object tree.
        // An empty VariantPath (segments.empty()) replaces the root entirely.
        // Only object-key segments are supported; array segments are not allowed.
        VariantPath path;
        VariantRowValue value;
    };

    // base == nullptr means start from empty root.
    VariantBuilder() = default;
    explicit VariantBuilder(const VariantRowValue* base)
            : _base(base ? std::optional<VariantRowRef>(base->as_ref()) : std::nullopt) {}
    explicit VariantBuilder(const VariantRowRef* base)
            : _base(base ? std::optional<VariantRowRef>(*base) : std::nullopt) {}
    ~VariantBuilder() = default;

    // Replace overlays in one batch. Builder is not designed for incremental updates.
    Status set_overlays(std::vector<Overlay>&& overlays);

    // Build the final row-level variant.
    StatusOr<VariantRowValue> build() const;

private:
    std::optional<VariantRowRef> _base;
    std::vector<Overlay> _overlays;
};

// Streaming builder for array-level VARIANT binary.
//
// Usage:
//   VariantArrayBuilder builder;
//   for each element: builder.add(element) or builder.add_null()
//   auto result = builder.build();
//
// All elements are re-encoded under a single unified metadata dictionary.
class VariantArrayBuilder {
public:
    void add(VariantRowValue element) { _elements.emplace_back(std::move(element)); }
    void add_null() { _elements.emplace_back(std::nullopt); }

    StatusOr<VariantRowValue> build() const;

private:
    std::vector<std::optional<VariantRowValue>> _elements;
};

} // namespace starrocks
