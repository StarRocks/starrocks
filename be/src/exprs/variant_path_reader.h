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

#include "column/variant_column.h"
#include "column/variant_path_parser.h"
#include "types/variant_value.h"

namespace starrocks {

enum class VariantReadState : uint8_t {
    kMissing, // path not found in typed or base — caller should return NULL
    kNull,    // typed null / SQL NULL — do NOT fallback to remain
    kValue,   // a value was found
};

struct VariantReadResult {
    VariantReadState state = VariantReadState::kMissing;
    VariantRowValue value;
};

// VariantPathReader provides structured, layered access to a VariantColumn
// given a parsed VariantPath.  Call prepare() once per batch (or per row for
// non-const paths) to bind the column and path, then call read_row() per row.
//
// Access layers (tried in order):
//   1. Typed shredded column (if path prefix matches a typed column)
//   2. Base remain-payload seek  (only when the leading object-key prefix of the
//      query path is NOT a parent of any shredded path — if it were, the remain
//      at that prefix would be incomplete and would return stale data)
//   3. Full row materialisation + seek (handles all remaining cases)
class VariantPathReader {
public:
    VariantPathReader() = default;

    // Bind column and path, then run schema matching.
    // Must be called before is_typed_exact() / read_row().
    // seg_offset allows sub-readers to reuse the same VariantPath without slicing.
    void prepare(const VariantColumn* col, const VariantPath* path, size_t seg_offset = 0);

    // Returns true when the query path maps exactly to a typed shredded column with
    // no remaining suffix.  In this case typed_type(), typed_type_desc(), and
    // typed_column() are valid and callers can take the bulk columnar fast-path.
    bool is_typed_exact() const;

    // Accessors valid when is_typed_exact() returns true.
    LogicalType typed_type() const;
    const TypeDescriptor& typed_type_desc() const;
    const Column* typed_column() const;

    // Read one row.  prepare() must have been called first.
    // For kTypedNoSuffix prefer the batch paths in variant_functions.cpp instead.
    VariantReadResult read_row(size_t row);

private:
    const VariantColumn* _col = nullptr;
    const VariantPath* _path = nullptr;
    size_t _seg_offset = 0;        // starting segment index within _path
    int _match_index = -1;         // index into shredded columns; -1 = no typed match
    VariantPath _suffix;           // path suffix after the matched typed prefix
    bool _has_typed_child = false; // leading object-key prefix is a parent of some shredded path
                                   // → remain at this prefix is incomplete → skip _seek_base

    // Try typed match (longest-to-shortest prefix).  On no match, also sets
    // _has_typed_child if the query path's leading object-key prefix is a parent
    // of any typed column path.
    void _try_match_typed(const VariantPath* path);

    // Layer 1: read from the matched typed shredded column.
    // Returns kNull (null cell) or kValue; never falls back to remain.
    VariantReadResult _read_typed_row(size_t row);

    // Layer 2: seek the requested path inside the base remain payload.
    // Returns true and fills *out on success.
    bool _seek_base(size_t row, VariantRowValue* out);

    // Layer 3: materialise the full row and seek.
    VariantReadResult _read_full(size_t row);
};

} // namespace starrocks
