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
#include <roaring/roaring.hh>
#include <utility>

namespace starrocks {

class RowIdFilter {
public:
    virtual ~RowIdFilter() = default;
    virtual bool is_member(int64_t row_id) const = 0;
};

// Roaring bitmap-backed filter. Owns the bitmap.
//
// Uses 32-bit roaring::Roaring: segment-local row ids fit in uint32,
// matching the convention used by storage's DelIdFilter.
class BitmapRowIdFilter final : public RowIdFilter {
public:
    explicit BitmapRowIdFilter(roaring::Roaring bitmap) : _bitmap(std::move(bitmap)) {}

    bool is_member(int64_t row_id) const override {
        return row_id >= 0 && row_id <= UINT32_MAX && _bitmap.contains(static_cast<uint32_t>(row_id));
    }

    uint64_t cardinality() const { return _bitmap.cardinality(); }

    const roaring::Roaring& bitmap() const { return _bitmap; }

private:
    roaring::Roaring _bitmap;
};

} // namespace starrocks
