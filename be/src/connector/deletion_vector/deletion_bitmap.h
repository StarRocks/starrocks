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

#include <roaring/roaring64.h>

#include <memory>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks {

class DeletionBitmap {
public:
    DeletionBitmap(roaring64_bitmap_t* bitmap) : _bitmap(bitmap) {}
    ~DeletionBitmap() {
        if (_bitmap != nullptr) {
            roaring::api::roaring64_bitmap_free(_bitmap);
        }
    }

    bool empty() const { return roaring64_bitmap_is_empty(_bitmap); };
    StatusOr<bool> fill_filter(uint64_t start, uint64_t end, Filter& filter);
    uint64_t get_range_cardinality(uint64_t start, uint64_t end) const;
    void add_value(uint64_t val);
    uint64_t get_cardinality() const;
    void to_array(std::vector<uint64_t>& array) const;

private:
    static const uint64_t kBatchSize = 256;

    roaring64_bitmap_t* _bitmap = nullptr;
};

using DeletionBitmapPtr = std::shared_ptr<DeletionBitmap>;

} // namespace starrocks