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

#include <roaring/roaring.hh>

#include "common/status.h"
#include "storage_primitive/range.h"
#include "storage_primitive/rowid_types.h"

namespace starrocks {

class BitmapIndexIterator {
public:
    virtual ~BitmapIndexIterator() = default;

    virtual bool has_null_bitmap() const = 0;
    virtual Status seek_dictionary(const void* value, bool* exact_match) = 0;
    virtual rowid_t current_ordinal() const = 0;
    virtual rowid_t bitmap_nums() const = 0;

    // An optional sidecar may disappear after this iterator was constructed. Such an index cannot prove
    // either a matching or an empty row set; callers must keep the original range and residual predicates.
    virtual bool is_file_missing() const { return false; }

    virtual Status read_bitmap(rowid_t ordinal, roaring::Roaring* result) = 0;

    virtual Status read_null_bitmap(roaring::Roaring* result) {
        if (has_null_bitmap()) {
            return read_bitmap(bitmap_nums() - 1, result);
        }
        return Status::OK();
    }

    virtual Status read_union_bitmap(rowid_t from, rowid_t to, roaring::Roaring* result) = 0;
    virtual Status read_union_bitmap(const SparseRange<>& range, roaring::Roaring* result) = 0;

    // Starts fetching, without waiting, the dictionary page seek_dictionary(|value|) reads, so that the lookups of
    // several values fetch their pages at the same time instead of one after another. An iterator whose storage cannot
    // fill its cache in the background ignores it. What it started stays wanted until the iterator is destroyed.
    virtual void prefetch_dictionary_page(const void* value) {}
    // Likewise for the pages holding the bitmaps of |ordinals|, which read_union_bitmap() and read_null_bitmap() read.
    virtual void prefetch_bitmap_pages(const SparseRange<>& ordinals) {}
};

} // namespace starrocks
