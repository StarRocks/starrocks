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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/bitmap_value.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <limits>
#include <map>
#include <new>
#include <numeric>
#include <optional>
#include <roaring/roaring.hh>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "util/coding.h"
#include "util/phmap/phmap_fwd_decl.h"
#include "util/slice.h"

namespace starrocks {

namespace detail {
class Roaring64Map;
}
// Represent the in-memory and on-disk structure of StarRocks's BITMAP data type.
// Optimize for the case where the bitmap contains 0 or 1 element which is common
// for streaming load scenario.
class BitmapValue {
public:
    // Construct an empty bitmap.
    BitmapValue();

    BitmapValue(const BitmapValue& other);
    BitmapValue& operator=(const BitmapValue& other);

    BitmapValue(BitmapValue&& other) noexcept;

    BitmapValue& operator=(BitmapValue&& other) noexcept;

    // Construct a bitmap with one element.
    explicit BitmapValue(uint64_t value);

    // Construct a bitmap from serialized data.
    explicit BitmapValue(const char* src);

    explicit BitmapValue(const Slice& src);

    // Construct a bitmap from given elements.
    explicit BitmapValue(const std::vector<uint64_t>& bits);

    void add(uint64_t value);

    // Note: rhs BitmapValue is only readable after this method
    // Compute the union between the current bitmap and the provided bitmap.
    // Possible type transitions are:
    // EMPTY  -> SINGLE
    // EMPTY  -> BITMAP
    // SINGLE -> BITMAP
    BitmapValue& operator|=(const BitmapValue& rhs);

    // Note: rhs BitmapValue is only readable after this method
    // Compute the intersection between the current bitmap and the provided bitmap.
    // Possible type transitions are:
    // SINGLE -> EMPTY
    // BITMAP -> EMPTY
    // BITMAP -> SINGLE
    BitmapValue& operator&=(const BitmapValue& rhs);

    void remove(uint64_t rhs);

    BitmapValue& operator-=(const BitmapValue& rhs);
    BitmapValue& operator^=(const BitmapValue& rhs);

    // check if value x is present
    bool contains(uint64_t x);

    // TODO should the return type be uint64_t?
    int64_t cardinality() const;

    std::optional<uint64_t> max() const;

    std::optional<uint64_t> min() const;

    // Return how many bytes are required to serialize this bitmap.
    // See BitmapTypeCode for the serialized format.
    size_t getSizeInBytes() const;

    // Serialize the bitmap value to dst, which should be large enough.
    // Client should call `getSizeInBytes` first to get the serialized size.
    void write(char* dst) const;

    // Deserialize a bitmap value from `src`.
    // Return false if `src` begins with unknown type code, true otherwise.
    bool deserialize(const char* src);
    // Use max_bytes to read from src safely.
    bool valid_and_deserialize(const char* src, size_t max_bytes);

    // TODO limit string size to avoid OOM
    std::string to_string() const;

    // Append values to array
    void to_array(std::vector<int64_t>* array) const;

    size_t serialize(uint8_t* dst) const;

    uint64_t serialize_size() const { return getSizeInBytes(); }

    // When you persist bitmap value to disk, you could call this method.
    // This method should be called before `serialize_size`.
    void compress() const;

    void clear();

    int64_t sub_bitmap_internal(const int64_t& offset, const int64_t& len, BitmapValue* ret_bitmap);

    int64_t bitmap_subset_limit_internal(const int64_t& range_start, const int64_t& limit, BitmapValue* ret_bitmap);

private:
    void _convert_to_smaller_type();
    void _from_set_to_bitmap();

    enum BitmapDataType {
        EMPTY = 0,
        SINGLE = 1, // single element
        BITMAP = 2, // more than one elements
        SET = 3
    };

    // Use shared_ptr, not unique_ptr, because we want to avoid unnecessary copy
    std::shared_ptr<detail::Roaring64Map> _bitmap = nullptr;
    std::unique_ptr<phmap::flat_hash_set<uint64_t>> _set;
    uint64_t _sv = 0; // store the single value when _type == SINGLE
    BitmapDataType _type{EMPTY};
};

} // namespace starrocks
