// This file is made available under Elastic License 2.0.
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
#include "types/bitmap_value_detail.h"
#include "util/coding.h"
#include "util/phmap/phmap.h"
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

    BitmapValue(const BitmapValue& other, bool deep_copy = true);
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

    // It is recommended to use batch writing to improve performance, such as add_many.
    void add(uint64_t value) {
        switch (_type) {
        case EMPTY:
            _sv = value;
            _type = SINGLE;
            break;
        case SINGLE:
            //there is no need to convert the type if two variables are equal
            if (_sv == value) {
                break;
            }

            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            _set->insert(_sv);
            _set->insert(value);
            _type = SET;
            break;
        case BITMAP:
            _copy_on_write();
            _bitmap->add(value);
            break;
        case SET:
            if (_set->size() < 32) {
                _set->insert(value);
            } else {
                _from_set_to_bitmap();
                _bitmap->add(value);
            }
        }
    }

    void add_many(size_t n_args, const uint32_t* vals);

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

<<<<<<< HEAD
    int64_t sub_bitmap_internal(const int64_t& offset, const int64_t& len, BitmapValue* ret_bitmap);
=======
    int64_t sub_bitmap_internal(const int64_t& offset, const int64_t& len, BitmapValue* ret_bitmap) const;

    int64_t bitmap_subset_limit_internal(const int64_t& range_start, const int64_t& limit,
                                         BitmapValue* ret_bitmap) const;

    int64_t bitmap_subset_in_range_internal(const int64_t& range_start, const int64_t& range_end,
                                            BitmapValue* ret_bitmap) const;

    BitmapDataType type() const { return _type; }
    bool is_shared() const { return _bitmap.use_count() > 1; }
>>>>>>> 6c74527af1 ([Enhancement] Optimize the performance of BitmapValue::add() (#34482))

private:
    void _convert_to_smaller_type();
    void _from_set_to_bitmap();
<<<<<<< HEAD

    enum BitmapDataType {
        EMPTY = 0,
        SINGLE = 1, // single element
        BITMAP = 2, // more than one elements
        SET = 3
    };
=======
    inline void _copy_on_write() {
        if (UNLIKELY(_bitmap == nullptr)) {
            _bitmap = std::make_shared<detail::Roaring64Map>();
            return;
        }

        if (UNLIKELY(_bitmap.use_count() > 1)) {
            _bitmap = std::make_shared<detail::Roaring64Map>(*_bitmap);
        }
    }
>>>>>>> 6c74527af1 ([Enhancement] Optimize the performance of BitmapValue::add() (#34482))

    // Use shared_ptr, not unique_ptr, because we want to avoid unnecessary copy
    std::shared_ptr<detail::Roaring64Map> _bitmap = nullptr;
    std::unique_ptr<phmap::flat_hash_set<uint64_t>> _set;
    uint64_t _sv = 0; // store the single value when _type == SINGLE
    BitmapDataType _type{EMPTY};
};
} // namespace starrocks
