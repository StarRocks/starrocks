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

#include "types/bitmap_value.h"

#include "gutil/strings/substitute.h"
#include "types/bitmap_value_detail.h"
#include "util/defer_op.h"
#include "util/phmap/phmap.h"
#include "util/raw_container.h"

namespace starrocks {

// only_value: values that in original_set and not in original_bitmap,
// common_value: values that in original_set and original_bitmap.
static void get_only_value_to_set_and_common_value_to_bitmap(const phmap::flat_hash_set<uint64_t>& original_set,
                                                             const detail::Roaring64Map& original_bitmap,
                                                             phmap::flat_hash_set<uint64_t>* set,
                                                             detail::Roaring64Map* bitmap) {
    for (auto x : original_set) {
        if (!original_bitmap.contains(x)) {
            // collect values only in set.
            set->insert(x);
        } else {
            // collect values in common of set and bitmap.
            bitmap->add(x);
        }
    }
}

BitmapValue::BitmapValue() = default;

BitmapValue::BitmapValue(BitmapValue&& other) noexcept
        : _bitmap(std::move(other._bitmap)), _set(std::move(other._set)), _sv(other._sv), _type(other._type) {
    other._sv = 0;
    other._type = EMPTY;
}

BitmapValue& BitmapValue::operator=(BitmapValue&& other) noexcept {
    if (this != &other) {
        this->_bitmap = std::move(other._bitmap);
        this->_set = std::move(other._set);
        this->_sv = other._sv;
        this->_type = other._type;
        other._sv = 0;
        other._type = EMPTY;
    }
    return *this;
}

// Construct a bitmap with one element.
BitmapValue::BitmapValue(uint64_t value) : _sv(value), _type(SINGLE) {}

// Construct a bitmap from serialized data.
BitmapValue::BitmapValue(const char* src) {
    bool res = deserialize(src);
    DCHECK(res);
}

BitmapValue::BitmapValue(const Slice& src) {
    bool res = deserialize(src.data);
    DCHECK(res);
}

BitmapValue::BitmapValue(const BitmapValue& other)
        : _bitmap(other._bitmap),
          _set(other._set == nullptr ? nullptr : std::make_unique<phmap::flat_hash_set<uint64_t>>(*other._set)),
          _sv(other._sv),
          _type(other._type) {
    // TODO: _set is usually relatively small, and it needs system performance testing to decide
    //  whether to change std::unique_ptr to std::shared_ptr and support shallow copy
}

BitmapValue& BitmapValue::operator=(const BitmapValue& other) {
    if (this != &other) {
        this->_bitmap = other._bitmap;
        this->_set = other._set == nullptr ? nullptr : std::make_unique<phmap::flat_hash_set<uint64_t>>(*other._set);
        this->_sv = other._sv;
        this->_type = other._type;
    }
    return *this;
}

// Construct a bitmap from given elements.
BitmapValue::BitmapValue(const std::vector<uint64_t>& bits) {
    // TODO: why not use SET ?
    switch (bits.size()) {
    case 0:
        _type = EMPTY;
        break;
    case 1:
        _type = SINGLE;
        _sv = bits[0];
        break;
    default:
        _type = BITMAP;
        _bitmap = std::make_shared<detail::Roaring64Map>();
        _bitmap->addMany(bits.size(), &bits[0]);
    }
}

void BitmapValue::_from_set_to_bitmap() {
    _bitmap = std::make_shared<detail::Roaring64Map>();
    for (auto x : *_set) {
        _bitmap->add(x);
    }
    _set.reset();
    _type = BITMAP;
}

// Note: rhs BitmapValue is only readable after this method
// Compute the union between the current bitmap and the provided bitmap.
// Possible type transitions are:
// EMPTY  -> SINGLE
// EMPTY  -> BITMAP
// SINGLE -> BITMAP
BitmapValue& BitmapValue::operator|=(const BitmapValue& rhs) {
    switch (rhs._type) {
    case EMPTY:
        return *this;
    case SINGLE:
        add(rhs._sv);
        return *this;
    case BITMAP:
        switch (_type) {
        case EMPTY:
            _bitmap = rhs._bitmap;
            _type = BITMAP;
            break;
        case SINGLE:
            _bitmap = std::make_shared<detail::Roaring64Map>(*rhs._bitmap);
            _bitmap->add(_sv);
            _type = BITMAP;
            break;
        case BITMAP:
            _copy_on_write();
            *_bitmap |= *rhs._bitmap;
            break;
        case SET:
            _bitmap = std::make_shared<detail::Roaring64Map>(*rhs._bitmap);
            for (auto x : *_set) {
                _bitmap->add(x);
            }
            _type = BITMAP;
            _set.reset();
        }
        break;
    case SET:
        switch (_type) {
        case EMPTY:
            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>(*rhs._set);
            _type = SET;
            break;
        case SINGLE:
            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>(*rhs._set);
            _type = SET;
            if (_set->size() < 32) {
                _set->insert(_sv);
            } else {
                _from_set_to_bitmap();
                _bitmap->add(_sv);
            }
            break;
        case SET:
            for (auto x : *rhs._set) {
                add(x);
            }
            break;
        case BITMAP:
            _copy_on_write();
            for (auto x : *rhs._set) {
                _bitmap->add(x);
            }
            break;
        }
    }
    return *this;
}

// Note: rhs BitmapValue is only readable after this method
// Compute the intersection between the current bitmap and the provided bitmap.
// Possible type transitions are:
// SINGLE -> EMPTY
// BITMAP -> EMPTY
// BITMAP -> SINGLE
BitmapValue& BitmapValue::operator&=(const BitmapValue& rhs) {
    switch (rhs._type) {
    case EMPTY:
        reset();
        break;
    case SINGLE:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (_sv != rhs._sv) {
                reset();
            }
            break;
        case BITMAP:
            if (!_bitmap->contains(rhs._sv)) {
                _type = EMPTY;
            } else {
                _type = SINGLE;
                _sv = rhs._sv;
            }
            _bitmap.reset();
            break;
        case SET:
            if (!_set->contains(rhs._sv)) {
                _type = EMPTY;
            } else {
                _type = SINGLE;
                _sv = rhs._sv;
            }
            _set.reset();
            break;
        }
        break;
    case BITMAP:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (!rhs._bitmap->contains(_sv)) {
                reset();
            }
            break;
        case BITMAP:
            _copy_on_write();
            *_bitmap &= *rhs._bitmap;
            _from_bitmap_to_smaller_type();
            break;
        case SET: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            for (auto x : *_set) {
                if (rhs._bitmap->contains(x)) {
                    set->insert(x);
                }
            }
            _set = std::move(set);
            break;
        }
        }
        break;
    case SET:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (!rhs._set->contains(_sv)) {
                reset();
            }
            break;
        case BITMAP: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            for (auto x : *rhs._set) {
                if (_bitmap->contains(x)) {
                    set->insert(x);
                }
            }
            _set = std::move(set);
            _bitmap.reset();
            _type = SET;
            break;
        }
        case SET: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            for (auto x : *rhs._set) {
                if (_set->contains(x)) {
                    set->insert(x);
                }
            }
            _set = std::move(set);
            break;
        }
        }
        break;
    }
    return *this;
}

void BitmapValue::remove(uint64_t rhs) {
    switch (_type) {
    case EMPTY:
        break;
    case SINGLE:
        if (_sv == rhs) {
            reset();
        }
        break;
    case BITMAP:
        _copy_on_write();
        _bitmap->remove(rhs);
        break;
    case SET:
        _set->erase(rhs);
        break;
    }
}

BitmapValue& BitmapValue::operator-=(const BitmapValue& rhs) {
    switch (rhs._type) {
    case EMPTY:
        break;
    case SINGLE:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (_sv == rhs._sv) {
                reset();
            }
            break;
        case BITMAP:
            _copy_on_write();
            _bitmap->remove(rhs._sv);
            break;
        case SET:
            _set->erase(rhs._sv);
            break;
        }
        break;
    case BITMAP:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (rhs._bitmap->contains(_sv)) {
                reset();
            }
            break;
        case BITMAP:
            _copy_on_write();
            *_bitmap -= *rhs._bitmap;
            _from_bitmap_to_smaller_type();
            break;
        case SET: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            for (const auto& x : *_set) {
                if (!rhs._bitmap->contains(x)) {
                    set->insert(x);
                }
            }
            _set = std::move(set);
            break;
        }
        }
        break;
    case SET:
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            if (rhs._set->contains(_sv)) {
                reset();
            }
            break;
        case BITMAP: {
            _copy_on_write();
            for (auto x : *rhs._set) {
                _bitmap->remove(x);
            }
            _from_bitmap_to_smaller_type();
            break;
        }
        case SET: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            for (auto x : *_set) {
                if (!rhs._set->contains(x)) {
                    set->insert(x);
                }
            }
            _set = std::move(set);
            break;
        }
        }
        break;
    }
    return *this;
}

BitmapValue& BitmapValue::operator^=(const BitmapValue& rhs) {
    switch (rhs._type) {
    case EMPTY:
        break;
    case SINGLE:
        switch (_type) {
        case EMPTY:
            add(rhs._sv);
            break;
        case SINGLE:
            if (_sv == rhs._sv) {
                reset();
            } else {
                add(rhs._sv);
            }
            break;
        case BITMAP:
            _copy_on_write();
            if (_bitmap->contains(rhs._sv)) {
                _bitmap->remove(rhs._sv);
            } else {
                _bitmap->add(rhs._sv);
            }
            break;
        case SET:
            if (_set->contains(rhs._sv)) {
                _set->erase(rhs._sv);
            } else {
                _set->insert(rhs._sv);
            }
            break;
        }
        break;
    case BITMAP:
        switch (_type) {
        case EMPTY:
            _bitmap = rhs._bitmap;
            _type = BITMAP;
            break;
        case SINGLE:
            _bitmap = std::make_shared<detail::Roaring64Map>(*rhs._bitmap);
            if (_bitmap->contains(_sv)) {
                _bitmap->remove(_sv);
            } else {
                _bitmap->add(_sv);
            }
            _type = BITMAP;
            break;
        case BITMAP: {
            _copy_on_write();
            *_bitmap ^= *rhs._bitmap;
            break;
        }
        case SET: {
            phmap::flat_hash_set<uint64_t> set;
            detail::Roaring64Map bitmap;

            get_only_value_to_set_and_common_value_to_bitmap(*_set, *rhs._bitmap, &set, &bitmap);

            // obtain values only in right bitmap
            _bitmap = std::make_shared<detail::Roaring64Map>(*rhs._bitmap);
            *_bitmap -= bitmap;

            // collect all values that only in left set or only in right bitmap.
            for (auto x : set) {
                _bitmap->add(x);
            }
            _type = BITMAP;
            _set.reset();

            break;
        }
        }
        break;
    case SET:
        switch (_type) {
        case EMPTY:
            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>(*rhs._set);
            _type = SET;
            break;
        case SINGLE:
            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>(*rhs._set);
            if (_set->contains(_sv)) {
                _set->erase(_sv);
            } else {
                _set->insert(_sv);
            }
            _type = SET;
            break;
        case BITMAP: {
            phmap::flat_hash_set<uint64_t> set;
            detail::Roaring64Map bitmap;

            get_only_value_to_set_and_common_value_to_bitmap(*rhs._set, *_bitmap, &set, &bitmap);

            // obtain values only in left bitmap
            *_bitmap -= bitmap;

            // collect all values that only in right set or only in left bitmap.
            for (auto x : set) {
                _bitmap->add(x);
            }

            break;
        }
        case SET: {
            auto set = std::make_unique<phmap::flat_hash_set<uint64_t>>();

            // collect values only in left set.
            for (auto x : *_set) {
                if (!rhs._set->contains(x)) {
                    set->insert(x);
                }
            }

            // collect values only in right set.
            for (auto x : *rhs._set) {
                if (!_set->contains(x)) {
                    set->insert(x);
                }
            }

            // obtain all values only in left set or only in right set.
            _set = std::move(set);
            break;
        }
        }
        break;
    }
    return *this;
}

// check if value x is present
bool BitmapValue::contains(uint64_t x) const {
    switch (_type) {
    case EMPTY:
        return false;
    case SINGLE:
        return _sv == x;
    case BITMAP:
        return _bitmap->contains(x);
    case SET:
        return _set->contains(x);
    }
    return false;
}

// TODO should the return type be uint64_t?
int64_t BitmapValue::cardinality() const {
    switch (_type) {
    case EMPTY:
        return 0;
    case SINGLE:
        return 1;
    case BITMAP:
        return _bitmap->cardinality();
    case SET:
        return _set->size();
    }
    return 0;
}

std::optional<uint64_t> BitmapValue::max() const {
    switch (_type) {
    case EMPTY:
        return {};
    case SINGLE:
        return _sv;
    case BITMAP:
        return _bitmap->maximum();
    case SET:
        if (_set->size() == 0) {
            return {};
        }
        uint64_t max = 0;
        for (auto value : *_set) {
            if (value > max) {
                max = value;
            }
        }
        return max;
    }
    return {};
}

std::optional<uint64_t> BitmapValue::min() const {
    switch (_type) {
    case EMPTY:
        return {};
    case SINGLE:
        return _sv;
    case BITMAP:
        return _bitmap->minimum();
    case SET:
        if (_set->size() == 0) {
            return {};
        }
        uint64_t min = std::numeric_limits<uint64_t>::max();
        for (const auto value : *_set) {
            if (value < min) {
                min = value;
            }
        }
        return min;
    }
    return {};
}

// Return how many bytes are required to serialize this bitmap.
// See BitmapTypeCode for the serialized format.
size_t BitmapValue::getSizeInBytes() const {
    size_t res = 0;
    switch (_type) {
    case EMPTY:
        res = 1;
        break;
    case SINGLE:
        if (_sv <= std::numeric_limits<uint32_t>::max()) {
            res = 1 + sizeof(uint32_t);
        } else {
            res = 1 + sizeof(uint64_t);
        }
        break;
    case BITMAP:
        DCHECK(_bitmap->cardinality() > 1);
        res = _bitmap->getSizeInBytes(config::bitmap_serialize_version);
        break;
    case SET:
        res = 1 + sizeof(uint32_t) + sizeof(uint64_t) * _set->size();
    }
    return res;
}

// Serialize the bitmap value to dst, which should be large enough.
// Client should call `getSizeInBytes` first to get the serialized size.
void BitmapValue::write(char* dst) const {
    switch (_type) {
    case EMPTY:
        *dst = BitmapTypeCode::EMPTY;
        break;
    case SINGLE:
        if (_sv <= std::numeric_limits<uint32_t>::max()) {
            *(dst++) = BitmapTypeCode::SINGLE32;
            encode_fixed32_le(reinterpret_cast<uint8_t*>(dst), static_cast<uint32_t>(_sv));
        } else {
            *(dst++) = BitmapTypeCode::SINGLE64;
            encode_fixed64_le(reinterpret_cast<uint8_t*>(dst), _sv);
        }
        break;
    case BITMAP:
        _bitmap->write(dst, config::bitmap_serialize_version);
        break;
    case SET:
        *dst = BitmapTypeCode::SET;
        dst += 1;
        uint32_t size = _set->size();
        memcpy(dst, &size, sizeof(uint32_t));
        dst += sizeof(uint32_t);
        for (auto key : *_set) {
            memcpy(dst, &key, sizeof(uint64_t));
            dst += sizeof(uint64_t);
        }
    }
}

// Deserialize a bitmap value from `src`.
// Return false if `src` begins with unknown type code, true otherwise.
bool BitmapValue::deserialize(const char* src) {
    if (src == nullptr) {
        _type = EMPTY;
        return true;
    }

    DCHECK(*src >= BitmapTypeCode::EMPTY && *src <= BitmapTypeCode::BITMAP64_SERIV2);
    switch (*src) {
    case BitmapTypeCode::EMPTY:
        _type = EMPTY;
        break;
    case BitmapTypeCode::SINGLE32:
        _type = SINGLE;
        _sv = decode_fixed32_le(reinterpret_cast<const uint8_t*>(src + 1));
        break;
    case BitmapTypeCode::SINGLE64:
        _type = SINGLE;
        _sv = decode_fixed64_le(reinterpret_cast<const uint8_t*>(src + 1));
        break;
    case BitmapTypeCode::BITMAP32:
    case BitmapTypeCode::BITMAP64:
    case BitmapTypeCode::BITMAP32_SERIV2:
    case BitmapTypeCode::BITMAP64_SERIV2:
        _type = BITMAP;
        _bitmap = std::make_shared<detail::Roaring64Map>(detail::Roaring64Map::read(src));
        break;
    case BitmapTypeCode::SET: {
        _type = SET;

        uint32_t size{};
        memcpy(&size, src + 1, sizeof(uint32_t));
        src += sizeof(uint32_t) + 1;

        _set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
        _set->reserve(size);

        for (int i = 0; i < size; ++i) {
            uint64_t key{};
            memcpy(&key, src, sizeof(uint64_t));
            _set->insert(key);
            src += sizeof(uint64_t);
        }
        break;
    }
    default:
        return false;
    }
    return true;
}

bool BitmapValue::valid_and_deserialize(const char* src, size_t max_bytes) {
    if (!max_bytes) {
        return false;
    }

    if (src == nullptr) {
        _type = EMPTY;
        return true;
    }

    if (*src < BitmapTypeCode::EMPTY || *src > BitmapTypeCode::BITMAP64_SERIV2) {
        return false;
    } else {
        bool valid = true;
        switch (*src) {
        case BitmapTypeCode::EMPTY:
            _type = EMPTY;
            break;
        case BitmapTypeCode::SINGLE32:
            if (max_bytes < (1 + sizeof(uint32_t))) {
                return false;
            }
            _type = SINGLE;
            _sv = decode_fixed32_le(reinterpret_cast<const uint8_t*>(src + 1));
            break;
        case BitmapTypeCode::SINGLE64:
            if (max_bytes < (1 + sizeof(uint64_t))) {
                return false;
            }
            _type = SINGLE;
            _sv = decode_fixed64_le(reinterpret_cast<const uint8_t*>(src + 1));
            break;
        case BitmapTypeCode::BITMAP32:
        case BitmapTypeCode::BITMAP64:
        case BitmapTypeCode::BITMAP32_SERIV2:
        case BitmapTypeCode::BITMAP64_SERIV2:
            _bitmap = std::make_shared<detail::Roaring64Map>(detail::Roaring64Map::read_safe(src, max_bytes, &valid));
            if (!valid) {
                return false;
            }
            _type = BITMAP;
            break;
        case BitmapTypeCode::SET: {
            if (max_bytes < (1 + sizeof(uint32_t))) {
                return false;
            }

            uint32_t set_size{};
            memcpy(&set_size, src + 1, sizeof(uint32_t));

            if (max_bytes < (1 + sizeof(uint32_t) + set_size * sizeof(uint64_t))) {
                return false;
            }

            _type = SET;
            src += sizeof(uint32_t) + 1;

            _set = std::make_unique<phmap::flat_hash_set<uint64_t>>();
            _set->reserve(set_size);

            for (int i = 0; i < set_size; ++i) {
                uint64_t key{};
                memcpy(&key, src, sizeof(uint64_t));
                _set->insert(key);
                src += sizeof(uint64_t);
            }
            break;
        }
        default:
            return false;
        }
        return true;
    }
}

// TODO limit string size to avoid OOM
std::string BitmapValue::to_string() const {
    std::stringstream ss;
    switch (_type) {
    case EMPTY:
        break;
    case SINGLE:
        ss << _sv;
        break;
    case BITMAP: {
        struct IterCtx {
            std::stringstream* ss = nullptr;
            bool first = true;
        } iter_ctx;
        iter_ctx.ss = &ss;

        _bitmap->iterate(
                [](uint64_t value, void* c) -> bool {
                    auto ctx = reinterpret_cast<IterCtx*>(c);
                    if (ctx->first) {
                        ctx->first = false;
                    } else {
                        (*ctx->ss) << ",";
                    }
                    (*ctx->ss) << value;
                    return true;
                },
                &iter_ctx);
        break;
    }
    case SET:
        int pos = 0;
        uint64_t values[_set->size()];
        for (auto value : *_set) {
            values[pos++] = value;
        }
        bool first = true;
        std::sort(values, values + pos);
        for (int i = 0; i < pos; ++i) {
            if (!first) {
                ss << ",";
            } else {
                first = false;
            }
            ss << values[i];
        }
        break;
    }
    return ss.str();
}

// Append values to array
void BitmapValue::to_array(std::vector<int64_t>* array) const {
    switch (_type) {
    case EMPTY:
        break;
    case SINGLE:
        array->emplace_back(_sv);
        break;
    case BITMAP: {
        size_t cur_size = array->size();
        array->resize(cur_size + _bitmap->cardinality());
        _bitmap->toUint64Array((uint64_t*)(*array).data() + cur_size);
        break;
    }
    case SET:
        array->reserve(array->size() + _set->size());
        auto iter = array->insert(array->end(), _set->begin(), _set->end());
        std::sort(iter, array->end());
        break;
    }
}

size_t BitmapValue::serialize(uint8_t* dst) const {
    write(reinterpret_cast<char*>(dst));
    return getSizeInBytes();
}

// When you persist bitmap value to disk, you could call this method.
// This method should be called before `serialize_size`.
void BitmapValue::compress() const {
    if (_type == BITMAP) {
        // no need to copy on write
        _bitmap->runOptimize();
        _bitmap->shrinkToFit();
    }
}

void BitmapValue::clear() {
    if (_bitmap != nullptr) {
        if (_bitmap.use_count() <= 1) {
            _bitmap->clear();
        } else {
            _bitmap.reset();
        }
    }
    if (_set != nullptr) {
        _set->clear();
    }
    _sv = 0;
    _type = EMPTY;
}

void BitmapValue::reset() {
    _bitmap.reset();
    _set.reset();
    _sv = 0;
    _type = EMPTY;
}

void BitmapValue::_from_bitmap_to_smaller_type() {
    uint64_t c = _bitmap->cardinality();
    if (c > 1) return;
    if (c == 0) {
        _type = EMPTY;
    } else {
        _type = SINGLE;
        auto min_value = _bitmap->minimum();
        DCHECK(min_value.has_value());
        _sv = min_value.value();
    }
    _bitmap.reset();
}

std::vector<BitmapValue> BitmapValue::split_bitmap(size_t batch_size) {
    std::vector<BitmapValue> results;

    if (batch_size == 0) {
        return results;
    }

    size_t cardinary_size = cardinality();
    size_t split_num = cardinary_size / batch_size + (cardinary_size % batch_size != 0);

    if (split_num <= 1) {
        results.emplace_back(*this);
        return results;
    }

    switch (_type) {
    case EMPTY:
        results.emplace_back(BitmapValue());
        break;
    case SINGLE:
        results.emplace_back(BitmapValue(*this));
        break;
    case SET: {
        std::vector values(_set->begin(), _set->end());
        std::sort(values.begin(), values.end());

        for (size_t i = 0; i < split_num; i++) {
            BitmapValue sub_bitmap;
            size_t end = std::min((i + 1) * batch_size, cardinary_size);
            for (size_t j = i * batch_size; j < end; j++) {
                sub_bitmap.add(values[j]);
            }
            results.emplace_back(std::move(sub_bitmap));
        }
        break;
    }
    case BITMAP: {
        auto iter = _bitmap->begin();
        for (size_t i = 0; i < split_num; i++) {
            BitmapValue sub_bitmap;
            for (size_t j = 0; j < batch_size && iter != _bitmap->end(); j++, iter++) {
                //TODO: add batch for performance
                sub_bitmap.add(*iter);
            }
            results.emplace_back(std::move(sub_bitmap));
        }
        break;
    }
    default:
        CHECK(false);
    }

    return results;
}

int64_t BitmapValue::sub_bitmap_internal(const int64_t& offset, const int64_t& len, BitmapValue* ret_bitmap) const {
    switch (_type) {
    case EMPTY:
        return 0;
    case SINGLE: {
        if (offset >= 1 || offset < -1 || len <= 0) {
            return 0;
        } else {
            ret_bitmap->add(_sv);
            return 1;
        }
    }
    case SET: {
        size_t cardinality = _set->size();
        if ((offset > 0 && offset >= cardinality) || (offset < 0 && std::abs(offset) > cardinality)) {
            return 0;
        }
        int64_t abs_offset = offset;
        if (offset < 0) {
            abs_offset = cardinality + offset;
        }

        std::vector values(_set->begin(), _set->end());
        std::sort(values.begin(), values.end());

        int64_t count = 0;
        for (auto idx = abs_offset; idx < values.size() && count < len; ++idx, ++count) {
            ret_bitmap->add(values[idx]);
        }
        return count;
    }
    default:
        DCHECK_EQ(_type, BITMAP);
        size_t cardinality = _bitmap->cardinality();
        if ((offset > 0 && offset >= cardinality) || (offset < 0 && std::abs(offset) > cardinality)) {
            return 0;
        }
        int64_t abs_offset = offset;
        if (offset < 0) {
            abs_offset = cardinality + offset;
        }

        int64_t count = 0;
        int64_t offset_count = 0;
        auto it = _bitmap->begin();
        for (; it != _bitmap->end() && offset_count < abs_offset; ++it) {
            ++offset_count;
        }
        for (; it != _bitmap->end() && count < len; ++it, ++count) {
            ret_bitmap->add(*it);
        }
        return count;
    }
}

void BitmapValue::add_many(size_t n_args, const uint32_t* vals) {
    if (_type != BITMAP) {
        for (size_t i = 0; i < n_args; i++) {
            add(vals[i]);
        }
    } else {
        _copy_on_write();
        _bitmap->addMany(n_args, vals);
    }
}

} // namespace starrocks
