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
#include <string_view>
#include <utility>

#include "column/vectorized_fwd.h"
#include "storage/aggregate_type.h"
#include "storage/olap_common.h"
#include "storage/types.h"
#include "util/c_string.h"

namespace starrocks {

class Datum;

class Field {
public:
    Field(ColumnId id, std::string_view name, TypeInfoPtr type, starrocks::StorageAggregateType agg,
          uint8_t short_key_length, bool is_key, bool nullable)
            : _id(id),
              _agg_method(agg),
              _name(name),
              _type(std::move(type)),
              _sub_fields(nullptr),
              _short_key_length(short_key_length),
              _flags(static_cast<uint8_t>((is_key << kIsKeyShift) | (nullable << kNullableShift))) {}

    // Non-key field of any type except for ARRAY
    Field(ColumnId id, std::string_view name, LogicalType type, int precision, int scale, bool nullable)
            : Field(id, name, get_type_info(type, precision, scale), STORAGE_AGGREGATE_NONE, 0, false, nullable) {}

    // Non-key field of any type except for DECIMAL32, DECIMAL64, DECIMAL128, and ARRAY
    Field(ColumnId id, std::string_view name, LogicalType type, bool nullable)
            : Field(id, name, type, -1, -1, nullable) {
        DCHECK(type != TYPE_DECIMAL32);
        DCHECK(type != TYPE_DECIMAL64);
        DCHECK(type != TYPE_DECIMAL128);
        DCHECK(type != TYPE_ARRAY);
    }

    // Non-key field of any type
    Field(ColumnId id, std::string_view name, TypeInfoPtr type, bool nullable = true)
            : Field(id, name, std::move(type), STORAGE_AGGREGATE_NONE, 0, false, nullable) {}

    ~Field() { delete _sub_fields; }

    FieldPtr copy() const { return std::make_shared<Field>(*this); }

    Field(const Field& rhs)
            : _id(rhs._id),
              _agg_method(rhs._agg_method),
              _name(rhs._name),
              _type(rhs._type),
              _sub_fields(rhs._sub_fields ? new Buffer<Field>(*rhs._sub_fields) : nullptr),
              _short_key_length(rhs._short_key_length),
              _flags(rhs._flags),
              _uid(rhs._uid) {}

    Field(Field&& rhs) noexcept
            : _id(rhs._id),
              _agg_method(rhs._agg_method),
              _name(std::move(rhs._name)),
              _type(std::move(rhs._type)),
              _sub_fields(rhs._sub_fields),
              _short_key_length(rhs._short_key_length),
              _flags(rhs._flags),
              _uid(rhs._uid) {
        rhs._sub_fields = nullptr;
    }

    Field& operator=(const Field& rhs) {
        if (&rhs != this) {
            delete _sub_fields;
            _id = rhs._id;
            _name = rhs._name;
            _type = rhs._type;
            _agg_method = rhs._agg_method;
            _short_key_length = rhs._short_key_length;
            _flags = rhs._flags;
            _sub_fields = rhs._sub_fields ? new Buffer<Field>(*rhs._sub_fields) : nullptr;
            _uid = rhs._uid;
        }
        return *this;
    }

    Field& operator=(Field&& rhs) noexcept {
        if (&rhs != this) {
            _id = rhs._id;
            _name = std::move(rhs._name);
            _type = std::move(rhs._type);
            _agg_method = rhs._agg_method;
            _short_key_length = rhs._short_key_length;
            _flags = rhs._flags;
            _uid = rhs._uid;
            std::swap(_sub_fields, rhs._sub_fields);
        }
        return *this;
    }

    // return a copy of this field with the replaced type
    FieldPtr with_type(const TypeInfoPtr& type);

    // return a copy of this field with the replaced name
    FieldPtr with_name(std::string_view name);

    // return a copy of this field with the replaced nullability
    FieldPtr with_nullable(bool nullable);

    std::string to_string() const;

    ColumnId id() const { return _id; }
    std::string_view name() const { return {_name.data(), _name.size()}; }
    const TypeInfoPtr& type() const { return _type; }

    bool is_nullable() const;

    bool is_key() const;
    void set_is_key(bool is_key);

    int32_t length() const { return _length; }
    void set_length(int32_t l) { _length = l; }

    uint8_t short_key_length() const { return _short_key_length; }
    void set_short_key_length(uint8_t n) { _short_key_length = n; }

    // Encode the first |short_key_length| bytes.
    void encode_ascending(const Datum& value, std::string* buf) const;

    // Encode the full field.
    void full_encode_ascending(const Datum& value, std::string* buf) const;

    // Status decode_ascending(Slice* encoded_key, uint8_t* cell_ptr, MemPool* pool) const;

    void set_aggregate_method(StorageAggregateType agg_method) { _agg_method = agg_method; }

    starrocks::StorageAggregateType aggregate_method() const { return _agg_method; }

    FieldPtr convert_to(LogicalType to_type) const;

    void add_sub_field(const Field& sub_field);

    const Field& sub_field(int i) const;

    const std::vector<Field>& sub_fields() const { return *_sub_fields; }

    bool has_sub_fields() const { return _sub_fields != nullptr; }

    ColumnPtr create_column() const;

    void set_uid(ColumnUID uid) { _uid = uid; }
    const ColumnUID& uid() const { return _uid; }

    static FieldPtr convert_to_dict_field(const Field& field) {
        DCHECK(field.type()->type() == TYPE_VARCHAR);
        FieldPtr res = std::make_shared<Field>(field);
        res->_type = get_type_info(TYPE_INT);
        return res;
    }

private:
    constexpr static int kIsKeyShift = 0;
    constexpr static int kNullableShift = 1;

    ColumnId _id = 0;
    starrocks::StorageAggregateType _agg_method;
    CString _name;
    TypeInfoPtr _type = nullptr;
    std::vector<Field>* _sub_fields;
    int32_t _length = 0;
    uint8_t _short_key_length;
    uint8_t _flags;
    ColumnUID _uid = -1;
};

inline bool Field::is_nullable() const {
    return _flags & (1 << kNullableShift);
}

inline bool Field::is_key() const {
    return _flags & (1 << kIsKeyShift);
}

inline void Field::set_is_key(bool is_key) {
    if (is_key) {
        _flags |= static_cast<uint8_t>(1 << kIsKeyShift);
    } else {
        _flags &= static_cast<uint8_t>(~(1 << kIsKeyShift));
    }
}

inline void Field::add_sub_field(const Field& sub_field) {
    if (_sub_fields == nullptr) {
        _sub_fields = new std::vector<Field>();
    }
    _sub_fields->emplace_back(sub_field);
}

inline const Field& Field::sub_field(int i) const {
    return (*_sub_fields)[i];
}

inline FieldPtr Field::with_type(const TypeInfoPtr& type) {
    return std::make_shared<Field>(_id, std::string_view(_name.data(), _name.size()), type, _agg_method,
                                   _short_key_length, is_key(), is_nullable());
}

inline FieldPtr Field::with_name(std::string_view name) {
    return std::make_shared<Field>(_id, name, _type, _agg_method, _short_key_length, is_key(), is_nullable());
}

inline FieldPtr Field::with_nullable(bool nullable) {
    return std::make_shared<Field>(_id, std::string_view(_name.data(), _name.size()), _type, _agg_method,
                                   _short_key_length, is_key(), nullable);
}

inline std::ostream& operator<<(std::ostream& os, const Field& field) {
    os << field.id() << ":" << field.name() << " " << field.type()->type() << " "
       << (field.is_nullable() ? "NULL" : "NOT NULL") << (field.is_key() ? " KEY" : "") << " "
       << field.aggregate_method() << " uid:" << field.uid();
    return os;
}

inline std::string Field::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

} // namespace starrocks
